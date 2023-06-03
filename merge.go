package yojoudb

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"yojoudb/data"
	"yojoudb/utils"
)

const (
	mergeDirName = "-merge"
	mergeFinKey  = "merged-before-fid"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(totalSize) < db.options.MergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	defer func() {
		db.isMerging = false
	}()
	db.isMerging = true

	// sync current active file, open a new one
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	notMergedFid := db.activeFile.FileId

	// files ready to merge
	var mergeFiles []*data.DataFile
	for _, df := range db.olderFiles {
		mergeFiles = append(mergeFiles, df)
	}
	// release lock
	db.mu.Unlock()

	// sort merge files in ascent older
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	// create a dir for merging, delete the old one
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// open a temp merge DB instance
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// open hint file for storing merged file index
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// MERGING
	for _, mf := range mergeFiles {
		var offset int64 = 0
		for {
			lr, sz, err := mf.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			key, _ := splitSeqNoAndKey(lr.Key)
			loc := db.index.Get(key)
			// if record is the newest version, meaning it's valid
			if loc != nil && loc.Fid == mf.FileId && loc.Offset == offset {
				// get rid of the Tx info and append to mergeDB
				lr.Key = spliceSeqNoAndKey(key, nonTxSeqNo)
				loc, err = mergeDB.appendLogRecord(lr)
				if err != nil {
					return err
				}
				// append location to hint file
				if err = hintFile.WriteHintRecord(key, loc); err != nil {
					return err
				}
			}

			offset += sz
		}
	}

	// sync mergeDB and it's hint file
	if err = mergeDB.Sync(); err != nil {
		return err
	}
	if err = hintFile.Sync(); err != nil {
		return err
	}

	// open a file to indicate merge finished
	// save the not-merged-fid
	mergeFinFile, err := data.OpenMergeFinFile(mergePath)
	if err != nil {
		return err
	}
	lr := &LR{
		Key: []byte(mergeFinKey),
		Val: []byte(strconv.Itoa(int(notMergedFid))),
	}
	encLr, _ := data.Encode(lr)
	if err = mergeFinFile.Write(encLr); err != nil {
		return err
	}
	if err = mergeFinFile.Sync(); err != nil {
		return err
	}

	return nil

}

func (db *DB) loadMergedFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	var mergeFin bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinFileName {
			mergeFin = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !mergeFin {
		return nil
	}

	notMergedFileId, err := db.getNotMergedFileId(mergePath)
	if err != nil {
		return err
	}

	// delete old files
	var fid uint32 = 0
	for ; fid < notMergedFileId; fid++ {
		fn := data.GetDataFileName(db.options.DirPath, fid)
		if _, err = os.Stat(fn); err == nil {
			if err = os.Remove(fn); err != nil {
				return err
			}
		}
	}

	// move merged files (including hint file) to current db data dir
	for _, fn := range mergeFileNames {
		src := filepath.Join(mergePath, fn)
		dst := filepath.Join(db.options.DirPath, fn)
		if err = os.Rename(src, dst); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) loadIndexerFromHint() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hf, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		lr, sz, err := hf.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		loc := data.DecodeLRLoc(lr.Val)
		db.index.Put(lr.Key, loc)
		offset += sz
	}

	return nil
}

func (db *DB) getNotMergedFileId(dirPath string) (uint32, error) {
	mergeFinFile, err := data.OpenMergeFinFile(dirPath)
	if err != nil {
		return 0, err
	}
	lr, _, err := mergeFinFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	notMergedFileId, err := strconv.Atoi(string(lr.Val))
	if err != nil {
		return 0, err
	}
	return uint32(notMergedFileId), nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}
