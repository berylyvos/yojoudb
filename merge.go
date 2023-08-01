package yojoudb

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"yojoudb/wal"
)

const (
	mergeDirSuffix  = "-merge"
	mergeFinBatchId = 0
)

// Merge merges all the data files.
// It will iterate all over the WAL, filtering valid data,
// then writing them to a new WAL in the merge directory.
//
// Calling this method is up to the users, it may be very
// time-consuming when the database is large.
func (db *DB) Merge() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	if db.dataFiles.IsEmpty() {
		db.mu.Unlock()
		return nil
	}
	if atomic.LoadUint32(&db.mergeRunning) == 1 {
		db.mu.Unlock()
		return ErrMergeIsRunning
	}
	atomic.StoreUint32(&db.mergeRunning, 1)
	defer atomic.StoreUint32(&db.mergeRunning, 0)

	lastActiveSegId := db.dataFiles.ActiveSegID()
	if err := db.dataFiles.OpenNewActiveSeg(); err != nil {
		return err
	}

	// release lock here
	db.mu.Unlock()

	// open a merge db to hold the merged data.
	mergeDB, err := db.openMergeDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()

	reader := db.dataFiles.NewReaderLE(lastActiveSegId)
	for {
		chunk, loc, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLR(chunk)
		// Deleted & Batch-Finished logs are not valid.
		if record.Type == LRNormal {
			db.mu.RLock()
			indexLoc := db.index.Get(record.Key)
			db.mu.RUnlock()
			// this means current record is the newest one.
			if indexLoc != nil && locEqual(indexLoc, loc) {
				record.BatchId = mergeFinBatchId
				newLoc, err := mergeDB.dataFiles.Write(encodeLR(record))
				if err != nil {
					return err
				}
				// append key/newLoc to HINT-FILE, which is for rebuilding index
				// quickly when db is restarted.
				_, err = mergeDB.hintFile.Write(encodeHintRecord(record.Key, newLoc))
				if err != nil {
					return err
				}
			}
		}
	}

	// To make sure the completeness of the merged data.
	// At the end of merging, adding a file to indicate that the merge operation is done.
	mergeFinFile, err := mergeDB.openMergeFinFile()
	if err != nil {
		return err
	}
	_, err = mergeFinFile.Write(encodeMergeFinRecord(lastActiveSegId))
	if err != nil {
		return err
	}
	if err = mergeFinFile.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) openMergeDB() (*DB, error) {
	mergePath := mergeDirPath(db.options.DirPath)
	if err := os.RemoveAll(mergePath); err != nil {
		return nil, err
	}
	options := db.options
	options.Sync, options.BytesPerSync = false, 0
	options.DirPath = mergePath
	mergeDB, err := Open(options)
	if err != nil {
		return nil, err
	}

	// open the hint files to hold the new location.
	hintFile, err := wal.Open(wal.Options{
		DirPath:        options.DirPath,
		SegmentSize:    math.MaxInt64, // INF
		SegmentFileExt: hintFileSuffix,
		Sync:           false,
		BytesPerSync:   0,
	})
	if err != nil {
		return nil, err
	}
	mergeDB.hintFile = hintFile
	return mergeDB, nil
}

func (db *DB) openMergeFinFile() (*wal.WAL, error) {
	return wal.Open(wal.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    GB,
		SegmentFileExt: mergeFinSuffix,
		Sync:           false,
		BytesPerSync:   0,
		BlockCacheSize: 0,
	})
}

func mergeDirPath(path string) string {
	dir := filepath.Dir(filepath.Clean(path))
	return filepath.Join(dir, filepath.Base(path)+mergeDirSuffix)
}

func (db *DB) loadMergedFiles() error {

	return nil
}

func (db *DB) loadIndexerFromHint() error {

	return nil
}

func (db *DB) getNotMergedFileId(dirPath string) (uint32, error) {
	return 0, nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirSuffix)
}

func locEqual(a, b *Loc) bool {
	return a.SegId == b.SegId &&
		a.BlockIndex == b.BlockIndex &&
		a.ChunkOffset == b.ChunkOffset
}

func encodeHintRecord(key K, loc *Loc) []byte {
	// SegId BlockIndex ChunkOffset ChunkSize  key
	//    5 +    5     +    10     +    5     +len(key)
	kl := len(key)
	b := make([]byte, 25+kl)
	idx := 0
	idx += binary.PutUvarint(b[idx:], uint64(loc.SegId))
	idx += binary.PutUvarint(b[idx:], uint64(loc.BlockIndex))
	idx += binary.PutUvarint(b[idx:], uint64(loc.ChunkOffset))
	idx += binary.PutUvarint(b[idx:], uint64(loc.ChunkSize))

	copy(b[idx:], key)
	idx += kl
	return b[:idx]
}

func decodeHintRecord(b []byte) (K, *Loc) {
	idx := 0
	segId, n := binary.Uvarint(b[idx:])
	idx += n
	blockIndex, n := binary.Uvarint(b[idx:])
	idx += n
	chunkOffset, n := binary.Uvarint(b[idx:])
	idx += n
	chunkSize, n := binary.Uvarint(b[idx:])
	idx += n
	key := b[idx:]

	return key, &Loc{
		SegId:       wal.SegmentID(segId),
		BlockIndex:  uint32(blockIndex),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

func encodeMergeFinRecord(segId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segId)
	return buf
}
