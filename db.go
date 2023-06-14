package yojoudb

import (
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"yojoudb/data"
	"yojoudb/fio"
	"yojoudb/meta"
	"yojoudb/utils"
)

const (
	fileLockName = "flock"
)

// DB database instance
type DB struct {
	mu          *sync.RWMutex
	activeFile  *data.DataFile            // append & read
	olderFiles  map[uint32]*data.DataFile // read-only
	index       meta.Indexer
	fileIds     []int // for loading index
	options     *Options
	seqNo       uint64
	isMerging   bool
	fileLock    *flock.Flock // file lock for single process
	bytesWrite  uint
	reclaimSize int64
}

type Stat struct {
	KeyNum          uint
	DataFileNum     uint
	ReclaimableSize int64
	DiskSize        int64
}

// K key alias for []byte
type K = []byte

// V value alias for []byte
type V = []byte

// LR alias for data.LogRecord
type LR = data.LogRecord

// TxR alias for data.TxRecord
type TxR = data.TxRecord

// Loc alias for data.LRLoc
type Loc = data.LRLoc

// Open opens a db instance.
func Open(options *Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// check if there's another process get the file lock
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	db := &DB{
		mu:         new(sync.RWMutex),
		options:    options,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      meta.NewIndexer(options.IndexType),
		fileLock:   fileLock,
	}

	// load merged files
	if err := db.loadMergedFiles(); err != nil {
		return nil, err
	}

	// load data files
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// load indexer from hint file
	if err := db.loadIndexerFromHint(); err != nil {
		return nil, err
	}

	// load indexer from data files
	if err := db.loadIndexer(); err != nil {
		return nil, err
	}

	// reset i/o type to std file i/o
	if err := db.resetIOType(); err != nil {
		return nil, err
	}

	return db, nil
}

// SeqNoIncr increases db.seqNo by one
func (db *DB) SeqNoIncr() uint64 {
	return atomic.AddUint64(&db.seqNo, 1)
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Close closes the db instance. Closes active file and old files.
func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activeFile.Close(); err != nil {
		return err
	}
	for _, old := range db.olderFiles {
		if err := old.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync syncs active file into disk.
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put puts a normal log record with the given key/val.
func (db *DB) Put(key K, val V) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	lr := &LR{
		Key:  spliceSeqNoAndKey(key, nonTxSeqNo),
		Val:  val,
		Type: data.LRNormal,
	}

	loc, err := db.appendLogRecordWithLock(lr)
	if err != nil {
		return err
	}

	// update index
	if oldVal := db.index.Put(key, loc); oldVal != nil {
		db.reclaimSize += int64(oldVal.Size)
	}

	return nil
}

// Get gets the value of the given key. Returns nil if key is not found.
func (db *DB) Get(key K) (V, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}

	// find key from index
	loc := db.index.Get(key)
	if loc == nil {
		return nil, ErrKeyNotFound
	}

	// if key exist, retrieve value by log record location
	return db.retrievalByLoc(loc)
}

// Delete puts a delete-type log record of the given key,
// and deletes the key in index. Returns nil if key is not found.
func (db *DB) Delete(key K) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	if loc := db.index.Get(key); loc == nil {
		return nil
	}

	lr := &LR{
		Key:  spliceSeqNoAndKey(key, nonTxSeqNo),
		Type: data.LRDeleted,
	}

	loc, err := db.appendLogRecordWithLock(lr)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(loc.Size)

	oldVal, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldVal != nil {
		db.reclaimSize += int64(oldVal.Size)
	}
	return nil
}

// ListKeys returns all keys in db instance.
func (db *DB) ListKeys() [][]byte {
	keys := make([][]byte, db.index.Size())
	it := db.NewIterator(DefaultIteratorOptions)
	defer it.Close()
	idx := 0
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx++
	}
	return keys
}

// Fold iterates every key/val, executes func on it, stops when func return false.
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	it := db.NewIterator(DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		val, err := it.Value()
		if err != nil {
			return err
		}
		if !fn(it.Key(), val) {
			break
		}
	}
	return nil
}

func (db *DB) retrievalByLoc(loc *Loc) (V, error) {
	var df *data.DataFile
	if db.activeFile.FileId == loc.Fid {
		df = db.activeFile
	} else {
		df = db.olderFiles[loc.Fid]
	}
	if df == nil {
		return nil, ErrDataFileNotFound
	}

	lr, _, err := df.ReadLogRecord(loc.Offset)
	if err != nil {
		return nil, err
	}

	if lr.Type == data.LRDeleted {
		return nil, ErrKeyNotFound
	}

	return lr.Val, nil
}

func (db *DB) appendLogRecordWithLock(lr *LR) (*Loc, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(lr)
}

// appendLogRecord appends log record to active file
func (db *DB) appendLogRecord(lr *LR) (*Loc, error) {
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// encode log record into bytes
	encLr, sz := data.Encode(lr)

	// if active file meets size threshold, close active and open a new one
	if sz+db.activeFile.WriteOff > db.options.DataFileSize {
		// sync active file to disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// turn active file into older file
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// open a new active file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// take down current write offset BEFORE WRITE
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encLr); err != nil {
		return nil, err
	}

	// handle sync after write
	db.bytesWrite += uint(sz)
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// return the log record location
	return &Loc{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(sz),
	}, nil
}

func (db *DB) setActiveDataFile() error {
	var fid uint32 = 0
	// active file is not null, which means it meets size threshold,
	// the new active file id incr by 1
	if db.activeFile != nil {
		fid = db.activeFile.FileId + 1
	}
	df, err := data.OpenDataFile(db.options.DirPath, fid, fio.IOStdFile)
	if err != nil {
		return err
	}
	db.activeFile = df
	return nil
}

func (db *DB) loadDataFiles() error {
	dir, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	for _, v := range dir {
		if strings.HasSuffix(v.Name(), data.DataFileSuffix) {
			fid, err := strconv.Atoi(strings.Split(v.Name(), ".")[0])
			if err != nil {
				return ErrDataDirBroken
			}
			fileIds = append(fileIds, fid)
		}
	}

	// sort file ids and open every file, the last one is active
	sort.Ints(fileIds)
	for i, fid := range fileIds {
		// we use mmap to map data file to memory when launching db
		df, err := data.OpenDataFile(db.options.DirPath, uint32(fid), fio.IOMMap)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = df
		} else {
			db.olderFiles[df.FileId] = df
		}
	}
	db.fileIds = fileIds

	return nil
}

func (db *DB) loadIndexer() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// check if merged, get the not-merged-fid
	hasMerged, notMergedFid := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNotMergedFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerged = true
		notMergedFid = fid
	}

	updateIndex := func(key K, typ data.LRType, loc *Loc) {
		var oldVal *Loc
		if typ == data.LRDeleted {
			oldVal, _ = db.index.Delete(key)
			db.reclaimSize += int64(loc.Size)
		} else if typ == data.LRNormal {
			oldVal = db.index.Put(key, loc)
		}
		if oldVal != nil {
			db.reclaimSize += int64(oldVal.Size)
		}
	}

	// cache for tx records
	txRecords := make(map[uint64][]*TxR)
	var curSeqNo = nonTxSeqNo

	for _, fid := range db.fileIds {
		fileId := uint32(fid)

		// fid < not-merged-fid, meaning that the file is merged
		// and the index has been loaded from hint file
		if hasMerged && fileId < notMergedFid {
			continue
		}

		var df *data.DataFile
		if fileId == db.activeFile.FileId {
			df = db.activeFile
		} else {
			df = db.olderFiles[fileId]
		}

		// read log record one by one
		var offset int64 = 0
		for {
			lr, sz, err := df.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			loc := &Loc{
				Fid:    fileId,
				Offset: offset,
				Size:   uint32(sz),
			}

			key, seqNo := splitSeqNoAndKey(lr.Key)
			if seqNo == nonTxSeqNo {
				updateIndex(key, lr.Type, loc)
			} else { // handle Tx records
				if lr.Type == data.LRTxFin {
					for _, tr := range txRecords[seqNo] {
						updateIndex(tr.Lr.Key, tr.Lr.Type, tr.Loc)
					}
					delete(txRecords, seqNo)
				} else {
					lr.Key = key
					txr := &TxR{
						Lr:  lr,
						Loc: loc,
					}
					txRecords[seqNo] = append(txRecords[seqNo], txr)
				}
			}

			if seqNo > curSeqNo {
				curSeqNo = seqNo
			}

			offset += sz
		}

		// update the active file write offset
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
		}
	}

	db.seqNo = curSeqNo

	return nil
}

func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.IOStdFile); err != nil {
		return err
	}
	for _, df := range db.olderFiles {
		if err := df.SetIOManager(db.options.DirPath, fio.IOStdFile); err != nil {
			return err
		}
	}
	return nil
}

func checkOptions(options *Options) error {
	if options.DirPath == "" {
		return ErrDirPathIsEmpty
	}
	if options.DataFileSize <= 0 {
		return ErrDataFileSizeNotPositive
	}
	if options.MergeRatio < 0 || options.MergeRatio > 1 {
		return ErrInvalidMergeRatio
	}
	return nil
}

func (db *DB) GetDirPath() string {
	return db.options.DirPath
}
