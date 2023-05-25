package yojoudb

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"yojoudb/data"
	"yojoudb/index"
)

// DB database engine instance
type DB struct {
	activeFile *data.DataFile            // append & read
	olderFiles map[uint32]*data.DataFile // read-only
	index      index.Indexer
	fileIds    []int // for loading index
	options    *Options
	mu         *sync.RWMutex
}

// K key alias for []byte
type K = []byte

// V value alias for []byte
type V = []byte

// LR alias for data.LogRecord
type LR = data.LogRecord

// Loc alias for data.LRLoc
type Loc = data.LRLoc

// Open opens the db engine instance
func Open(options *Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		mu:         new(sync.RWMutex),
		options:    options,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// load data files
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// load indexer
	if err := db.loadIndexer(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the db engine instance
func (db *DB) Close() error {
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

// Put puts key/val
func (db *DB) Put(key K, val V) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	lr := &LR{
		Key:  key,
		Val:  val,
		Type: data.LRNormal,
	}

	loc, err := db.appendLogRecord(lr)
	if err != nil {
		return err
	}

	// update index
	if ok := db.index.Put(key, loc); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get gets value of the given key, return nil if key not found
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

func (db *DB) Delete(key K) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	if loc := db.index.Get(key); loc == nil {
		return nil
	}

	lr := &LR{
		Key:  key,
		Type: data.LRDeleted,
	}

	_, err := db.appendLogRecord(lr)
	if err != nil {
		return err
	}

	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
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

// appendLogRecord appends log record to active file
func (db *DB) appendLogRecord(lr *LR) (*Loc, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

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

	// if SyncWrites open, sync after every write
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// return the log record location
	return &Loc{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}, nil
}

func (db *DB) setActiveDataFile() error {
	var fid uint32 = 0
	// active file is not null, which means it meets size threshold,
	// the new active file id incr by 1
	if db.activeFile != nil {
		fid = db.activeFile.FileId + 1
	}
	df, err := data.OpenDataFile(db.options.DirPath, fid)
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
		df, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
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

	for _, fid := range db.fileIds {
		var df *data.DataFile
		fileId := uint32(fid)
		if fileId == db.activeFile.FileId {
			df = db.activeFile
		} else {
			df = db.olderFiles[fileId]
		}

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
			}
			var ok bool
			if lr.Type == data.LRDeleted {
				ok = db.index.Delete(lr.Key)
			} else {
				ok = db.index.Put(lr.Key, loc)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}

			offset += sz
		}

		// update the active file write offset
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOff = offset
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
	return nil
}
