package yojoudb

import (
	"sync"
	"yojoudb/data"
	"yojoudb/index"
)

// DB database instance
type DB struct {
	activeFile *data.DataFile            // append-only
	olderFiles map[uint32]*data.DataFile // read-only
	index      index.Indexer
	options    Options
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

	// write
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
		Offset: db.activeFile.WriteOff,
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
