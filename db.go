package yojoudb

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sync"
	"yojoudb/meta"
	"yojoudb/utils"
	"yojoudb/wal"
)

const (
	fileLockName   = "FLOCK"
	dataFileSuffix = ".SEG"
	hintFileSuffix = ".HINT"
	mergeFinSuffix = ".MERGE_FIN"
)

// DB is a database instance.
// It's built on a log-structured model, the Bitcask.
// Read and write data based on WAL(Write-Ahead Log).
// An in-memory index is for holding the keys and the
// corresponding locations. The index is rebuilt each
// time the database is restarted.
type DB struct {
	dataFiles    *wal.WAL
	hintFile     *wal.WAL
	index        meta.Indexer
	options      Options
	fileLock     *flock.Flock
	mu           sync.RWMutex
	closed       bool
	mergeRunning uint32
	reclaimSize  int64
}

type Stat struct {
	KeyNum          uint64
	DataFileNum     uint32
	ReclaimableSize int64
	DiskSize        int64
}

// K key alias for []byte
type K = []byte

// V value alias for []byte
type V = []byte

// LR alias for LogRecord
type LR = LogRecord

// Loc alias for wal.ChunkLoc
type Loc = wal.ChunkLoc

// Open opens a db instance with specified options.
// It will open the WAL files and build the index.
func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// prevent multiple processes from occupying the same database.
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// load merged files if exists
	if err := loadMergedFiles(options.DirPath); err != nil {
		return nil, err
	}

	// open data files in WAL
	dataFiles, err := wal.Open(wal.Options{
		DirPath:        options.DirPath,
		SegmentSize:    options.SegmentSize,
		SegmentFileExt: dataFileSuffix,
		BlockCacheSize: options.BlockCacheSize,
		Sync:           options.Sync,
		BytesPerSync:   options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}

	// init db instance
	db := &DB{
		dataFiles: dataFiles,
		options:   options,
		index:     meta.NewIndexer(options.IndexType),
		fileLock:  fileLock,
	}

	// load index from hint file if there's a merged db
	if err := db.loadIndexerFromHint(); err != nil {
		return nil, err
	}

	// load index from data files
	if err := db.loadIndexer(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint64(uint(db.index.Size())),
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Close closes the db instance.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// close wal
	if err := db.dataFiles.Close(); err != nil {
		return err
	}

	// close hint file if exists
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}

	// release file lock
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	db.closed = true
	return nil
}

// Sync syncs all data files into disk.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.dataFiles.Sync()
}

// Put puts the given key/val.
func (db *DB) Put(key K, val V) error {
	batchOpt := DefaultBatchOptions
	batchOpt.Sync = false
	batch := db.NewBatch(batchOpt)
	if err := batch.Put(key, val); err != nil {
		return err
	}
	return batch.Commit()
}

// Get gets the value of the given key.
// Returns nil if key is not found.
func (db *DB) Get(key K) (V, error) {
	batchOpt := DefaultBatchOptions
	batchOpt.ReadOnly = true
	batch := db.NewBatch(batchOpt)
	defer func() {
		_ = batch.Commit()
	}()
	return batch.Get(key)
}

// Delete deletes the given key.
func (db *DB) Delete(key K) error {
	batchOpt := DefaultBatchOptions
	batchOpt.Sync = false
	batch := db.NewBatch(batchOpt)
	if err := batch.Delete(key); err != nil {
		return err
	}
	return batch.Commit()
}

// Exist checks if the given key exists.
func (db *DB) Exist(key K) (bool, error) {
	batchOpt := DefaultBatchOptions
	batchOpt.ReadOnly = true
	batch := db.NewBatch(batchOpt)
	defer func() {
		_ = batch.Commit()
	}()
	return batch.Exist(key)
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

// loadIndexer loads index from WAL.
// It will iterate all over the WAL files and
// read data from them to rebuild the index.
func (db *DB) loadIndexer() error {
	mergeFinSegId, err := getMergeFinSegId(db.options.DirPath)
	if err != nil {
		return err
	}

	// batchId => indexRecords
	indexRecords := make(map[uint64][]*IndexRecord)

	reader := db.dataFiles.NewReader()
	for {
		// skip the segments which segId is less than or equal to mergeFinSegId,
		// their indexes has already been loaded through hint file.
		if reader.CurrSegId() <= mergeFinSegId {
			reader.Skip()
			continue
		}

		chunk, loc, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLR(chunk)

		// if reaching to end-of-batch,
		// put or delete all records in the batch to index.
		if record.Type == LRBatchFin {
			snowflakeId, err := snowflake.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			batchId := uint64(snowflakeId)
			for _, idxRec := range indexRecords[batchId] {
				if idxRec.typ == LRNormal {
					db.index.Put(idxRec.key, idxRec.loc)
				}
				if idxRec.typ == LRDeleted {
					db.index.Delete(idxRec.key)
				}
			}
			delete(indexRecords, batchId)
		} else {
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&IndexRecord{
					key: record.Key,
					typ: record.Type,
					loc: loc,
				})
		}
	}

	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDirPathIsEmpty
	}
	if options.SegmentSize <= 0 {
		return ErrDataFileSizeNotPositive
	}
	return nil
}

func (db *DB) GetDirPath() string {
	return db.options.DirPath
}
