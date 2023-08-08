package yojoudb

import (
	"fmt"
	"github.com/bwmarrin/snowflake"
	"sync"
)

// Batch is a batch operation of the database.
// A batch can be read-only, calling Get() to get data.
// Otherwise, calling Delete() or Put() to put data.
// You must call Commit() to commit the batch, otherwise
// the deadlock happens.
//
// Batch is not a transaction, for it does not guarantee isolation.
// But it can guarantee atomicity, consistency and durability.
type Batch struct {
	db            *DB
	opt           BatchOptions
	mu            sync.RWMutex
	batchId       *snowflake.Node
	committed     bool
	rollbacked    bool
	pendingWrites map[string]*LR
}

func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:         db,
		opt:        options,
		committed:  false,
		rollbacked: false,
	}
	if !options.ReadOnly {
		batch.pendingWrites = make(map[string]*LR)
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func makeBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		opt:     DefaultBatchOptions,
		batchId: node,
	}
}

func (b *Batch) init(readOnly, sync bool, db *DB) *Batch {
	b.opt.ReadOnly = readOnly
	b.opt.Sync = sync
	b.db = db
	b.lock()
	return b
}

func (b *Batch) withPendingWrites() *Batch {
	b.pendingWrites = make(map[string]*LogRecord)
	return b
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = nil
	b.committed = false
	b.rollbacked = false
}

func (b *Batch) lock() {
	if b.opt.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.opt.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put adds a key/val to the batch for pending write.
func (b *Batch) Put(key K, value V) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if b.opt.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	b.pendingWrites[string(key)] = &LR{
		Key:  key,
		Val:  value,
		Type: LRNormal,
	}
	b.mu.Unlock()

	return nil
}

// Delete adds a key to the batch for pending delete.
func (b *Batch) Delete(key K) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if b.opt.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	if loc := b.db.index.Get(key); loc != nil {
		b.pendingWrites[string(key)] = &LR{
			Key:  key,
			Type: LRDeleted,
		}
	} else {
		delete(b.pendingWrites, string(key))
	}
	b.mu.Unlock()

	return nil
}

// Get gets the value of the given key.
// The key could still be in pendingWrites and haven't committed yet,
// return the value if the record type is not LRDeleted. Otherwise,
// get the value from WAL.
func (b *Batch) Get(key K) (V, error) {
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}

	// get from pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			if record.Type == LRDeleted {
				b.mu.RUnlock()
				return nil, ErrKeyNotFound
			}
			b.mu.RUnlock()
			return record.Val, nil
		}
		b.mu.RUnlock()
	}

	// get from WAL
	loc := b.db.index.Get(key)
	if loc == nil {
		return nil, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(loc)
	if err != nil {
		return nil, err
	}
	record := decodeLR(chunk)
	if record.Type == LRDeleted {
		panic("Deleted data cannot exist in the in-memory index")
	}
	return record.Val, nil
}

// Exist checks if the key exists in the database.
func (b *Batch) Exist(key K) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyEmpty
	}
	// check in pendingWrites
	if b.pendingWrites != nil {
		b.mu.RLock()
		if record := b.pendingWrites[string(key)]; record != nil {
			b.mu.RUnlock()
			return record.Type != LRDeleted, nil
		}
		b.mu.RUnlock()
	}
	// check in index
	loc := b.db.index.Get(key)
	return loc != nil, nil
}

// Commit commits the batch, if the batch is readonly or empty, return.
// It will iterate the pendingWrites and write the data to the db, then
// write a record to indicate the end of batch to guarantee atomicity.
// Finally, index will be updated.
func (b *Batch) Commit() error {
	defer b.unlock()

	if b.opt.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	batchId := b.batchId.Generate()
	locations := make(map[string]*Loc)

	// write to WAL
	for _, rec := range b.pendingWrites {
		rec.BatchId = uint64(batchId)
		encRec := encodeLR(rec)
		loc, err := b.db.dataFiles.Write(encRec)
		if err != nil {
			return err
		}
		locations[string(rec.Key)] = loc
	}

	// write an end-of-batch record
	encRec := encodeLR(&LR{
		Key:  batchId.Bytes(),
		Type: LRBatchFin,
	})
	if _, err := b.db.dataFiles.Write(encRec); err != nil {
		return err
	}

	// flush WAL if needed
	if b.opt.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// update index
	for key, rec := range b.pendingWrites {
		if rec.Type == LRDeleted {
			b.db.index.Delete(rec.Key)
		} else {
			b.db.index.Put(rec.Key, locations[key])
		}
	}

	b.committed = true
	return nil
}

// Rollback discards a uncommitted batch instance.
// It will clear pendingWrites and release db.lock.
func (b *Batch) Rollback() error {
	defer b.unlock()

	if !b.opt.ReadOnly {
		b.pendingWrites = nil
	}

	b.rollbacked = true
	return nil
}
