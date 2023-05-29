package yojoudb

import (
	"encoding/binary"
	"sync"
	"yojoudb/data"
)

const (
	nonTxSeqNo uint64 = 0
)

var (
	txFinKey = []byte("tx-fin")
)

type WriteBatch struct {
	db            *DB
	mu            *sync.RWMutex
	opt           WriteBatchOptions
	pendingWrites map[string]*LR
}

func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		db:            db,
		mu:            new(sync.RWMutex),
		opt:           options,
		pendingWrites: make(map[string]*LR),
	}
}

func (wb *WriteBatch) Put(key K, value V) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	wb.pendingWrites[string(key)] = &LR{
		Key: key,
		Val: value,
	}
	return nil
}

func (wb *WriteBatch) Delete(key K) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	k := string(key)
	loc := wb.db.index.Get(key)
	if loc == nil {
		if wb.pendingWrites[k] != nil {
			delete(wb.pendingWrites, k)
		}
		return nil
	}

	wb.pendingWrites[k] = &LR{
		Key:  key,
		Type: data.LRDeleted,
	}
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.opt.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// a BIG LOCK for serialized Tx
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// seqNo++
	seqNo := wb.db.SeqNoIncr()

	locs := make(map[string]*Loc)
	for _, lr := range wb.pendingWrites {
		// use appendLogRecord (no-lock) to avoid deadlock
		loc, err := wb.db.appendLogRecord(&LR{
			Key:  spliceSeqNoAndKey(lr.Key, seqNo),
			Val:  lr.Val,
			Type: lr.Type,
		})
		if err != nil {
			return err
		}
		locs[string(lr.Key)] = loc
	}

	// a log record to indicate the completion of a Tx
	finLR := &LR{
		Key:  spliceSeqNoAndKey(txFinKey, seqNo),
		Type: data.LRTxFin,
	}
	if _, err := wb.db.appendLogRecord(finLR); err != nil {
		return err
	}

	// check for sync write
	if wb.opt.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// all data persisted, update in-memory index
	for _, lr := range wb.pendingWrites {
		if lr.Type == data.LRDeleted {
			wb.db.index.Delete(lr.Key)
		} else if lr.Type == data.LRNormal {
			wb.db.index.Put(lr.Key, locs[string(lr.Key)])
		}
	}

	// reset pendingWrites
	wb.pendingWrites = make(map[string]*LR)

	return nil
}

func spliceSeqNoAndKey(key K, seqNo uint64) K {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	b := make([]byte, n+len(key))
	copy(b[:n], seq[:])
	copy(b[n:], key)

	return b
}

func splitSeqNoAndKey(key K) (K, uint64) {
	seqNo, n := binary.Uvarint(key)
	return key[n:], seqNo
}
