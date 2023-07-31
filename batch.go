package yojoudb

import (
	"encoding/binary"
	"sync"
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
		Type: LRDeleted,
	}
	return nil
}

func (wb *WriteBatch) Commit() error {

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
