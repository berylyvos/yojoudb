package yojoudb

import (
	"encoding/binary"
	"github.com/berylyvos/yojoudb/wal"
)

type LRType = byte

const (
	LRNormal LRType = iota
	LRDeleted
	LRBatchFin
)

const maxLogRecordHeaderSize = 0x15

// LogRecord is the log record of the key/val.
type LogRecord struct {
	Key     K
	Val     V
	Type    LRType
	BatchId uint64
}

// IndexRecord is the index record of the key.
// Only used in start up to build in-mem index.
type IndexRecord struct {
	key K
	typ LRType
	loc *wal.ChunkLoc
}

// encodeLR encodes a LogRecord into bytes.
//
//	+--------------------- header ---------------------+
//	+---------+--------------+------------+------------+------------+------------+
//	|   typ   |   batch_id   |  key_size  |  val_size  |    key     |    val     |
//	+--- 1 ---+----var(10)---+-- var(5) --+-- var(5) --+------------+------------+
//	+
func encodeLR(lr *LogRecord) []byte {
	header := make([]byte, maxLogRecordHeaderSize)

	header[0] = lr.Type
	idx := 1
	ksz, vsz := len(lr.Key), len(lr.Val)
	idx += binary.PutUvarint(header[idx:], lr.BatchId)
	idx += binary.PutVarint(header[idx:], int64(ksz))
	idx += binary.PutVarint(header[idx:], int64(vsz))

	b := make([]byte, idx+ksz+vsz)
	copy(b[:idx], header[:idx])
	copy(b[idx:], lr.Key)
	copy(b[idx+ksz:], lr.Val)

	return b
}

// decodeLR decodes the log record from the given bytes.
func decodeLR(b []byte) *LogRecord {
	typ := b[0]

	idx := 1
	batchId, n := binary.Uvarint(b[idx:])
	idx += n
	keySize, n := binary.Varint(b[idx:])
	idx += n
	valSize, n := binary.Varint(b[idx:])
	idx += n

	key := make([]byte, keySize)
	copy(key[:], b[idx:idx+int(keySize)])
	idx += int(keySize)

	val := make([]byte, valSize)
	copy(val[:], b[idx:idx+int(valSize)])

	return &LogRecord{
		Key:     key,
		Val:     val,
		Type:    typ,
		BatchId: batchId,
	}
}
