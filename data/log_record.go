package data

import "hash/crc32"

type LRType = byte

const (
	LRNormal LRType = iota
	LRDeleted
)

// crc type keySize valSize
// 4   1    5       5 (varint32)
const maxLogRecordHeaderSize = 0xf

// LogRecord a record in a file
type LogRecord struct {
	Key  []byte
	Val  []byte
	Type LRType
}

// LRHeader header of LogRecord
type LRHeader struct {
	crc uint32
	typ LRType
	ksz uint32
	vsz uint32
}

// LRLoc location of the log record on the disk
type LRLoc struct {
	Fid    uint32
	Offset int64
}

func Encode(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}

func decodeLRHeader(b []byte) (*LRHeader, int64) {
	return nil, 0
}

func calLogRecordCRC(lr *LogRecord, header []byte) (crc uint32) {
	if lr == nil {
		return
	}

	crc = crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Val)
	return
}
