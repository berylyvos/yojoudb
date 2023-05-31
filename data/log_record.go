package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LRType = byte

const (
	LRNormal LRType = iota
	LRDeleted
	LRTxFin
)

// crc type keySize valSize
// 4   1    5       5 (varInt32)
const maxLogRecordHeaderSize = 0xf

// LogRecord log record in a file
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

// TxRecord transaction log record and it's location
type TxRecord struct {
	Lr  *LogRecord
	Loc *LRLoc
}

// Encode encodes LogRecord into bytes and returns the number of bytes encoded.
//
//	+------------------ header -------------------+
//	+---------+---------+------------+------------+------------+------------+
//	|   crc   |   typ   |     ksz    |    vsz     |    key     |   value    |
//	+--- 4 ---+--- 1 ---+-- var(5) --+-- var(5) --+------------+------------+
//	|
func Encode(lr *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)
	header[CRC32Size] = lr.Type
	idx := CRC32Size + 1
	ksz, vsz := len(lr.Key), len(lr.Val)
	idx += binary.PutVarint(header[idx:], int64(ksz))
	idx += binary.PutVarint(header[idx:], int64(vsz))

	n := idx + ksz + vsz
	b := make([]byte, n)
	copy(b[:idx], header[:idx])
	copy(b[idx:], lr.Key)
	copy(b[idx+ksz:], lr.Val)

	binary.LittleEndian.PutUint32(b[:CRC32Size], crc32.ChecksumIEEE(b[CRC32Size:]))
	return b, int64(n)
}

func EncodeLRLoc(loc *LRLoc) []byte {
	b := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var idx = 0
	idx += binary.PutVarint(b[idx:], int64(loc.Fid))
	idx += binary.PutVarint(b[idx:], loc.Offset)
	return b[:idx]
}

func DecodeLRLoc(b []byte) *LRLoc {
	idx := 0
	fid, n := binary.Varint(b[idx:])
	idx += n
	offset, n := binary.Varint(b[idx:])
	return &LRLoc{
		Fid:    uint32(fid),
		Offset: offset,
	}
}

func decodeLRHeader(b []byte) (*LRHeader, int64) {
	if len(b) <= CRC32Size {
		return nil, 0
	}

	lrh := &LRHeader{
		crc: binary.LittleEndian.Uint32(b[:CRC32Size]),
		typ: b[CRC32Size],
	}
	idx := CRC32Size + 1
	ksz, n := binary.Varint(b[idx:])
	lrh.ksz = uint32(ksz)
	idx += n
	vsz, n := binary.Varint(b[idx:])
	lrh.vsz = uint32(vsz)
	idx += n

	return lrh, int64(idx)
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
