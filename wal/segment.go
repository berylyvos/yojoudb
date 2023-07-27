package wal

import (
	"encoding/binary"
	"os"
)

type (
	ChunkType = byte
	SegmentID = uint32
)

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

const (
	// crc(4) + length(2) + type(1)
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32768
)

// segment represents a single segment file in WAL.
// The segment file is append-only, and the data is written in blocks.
// Each block is 32KB, and the data is written in chunks.
type segment struct {
	id             SegmentID
	fd             *os.File
	curBlockNumber uint32
	curBlockSize   uint32
	closed         bool
	header         []byte
}

// ChunkLoc represents the location of a chunk in a segment file.
// Used to read the data from the segment file.
type ChunkLoc struct {
	SegId       SegmentID
	BlockNumber uint32
	ChunkOffset int64
	ChunkSize   uint32
}

// Encode encodes a ChunkLoc into bytes.
// In reverse, decode it by wal.DecodeChunkLoc().
func (loc *ChunkLoc) Encode() []byte {
	b := make([]byte, binary.MaxVarintLen32*3+binary.MaxVarintLen64)

	var idx = 0
	// SegId
	idx += binary.PutUvarint(b[idx:], uint64(loc.SegId))
	// BlockNumber
	idx += binary.PutUvarint(b[idx:], uint64(loc.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(b[idx:], uint64(loc.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(b[idx:], uint64(loc.ChunkSize))

	return b[:idx]
}

// DecodeChunkLoc decodes a ChunkLoc from bytes.
func DecodeChunkLoc(b []byte) *ChunkLoc {
	if len(b) == 0 {
		return nil
	}

	var idx = 0
	segId, n := binary.Uvarint(b[idx:])
	idx += n
	blockNumber, n := binary.Uvarint(b[idx:])
	idx += n
	chunkOffset, n := binary.Uvarint(b[idx:])
	idx += n
	chunkSize, n := binary.Uvarint(b[idx:])

	return &ChunkLoc{
		SegId:       uint32(segId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
