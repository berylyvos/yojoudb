package wal

import (
	"io"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Write_FULL1(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-full1")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 1. FULL chunks
	val := []byte(strings.Repeat("X", 100))

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	pos2, err := seg.Write(val)
	assert.Nil(t, err)

	val1, err := seg.Read(pos1.BlockIndex, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	val2, err := seg.Read(pos2.BlockIndex, pos2.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val2)

	// 2. Write until a new block
	for i := 0; i < 100000; i++ {
		pos, err := seg.Write(val)
		assert.Nil(t, err)
		val, err := seg.Read(pos.BlockIndex, pos.ChunkOffset)
		assert.Nil(t, err)
		assert.Equal(t, val, val)
	}
}

func TestSegment_Write_FULL2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-full2")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 3. chunk full in blockSize
	val := []byte(strings.Repeat("X", blockSize-chunkHeaderSize))

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos1.BlockIndex, uint32(0))
	assert.Equal(t, pos1.ChunkOffset, int64(0))
	val1, err := seg.Read(pos1.BlockIndex, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	pos2, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos2.BlockIndex, uint32(1))
	assert.Equal(t, pos2.ChunkOffset, int64(0))
	val2, err := seg.Read(pos2.BlockIndex, pos2.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val2)
}

func TestSegment_Write_Padding(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-padding")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 4. three spaces for padding
	val := []byte(strings.Repeat("X", blockSize-chunkHeaderSize-3))

	_, err = seg.Write(val)
	assert.Nil(t, err)

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos1.BlockIndex, uint32(1))
	assert.Equal(t, pos1.ChunkOffset, int64(0))
	val1, err := seg.Read(pos1.BlockIndex, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val1)
}

func TestSegment_Write_NOT_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-not-full")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 5. FIRST-LAST
	bytes1 := []byte(strings.Repeat("X", blockSize+100))

	pos1, err := seg.Write(bytes1)
	assert.Nil(t, err)
	val1, err := seg.Read(pos1.BlockIndex, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val1)

	pos2, err := seg.Write(bytes1)
	assert.Nil(t, err)
	val2, err := seg.Read(pos2.BlockIndex, pos2.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val2)

	pos3, err := seg.Write(bytes1)
	assert.Nil(t, err)
	val3, err := seg.Read(pos3.BlockIndex, pos3.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val3)

	// 6. FIRST-MIDDLE-LAST
	bytes2 := []byte(strings.Repeat("X", blockSize*3+100))
	pos4, err := seg.Write(bytes2)
	assert.Nil(t, err)
	val4, err := seg.Read(pos4.BlockIndex, pos4.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes2, val4)
}

func TestSegment_Reader_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-full")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// FULL chunks
	bytes1 := []byte(strings.Repeat("X", 100))
	pos1, err := seg.Write(bytes1)
	assert.Nil(t, err)
	pos2, err := seg.Write(bytes1)
	assert.Nil(t, err)

	reader := seg.NewReader()
	val, rpos1, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)
	assert.Equal(t, pos1, rpos1)

	val, rpos2, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)
	assert.Equal(t, pos2, rpos2)

	val, rpos3, err := reader.Next()
	assert.Nil(t, val)
	assert.Equal(t, err, io.EOF)
	assert.Nil(t, rpos3)
}

func TestSegment_Reader_Padding(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-padding")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	bytes1 := []byte(strings.Repeat("X", blockSize-chunkHeaderSize-7))

	pos1, err := seg.Write(bytes1)
	assert.Nil(t, err)
	pos2, err := seg.Write(bytes1)
	assert.Nil(t, err)

	reader := seg.NewReader()
	val, rpos1, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)
	assert.Equal(t, pos1.SegId, rpos1.SegId)
	assert.Equal(t, pos1.BlockIndex, rpos1.BlockIndex)
	assert.Equal(t, pos1.ChunkOffset, rpos1.ChunkOffset)

	val, rpos2, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)
	assert.Equal(t, pos2.SegId, rpos2.SegId)
	assert.Equal(t, pos2.BlockIndex, rpos2.BlockIndex)
	assert.Equal(t, pos2.ChunkOffset, rpos2.ChunkOffset)

	_, _, err = reader.Next()
	assert.Equal(t, err, io.EOF)
}

func TestSegment_Reader_NOT_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-not-full")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	bytes1 := []byte(strings.Repeat("X", blockSize+100))
	pos1, err := seg.Write(bytes1)
	assert.Nil(t, err)
	pos2, err := seg.Write(bytes1)
	assert.Nil(t, err)

	bytes2 := []byte(strings.Repeat("X", blockSize*3+10))
	pos3, err := seg.Write(bytes2)
	assert.Nil(t, err)
	pos4, err := seg.Write(bytes2)
	assert.Nil(t, err)

	reader := seg.NewReader()
	val, rpos1, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	val, rpos2, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	val, rpos3, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes2, val)

	val, rpos4, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes2, val)

	_, _, err = reader.Next()
	assert.Equal(t, err, io.EOF)

	assert.Equal(t, pos1, rpos1)
	assert.Equal(t, pos2, rpos2)
	assert.Equal(t, pos3, rpos3)
	assert.Equal(t, pos4, rpos4)
}

func TestSegment_Reader_ManyChunks_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-ManyChunks_FULL")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	positions := make([]*ChunkLoc, 0)
	bytes1 := []byte(strings.Repeat("X", 128))
	const sz = 1000000
	for i := 1; i <= sz; i++ {
		pos, err := seg.Write(bytes1)
		assert.Nil(t, err)
		positions = append(positions, pos)
	}

	reader := seg.NewReader()
	var values [][]byte
	var i = 0
	for {
		val, pos, err := reader.Next()
		if err == io.EOF {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, bytes1, val)
		values = append(values, val)

		assert.Equal(t, positions[i].SegId, pos.SegId)
		assert.Equal(t, positions[i].BlockIndex, pos.BlockIndex)
		assert.Equal(t, positions[i].ChunkOffset, pos.ChunkOffset)

		i++
	}
	assert.Equal(t, sz, len(values))
}

func TestSegment_Reader_ManyChunks_NOT_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-ManyChunks_NOT_FULL")
	seg, err := openSegmentFile(dir, ".SEG", 1)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	positions := make([]*ChunkLoc, 0)
	bytes1 := []byte(strings.Repeat("X", blockSize*3+10))
	const sz = 10000
	for i := 1; i <= sz; i++ {
		pos, err := seg.Write(bytes1)
		assert.Nil(t, err)
		positions = append(positions, pos)
	}

	reader := seg.NewReader()
	var values [][]byte
	var i = 0
	for {
		val, pos, err := reader.Next()
		if err == io.EOF {
			break
		}
		assert.Nil(t, err)
		assert.Equal(t, bytes1, val)
		values = append(values, val)

		assert.Equal(t, positions[i].SegId, pos.SegId)
		assert.Equal(t, positions[i].BlockIndex, pos.BlockIndex)
		assert.Equal(t, positions[i].ChunkOffset, pos.ChunkOffset)

		i++
	}
	assert.Equal(t, sz, len(values))
}

func TestChunkPosition_Encode(t *testing.T) {
	validate := func(pos *ChunkLoc) {
		res := pos.Encode()
		assert.NotNil(t, res)
		decRes := DecodeChunkLoc(res)
		assert.Equal(t, pos, decRes)
	}

	validate(&ChunkLoc{1, 2, 3, 100})
	validate(&ChunkLoc{0, 0, 0, 0})
	validate(&ChunkLoc{math.MaxUint32, math.MaxUint32, math.MaxInt64, math.MaxUint32})
}
