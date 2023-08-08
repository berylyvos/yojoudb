package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
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
	blockSize = 32 * KB

	fileModePerm = 0644
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

// segment represents a single segment file in WAL.
// The segment file is append-only, and the data is written in blocks.
// Each block is 32KB, and the data is written in chunks.
type segment struct {
	id            SegmentID
	fd            *os.File
	curBlockIndex uint32
	curBlockSize  uint32
	closed        bool
	cache         BlockCache
	header        []byte
	blockPool     sync.Pool
}

// segmentReader is used to iterate all the data from segment file.
// Call segmentReader.Next() to get the next chunk data,
// and io.EOF will be returned when there is no data.
type segmentReader struct {
	seg      *segment
	blockIdx uint32
	chunkOff int64
}

// block and chunk header, saved in pool.
type blockAndHeader struct {
	block  []byte
	header []byte
}

// ChunkLoc represents the location of a chunk in a segment file.
// Used to read the data from the segment file.
type ChunkLoc struct {
	SegId       SegmentID
	BlockIndex  uint32
	ChunkOffset int64
	ChunkSize   uint32
}

// openSegmentFile opens a segment file.
func openSegmentFile(dirPath, extName string, id SegmentID, cache BlockCache) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)
	if err != nil {
		return nil, err
	}

	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err))
	}

	return &segment{
		id:            id,
		fd:            fd,
		curBlockIndex: uint32(offset / blockSize),
		curBlockSize:  uint32(offset % blockSize),
		header:        make([]byte, chunkHeaderSize),
		cache:         cache,
		blockPool:     sync.Pool{New: newBlockAndHeader},
	}, nil
}

// SegmentFileName returns the file name of a segment file.
func SegmentFileName(dirPath, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

func newBlockAndHeader() interface{} {
	return &blockAndHeader{
		block:  make([]byte, blockSize),
		header: make([]byte, chunkHeaderSize),
	}
}

func (s *segment) NewReader() *segmentReader {
	return &segmentReader{
		seg:      s,
		blockIdx: 0,
		chunkOff: 0,
	}
}

func (s *segment) Sync() error {
	if s.closed {
		return nil
	}
	return s.fd.Sync()
}

func (s *segment) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.fd.Close()
}

func (s *segment) Remove() error {
	if !s.closed {
		s.closed = true
		_ = s.fd.Close()
	}
	return os.Remove(s.fd.Name())
}

func (s *segment) Size() int64 {
	return int64(s.curBlockIndex*blockSize + s.curBlockSize)
}

func (s *segment) Write(data []byte) (*ChunkLoc, error) {
	if s.closed {
		return nil, ErrClosed
	}

	// not enough block space for a chunk header
	if s.curBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if s.curBlockSize < blockSize {
			padding := make([]byte, blockSize-s.curBlockSize)
			if _, err := s.fd.Write(padding); err != nil {
				return nil, err
			}
		}
		// new block
		s.curBlockSize = 0
		s.curBlockIndex++
	}

	// chunk location for reading
	loc := &ChunkLoc{
		SegId:       s.id,
		BlockIndex:  s.curBlockIndex,
		ChunkOffset: int64(s.curBlockSize),
	}

	dataSize := uint32(len(data))
	// if the whole data can fit into current block, stuff a full chunk in
	if s.curBlockSize+dataSize+chunkHeaderSize <= blockSize {
		if err := s.writeInternal(data, ChunkTypeFull); err != nil {
			return nil, err
		}
		loc.ChunkSize = dataSize + chunkHeaderSize
		return loc, nil
	}

	// if the size of the data exceeds the block size, should be written in batches.
	var leftSize = dataSize
	var chunkCount uint32 = 0
	for leftSize > 0 {
		chunkSize := blockSize - s.curBlockSize - chunkHeaderSize
		if chunkSize > leftSize {
			chunkSize = leftSize
		}
		chunk := make([]byte, chunkSize)

		start := dataSize - leftSize
		end := start + chunkSize
		if end > dataSize {
			end = dataSize
		}
		copy(chunk[:], data[start:end])

		// write chunks
		var err error
		if leftSize == dataSize {
			err = s.writeInternal(chunk, ChunkTypeFirst)
		} else if leftSize == chunkSize {
			err = s.writeInternal(chunk, ChunkTypeLast)
		} else {
			err = s.writeInternal(chunk, ChunkTypeMiddle)
		}
		if err != nil {
			return nil, err
		}
		leftSize -= chunkSize
		chunkCount++
	}

	loc.ChunkSize = chunkCount*chunkHeaderSize + dataSize
	return loc, nil
}

func (s *segment) writeInternal(data []byte, chunkType ChunkType) error {
	dataSize := uint32(len(data))

	// Length    2B:4-5
	binary.LittleEndian.PutUint16(s.header[4:6], uint16(dataSize))
	// Type      1B:6
	s.header[6] = chunkType
	// Checksum  4B:0-3
	sum := crc32.ChecksumIEEE(s.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(s.header[:4], sum)

	// append to the file
	buf := bytebufferpool.Get()
	defer func() {
		bytebufferpool.Put(buf)
	}()
	buf.B = append(buf.B, s.header...)
	buf.B = append(buf.B, data...)
	if _, err := s.fd.Write(buf.Bytes()); err != nil {
		return err
	}

	if s.curBlockSize > blockSize {
		panic("can not exceed the max block size")
	}

	// update curBlockSize, curBlockIndex
	s.curBlockSize += dataSize + chunkHeaderSize
	if s.curBlockSize == blockSize {
		s.curBlockIndex++
		s.curBlockSize = 0
	}

	return nil
}

func (s *segment) Read(blockIndex uint32, chunkOffset int64) ([]byte, error) {
	val, _, err := s.readInternal(blockIndex, chunkOffset)
	return val, err
}

func (s *segment) readInternal(blockIndex uint32, chunkOffset int64) ([]byte, *ChunkLoc, error) {
	if s.closed {
		return nil, nil, ErrClosed
	}

	var (
		res       []byte
		bh        = s.blockPool.Get().(*blockAndHeader)
		segSize   = s.Size()
		nextChunk = &ChunkLoc{SegId: s.id}
	)
	defer func() {
		s.blockPool.Put(bh)
	}()

	for {
		sz := int64(blockSize)
		offset := int64(blockIndex * blockSize)
		// the block is not full, meaning that we've reached the last block
		if offset+sz > segSize {
			sz = segSize - offset
		}

		if chunkOffset >= sz {
			return nil, nil, io.EOF
		}

		// read an entire block
		var (
			ok          bool
			cachedBlock []byte
		)
		if s.cache != nil {
			cachedBlock, ok = s.cache.Get(s.cacheKey(blockIndex))
		}
		// cache hit, get block from the cache
		if ok {
			copy(bh.block, cachedBlock)
		} else {
			// cache miss, read from file
			if _, err := s.fd.ReadAt(bh.block[0:sz], offset); err != nil {
				return nil, nil, err
			}
			// cache the block, so that the next time it can be read from the cache.
			// if the block size is smaller than blockSize, meaning the block is not full,
			// so we will not cache it.
			if s.cache != nil && sz == blockSize {
				cacheBlock := make([]byte, blockSize)
				copy(cacheBlock, bh.block)
				s.cache.Add(s.cacheKey(blockIndex), cacheBlock)
			}
		}

		// header
		copy(bh.header, bh.block[chunkOffset:chunkOffset+chunkHeaderSize])

		// length
		length := binary.LittleEndian.Uint16(bh.header[4:6])

		// copy data
		start := chunkOffset + chunkHeaderSize
		end := start + int64(length)
		res = append(res, bh.block[start:end]...)

		// check sum
		checksum := crc32.ChecksumIEEE(bh.block[chunkOffset+4 : end])
		savedSum := binary.LittleEndian.Uint32(bh.header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// type
		chunkType := bh.header[6]

		// all chunks have been read
		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockIndex = blockIndex
			nextChunk.ChunkOffset = end
			// if this is the last chunk in the block, and the left is padding,
			// the next chunk should be in the next block.
			if end+chunkHeaderSize >= blockSize {
				nextChunk.BlockIndex++
				nextChunk.ChunkOffset = 0
			}
			break
		}

		blockIndex += 1
		chunkOffset = 0
	}

	return res, nextChunk, nil
}

func (s *segment) cacheKey(blockIndex uint32) uint64 {
	return uint64(s.id)<<32 | uint64(blockIndex)
}

func (sr *segmentReader) Next() ([]byte, *ChunkLoc, error) {
	if sr.seg.closed {
		return nil, nil, ErrClosed
	}

	// current chunk
	curChunk := &ChunkLoc{
		SegId:       sr.seg.id,
		BlockIndex:  sr.blockIdx,
		ChunkOffset: sr.chunkOff,
	}

	val, nextChunk, err := sr.seg.readInternal(sr.blockIdx, sr.chunkOff)
	if err != nil {
		return nil, nil, err
	}

	// estimated chunk size, paddings may exist between two chunks.
	curChunk.ChunkSize =
		nextChunk.BlockIndex*blockSize + uint32(nextChunk.ChunkOffset) -
			(curChunk.BlockIndex*blockSize + uint32(curChunk.ChunkOffset))

	// reader points to next position
	sr.blockIdx = nextChunk.BlockIndex
	sr.chunkOff = nextChunk.ChunkOffset

	return val, curChunk, nil
}

// Encode encodes a ChunkLoc into bytes.
// In reverse, decode it by wal.DecodeChunkLoc().
func (loc *ChunkLoc) Encode() []byte {
	b := make([]byte, binary.MaxVarintLen32*3+binary.MaxVarintLen64)

	var idx = 0
	// SegId
	idx += binary.PutUvarint(b[idx:], uint64(loc.SegId))
	// BlockIndex
	idx += binary.PutUvarint(b[idx:], uint64(loc.BlockIndex))
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
	blockIndex, n := binary.Uvarint(b[idx:])
	idx += n
	chunkOffset, n := binary.Uvarint(b[idx:])
	idx += n
	chunkSize, n := binary.Uvarint(b[idx:])

	return &ChunkLoc{
		SegId:       uint32(segId),
		BlockIndex:  uint32(blockIndex),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
