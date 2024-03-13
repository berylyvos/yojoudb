package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

const (
	initialSegmentID = 1
)

// [deprecated] BlockCache is alias for *lru.Cache[uint64, []byte].
// type BlockCache = *lru.Cache[uint64, []byte]

// WAL (Write-Ahead Log) provides durability and fault-tolerance
// for incoming writes.
//
// It consists of an activeSeg, which is the current segment file
// used for new incoming writes, and olderSegs, which is a map of
// segment files used for read operations.
type WAL struct {
	activeSeg  *segment               // active segment file, for reading and writing.
	olderSegs  map[SegmentID]*segment // older segment files, for reading only.
	options    Options
	mu         sync.RWMutex
	bytesWrite uint32
}

// Reader represents a reader for WAL.
// The readers are *segmentReader for every segment,
// sorted by segment id.
// And the currIdx points to current segment reader.
type Reader struct {
	readers []*segmentReader
	currIdx int
}

func Open(opt Options) (*WAL, error) {
	wal := &WAL{
		options:   opt,
		olderSegs: make(map[SegmentID]*segment),
	}

	// create directory if not exists
	if err := os.MkdirAll(opt.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	// iterate the dir and open all segment files
	entries, err := os.ReadDir(opt.DirPath)
	if err != nil {
		return nil, err
	}

	var segIds []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err = fmt.Sscanf(entry.Name(), "%d"+opt.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		segIds = append(segIds, id)
	}

	// empty dir, just initialize a new segment file
	if len(segIds) == 0 {
		seg, err := openSegmentFile(opt.DirPath, opt.SegmentFileExt, initialSegmentID)
		if err != nil {
			return nil, err
		}
		wal.activeSeg = seg
	} else {
		// open segment files in order, the last one is active segment file
		sort.Ints(segIds)
		for i, sid := range segIds {
			seg, err := openSegmentFile(opt.DirPath, opt.SegmentFileExt, SegmentID(sid))
			if err != nil {
				return nil, err
			}
			if i == len(segIds)-1 {
				wal.activeSeg = seg
			} else {
				wal.olderSegs[seg.id] = seg
			}
		}
	}

	return wal, nil
}

// OpenNewActiveSeg opens a new segment file and sets it
// as the active segment file regardless of the old one.
// Calling it in merge process.
func (w *WAL) OpenNewActiveSeg() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.activeSeg.Sync(); err != nil {
		return err
	}
	seg, err := openSegmentFile(w.options.DirPath,
		w.options.SegmentFileExt, w.activeSeg.id+1)
	if err != nil {
		return err
	}
	w.olderSegs[w.activeSeg.id] = w.activeSeg
	w.activeSeg = seg
	return nil
}

// Write writes the data to the WAL.
// Actually, it writes the data to the active segment file.
// Returns the location of the data in the WAL.
func (w *WAL) Write(data []byte) (*ChunkLoc, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	dataSize := int64(len(data))
	if dataSize+chunkHeaderSize > w.options.SegmentSize {
		return nil, errors.New("the data size larger than max segment size")
	}

	// if the active segment file is full, sync it and create a new one.
	if w.isFull(dataSize) {
		if err := w.activeSeg.Sync(); err != nil {
			return nil, err
		}
		w.bytesWrite = 0
		seg, err := openSegmentFile(w.options.DirPath, w.options.SegmentFileExt,
			w.activeSeg.id+1)
		if err != nil {
			return nil, err
		}
		w.olderSegs[w.activeSeg.id] = w.activeSeg
		w.activeSeg = seg
	}

	// write the data to the active segment file
	loc, err := w.activeSeg.Write(data)
	if err != nil {
		return nil, err
	}

	w.bytesWrite += loc.ChunkSize

	// sync the active segment file if needed
	var needSync = w.options.Sync
	if !needSync && w.options.BytesPerSync > 0 {
		needSync = w.bytesWrite >= w.options.BytesPerSync
	}
	if needSync {
		if err = w.activeSeg.Sync(); err != nil {
			return nil, err
		}
		w.bytesWrite = 0
	}

	return loc, nil
}

// Read reads the data in the given chunk location from the WAL.
func (w *WAL) Read(loc *ChunkLoc) ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var seg *segment
	if loc.SegId == w.activeSeg.id {
		seg = w.activeSeg
	} else {
		seg = w.olderSegs[loc.SegId]
	}

	if seg == nil {
		return nil, fmt.Errorf("segment file %d%s not found", loc.SegId, w.options.SegmentFileExt)
	}

	// read the data from the segment file
	return seg.Read(loc.BlockIndex, loc.ChunkOffset)
}

// NewReaderLE returns a new reader for WAL which only read
// data from the segment whose id is less than or equal to
// the given segId.
// If segId is 0, meaning read from all segments.
// You may also call it in the merge process.
func (w *WAL) NewReaderLE(segId SegmentID) *Reader {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var readers []*segmentReader
	for _, seg := range w.olderSegs {
		if segId == 0 || seg.id <= segId {
			readers = append(readers, seg.NewReader())
		}
	}
	if segId == 0 || w.activeSeg.id <= segId {
		readers = append(readers, w.activeSeg.NewReader())
	}

	sort.Slice(readers, func(i, j int) bool {
		return readers[i].seg.id < readers[j].seg.id
	})

	return &Reader{
		readers: readers,
		currIdx: 0,
	}
}

// NewReaderWithLoc returns a new reader for WAL which only read
// data from the given chunk location.
func (w *WAL) NewReaderWithLoc(loc *ChunkLoc) (*Reader, error) {
	if loc == nil {
		return nil, errors.New("start location is nil")
	}
	w.mu.RLock()
	defer w.mu.RUnlock()

	reader := w.NewReader()
	for {
		// skip the segment whose id is less than loc.SegId
		if reader.CurrSegId() < loc.SegId {
			reader.Skip()
			continue
		}
		// skip the chunk whose location is less than loc
		curLoc := reader.CurrChunkLoc()
		if curLoc.BlockIndex >= loc.BlockIndex &&
			curLoc.ChunkOffset >= loc.ChunkOffset {
			break
		}
		// call Next()
		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	return reader, nil
}

// NewReader returns a new reader for WAL which read all segments.
func (w *WAL) NewReader() *Reader {
	return w.NewReaderLE(0)
}

// Skip skips the current segment.
func (r *Reader) Skip() {
	r.currIdx++
}

// CurrSegId returns the id of current segment.
func (r *Reader) CurrSegId() SegmentID {
	return r.readers[r.currIdx].seg.id
}

// CurrChunkLoc returns the location of current chunk.
func (r *Reader) CurrChunkLoc() *ChunkLoc {
	curReader := r.readers[r.currIdx]
	return &ChunkLoc{
		SegId:       curReader.seg.id,
		BlockIndex:  curReader.blockIdx,
		ChunkOffset: curReader.chunkOff,
	}
}

// Next returns the next chunk data with location.
// If there's no data, io.EOF will be returned.
func (r *Reader) Next() ([]byte, *ChunkLoc, error) {
	if r.currIdx >= len(r.readers) {
		return nil, nil, io.EOF
	}

	data, loc, err := r.readers[r.currIdx].Next()
	if err == io.EOF {
		r.currIdx++
		return r.Next()
	}
	return data, loc, err
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seg := range w.olderSegs {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	w.olderSegs = nil

	return w.activeSeg.Close()
}

// Delete deletes all segment files of the WAL.
func (w *WAL) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, seg := range w.olderSegs {
		if err := seg.Remove(); err != nil {
			return err
		}
	}
	w.olderSegs = nil

	return w.activeSeg.Remove()
}

// Sync syncs the active segment file to stable storage like disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.activeSeg.Sync()
}

// ActiveSegID returns the current active segment id.
func (w *WAL) ActiveSegID() SegmentID {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.activeSeg.id
}

// IsEmpty returns whether the WAL is empty.
// Only when there is only one active segment, and it's empty.
func (w *WAL) IsEmpty() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return len(w.olderSegs) == 0 && w.activeSeg.Size() == 0
}

func (w *WAL) isFull(delta int64) bool {
	return w.activeSeg.Size()+chunkHeaderSize+delta > w.options.SegmentSize
}
