package wal

import (
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"os"
	"sort"
	"sync"
)

const (
	initialSegmentFileID = 1
)

// BlockCache is alias for *lru.Cache[uint64, []byte].
type BlockCache = *lru.Cache[uint64, []byte]

// WAL (Write-Ahead Log) provides durability and fault-tolerance
// for incoming writes.
//
// It consists of an activeSeg, which is the current segment file
// used for new incoming writes, and olderSegs, which is a map of
// segment files used for read operations.
//
// The blockCache is a lru cache to store recently accessed  data
// blocks, improving reading performance by reducing the count of
// disk i/o.
type WAL struct {
	activeSeg  *segment               // active segment file, for reading and writing.
	olderSegs  map[SegmentID]*segment // older segment files, for reading only.
	options    Options
	mu         sync.RWMutex
	blockCache BlockCache
	bytesWrite uint32
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

	// create the block cache if needed
	if opt.BlockCacheSize > 0 {
		lruSize := int(opt.BlockCacheSize / blockSize)
		if opt.BlockCacheSize%blockSize != 0 {
			lruSize++
		}
		cache, err := lru.New[uint64, []byte](lruSize)
		if err != nil {
			return nil, err
		}
		wal.blockCache = cache
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
		seg, err := openSegmentFile(opt.DirPath, opt.SegmentFileExt,
			initialSegmentFileID, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.activeSeg = seg
	} else {
		// open segment files in order, the last one is active segment file
		sort.Ints(segIds)
		for i, sid := range segIds {
			seg, err := openSegmentFile(opt.DirPath, opt.SegmentFileExt,
				SegmentID(sid), wal.blockCache)
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
			w.activeSeg.id+1, w.blockCache)
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

// Close closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.blockCache != nil {
		w.blockCache.Purge()
	}

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

	if w.blockCache != nil {
		w.blockCache.Purge()
	}

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

func (w *WAL) isFull(delta int64) bool {
	return w.activeSeg.Size()+chunkHeaderSize+delta > w.options.SegmentSize
}
