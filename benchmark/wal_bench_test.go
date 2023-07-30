package benchmark

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"yojoudb/wal"
)

var w *wal.WAL

func init() {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	var err error
	w, err = wal.Open(opts)
	if err != nil {
		panic(err)
	}
}

func BenchmarkWAL_Write(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := w.Write([]byte("Hello World"))
		assert.Nil(b, err)
	}
}

// BenchmarkWAL_Read-8   	  271761	      4246 ns/op	   32814 B/op	       3 allocs/op
func BenchmarkWAL_Read(b *testing.B) {
	var positions []*wal.ChunkLoc
	for i := 0; i < 1000000; i++ {
		pos, err := w.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := w.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
}
