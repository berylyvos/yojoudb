package benchmark

import (
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/berylyvos/yojoudb/wal"
	"github.com/stretchr/testify/assert"
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

// BenchmarkWAL_Write-8   	  898642	      1354 ns/op	      40 B/op	       2 allocs/op
func BenchmarkWAL_Write(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := w.Write([]byte("Hello World"))
		assert.Nil(b, err)
	}
}

// BenchmarkWAL_Read-8   	  		  		  271761	      4246 ns/op	   32814 B/op	       3 allocs/op
// BenchmarkWAL_Read-8-With-Pool  	  		  658446	      1699 ns/op	      40 B/op	       2 allocs/op
// BenchmarkWAL_Read-8-With-Pool-LRU-32   	  240200	      4685 ns/op	   30900 B/op	       3 allocs/op
// BenchmarkWAL_Read-8-With-Pool-LRU-512  	  851979	      1463 ns/op	    2317 B/op	       2 allocs/op
// BenchmarkWAL_Read-8-With-Pool-LRU-1024	 1203106	       977.1 ns/op	      55 B/op	       2 allocs/op
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

// BenchmarkWAL_WriteLargeSize-8       9350        140549 ns/op      85 B/op      1 allocs/op
func BenchmarkWAL_WriteLargeSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	content := []byte(strings.Repeat("X", 256*wal.KB+500))
	for i := 0; i < b.N; i++ {
		_, err := w.Write(content)
		assert.Nil(b, err)
	}
}
