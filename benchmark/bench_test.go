package benchmark

import (
	"github.com/berylyvos/yojoudb"
	"github.com/berylyvos/yojoudb/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	KeyNum     = 1000000
	MaxValSize = 512
)

var db *yojoudb.DB

func init() {
	options := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-bench-")
	options.DirPath = dir

	var err error
	db, err = yojoudb.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(MaxValSize))
		assert.Nil(b, err)
	}
}

func Benchmark_Seq_Get(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.TestKey(i))
		if err != nil && err != yojoudb.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Random_Get(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.TestKey(rand.Intn(KeyNum)))
		if err != nil && err != yojoudb.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.TestKey(rand.Intn(KeyNum)))
		assert.Nil(b, err)
	}
}

func Benchmark_Destroy(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	if db != nil {
		_ = db.Close()
		err := os.RemoveAll(db.GetDirPath())
		if err != nil {
			panic(err)
		}
	}
}
