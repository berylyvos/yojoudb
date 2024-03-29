# yojoudb
An embeddable, fast and persistent key-value storage engine based on Bitcask.

## Getting Started

### Basic Example
```go
package main

import (
	"fmt"
	"github.com/berylyvos/yojoudb"
)

func main() {
	// specify the options
	options := yojoudb.DefaultOptions

	// open a database
	db, err := yojoudb.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	// put a key
	key := []byte("hello")
	if err = db.Put(key, []byte("yojoudb")); err != nil {
		panic(err)
	}

	// get a key
	val, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	println(string(val))

	// delete a key
	if err = db.Delete(key); err != nil {
		panic(err)
	}

	// create a batch
	batch := db.NewBatch(yojoudb.DefaultBatchOptions)

	// batch put keys/values
	for i := 0; i < 100; i++ {
		_ = batch.Put([]byte(fmt.Sprintf("#%d", i)), []byte(fmt.Sprintf("yojoudb-%d", i)))
	}

	// commit the batch
	_ = batch.Commit()

	// create an iterator
	iter := db.NewIterator(yojoudb.DefaultIteratorOptions)
	defer iter.Close()
	// iterate over all data
	for ; iter.Valid(); iter.Next() {
		v, _ := iter.Value()
		println(string(v))
	}
}
```

## Benchmarks

We compared how well yojoudb performs in **random writes** and **random point lookups** against several high-performing Golang-based key-value stores using the benchmarking tool [pogreb-bench](https://github.com/akrylysov/pogreb-bench).

### Performance Metrics

| Engine                                           | PUT                                | GET                                 | put + get | file size | peak sys mem |
|--------------------------------------------------|------------------------------------|-------------------------------------|-----------|-----------|--------------|
| [yojoudb](https://github.com/berylyvos/yojoudb)  | 13.771s  &nbsp;&nbsp; 145229 ops/s | 2.163s &nbsp;&nbsp;   924817 ops/s  | 15.934s   | 782.15MB  | 1.31GB       |
| [badger](https://github.com/dgraph-io/badger)    | 8.813s   &nbsp;&nbsp; 226930 ops/s | 4.939s  &nbsp;&nbsp;   404921 ops/s | 13.752s   | 250.95MB  | 3.60GB       |
| [pebble](https://github.com/cockroachdb/pebble)  | 13.594s  &nbsp;&nbsp; 147125 ops/s | 4.844s  &nbsp;&nbsp;   412882 ops/s | 18.438s   | 229.16MB  | 446.60MB     |
| [goleveldb](https://github.com/syndtr/goleveldb) | 25.199s  &nbsp;&nbsp;  79367 ops/s | 6.956s &nbsp;&nbsp;   287539 ops/s  | 32.155s   | 714.31MB  | 529.79MB     |
| [bbolt](https://github.com/etcd-io/bbolt)        | 84.245s  &nbsp;&nbsp;  23740 ops/s | 1.555s &nbsp;&nbsp;   1286247 ops/s | 85.800s   | 1.03GB    | 481.17MB     |

### Parameters

| key nums  | key size | val size  | concurrency |
|-----------|----------|-----------|-------------|
| 2000000   | 16 ~ 64  | 128 ~ 512 | 5           |

## In-memory Table

For yojoudb, we use [ART](https://github.com/plar/go-adaptive-radix-tree)(Adaptive Radix Tree) as the default in-memory table. Alternatively, other index types (B-tree, skiplist) can be specified by `yojoudb.DefaultOptions.IndexType` (`IndexBTree`, `IndexART`, `IndexSKL`).