# yojoudb
A persistent key-value store based on Bitcask that supports the Redis protocol.

## Benchmarks

| Engine                                           | PUT                                | GET                                 | put + get | file size | peak sys mem |
|--------------------------------------------------|------------------------------------|-------------------------------------|-----------|-----------|--------------|
| [yojoudb](https://github.com/berylyvos/yojoudb)  | 13.771s  &nbsp;&nbsp; 145229 ops/s | 2.163s &nbsp;&nbsp;   924817 ops/s  | 15.934s   | 782.15MB  | 1.31GB       |
| [badger](https://github.com/dgraph-io/badger)    | 8.813s   &nbsp;&nbsp; 226930 ops/s | 4.939s  &nbsp;&nbsp;   404921 ops/s | 13.752s   | 250.95MB  | 3.60GB       |
| [pebble](https://github.com/cockroachdb/pebble)  | 13.594s  &nbsp;&nbsp; 147125 ops/s | 4.844s  &nbsp;&nbsp;   412882 ops/s | 18.438s   | 229.16MB  | 446.60MB     |
| [goleveldb](https://github.com/syndtr/goleveldb) | 25.199s  &nbsp;&nbsp;  79367 ops/s | 6.956s &nbsp;&nbsp;   287539 ops/s  | 32.155s   | 714.31MB  | 529.79MB     |
| [bbolt](https://github.com/etcd-io/bbolt)        | 84.245s  &nbsp;&nbsp;  23740 ops/s | 1.555s &nbsp;&nbsp;   1286247 ops/s | 85.800s   | 1.03GB    | 481.17MB     |

| key nums  | key size | val size  | concurrency |
|-----------|----------|-----------|-------------|
| 2000000   | 16 ~ 64  | 128 ~ 512 | 5           |

### Index

#### Adaptive-Radix-Tree

```
Benchmark_Put-8           161097            7469  ns/op            4604 B/op        9 allocs/op
Benchmark_Get-8          6997028            165.9 ns/op            72   B/op        3 allocs/op
Benchmark_Delete-8       7383976            162.9 ns/op            72   B/op        3 allocs/op
```

#### B-Tree

```
Benchmark_Put-8           162087            7637  ns/op           4620 B/op         10 allocs/op
Benchmark_Get-8          4419790            270.8 ns/op           104  B/op         4  allocs/op
Benchmark_Delete-8       4503111            265.8 ns/op           104  B/op         4  allocs/op
```

#### Skiplist

```
Benchmark_Put-8           120864            8586  ns/op            4728 B/op        12 allocs/op
Benchmark_Get-8          5000707            239.5 ns/op            96 B/op          4  allocs/op
Benchmark_Delete-8       5045925            237.7 ns/op            96 B/op          4  allocs/op
```