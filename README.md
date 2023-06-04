# yojoudb
A persistent key-value store based on Bitcask that supports the Redis protocol.

## Benchmarks

### Adaptive-Radix-Tree

```
Benchmark_Put-8           161097               7469  ns/op            4604 B/op        9 allocs/op
Benchmark_Get-8          6997028               165.9 ns/op            72 B/op          3 allocs/op
Benchmark_Delete-8       7383976               162.9 ns/op            72 B/op          3 allocs/op
```

### B-Tree

```
Benchmark_Put-8           162087               7637  ns/op           4620 B/op         10 allocs/op
Benchmark_Get-8          4419790               270.8 ns/op           104 B/op          4 allocs/op
Benchmark_Delete-8       4503111               265.8 ns/op           104 B/op          4 allocs/op
```