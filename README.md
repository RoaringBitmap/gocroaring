# gocroaring   [![GoDoc](https://godoc.org/github.com/RoaringBitmap/gocroaring?status.svg)](https://godoc.org/github.com/RoaringBitmap/gocroaring) [![Tests (Ubuntu, macOS)](https://github.com/RoaringBitmap/gocroaring/actions/workflows/test.yml/badge.svg)](https://github.com/RoaringBitmap/gocroaring/actions/workflows/test.yml)
Well-tested Go wrapper for [CRoaring](https://github.com/RoaringBitmap/CRoaring) (a C/C++ implementation of Roaring Bitmaps)

Roaring bitmaps are used by several important systems:

*   [Apache Lucene](http://lucene.apache.org/core/) and derivative systems such as Solr and [Elastic](https://www.elastic.co/),
*   Metamarkets' [Druid](http://druid.io/),
*   [Apache Spark](http://spark.apache.org),
*   [Netflix Atlas](https://github.com/Netflix/atlas),
*   [LinkedIn Pinot](https://github.com/linkedin/pinot/wiki),
*   [OpenSearchServer](http://www.opensearchserver.com),
*   [Cloud Torrent](https://github.com/jpillora/cloud-torrent),
*   [Whoosh](https://pypi.python.org/pypi/Whoosh/),
*   [Pilosa](https://www.pilosa.com/),
*   [Microsoft Visual Studio Team Services (VSTS)](https://www.visualstudio.com/team-services/),
*   eBay's [Apache Kylin](http://kylin.io).

Roaring bitmaps are found to work well in many important applications:

> Use Roaring for bitmap compression whenever possible. Do not use other bitmap compression methods ([Wang et al., SIGMOD 2017](http://db.ucsd.edu/wp-content/uploads/2017/03/sidm338-wangA.pdf))


The original java version can be found at https://github.com/RoaringBitmap/RoaringBitmap

There is a native Go version at https://github.com/RoaringBitmap/roaring


This code is licensed under Apache License, Version 2.0 (ASL2.0).

Copyright 2016 by the authors.


### Performance compared with the pure Go version

[roaring](https://github.com/RoaringBitmap/roaring) is a native Go
implementation of Roaring bitmaps. It avoids cgo, and it shares the same
serialization format, so the two libraries can read each other's bitmaps. Which
one is faster depends on how much work each call does:

- gocroaring is usually faster on whole-bitmap operations (unions,
  intersections, differences, aggregations of many bitmaps, flips,
  serialization), often by 2x to 10x. CRoaring uses SIMD instructions (AVX2,
  AVX-512, NEON) and allocates outside the Go heap.
- roaring is faster on operations that do little work per call, such as adding
  values one at a time. Each call into gocroaring crosses the Go/C boundary,
  which costs about 40 ns. Batch your calls (`Add(values...)`, `Iterator()`,
  `ToArray()`) to amortize it. To avoid that cost, gocroaring answers short
  read-only queries (`Contains`, `Rank`, `Minimum`, `Maximum`, `Select`,
  `Intersect`) in Go, reading the CRoaring data structures directly, on amd64
  and arm64.

The following timings come from `benchmarks/compare` and are medians of 6
runs on one core of a Xeon Gold 6548N (Linux, Go 1.26), comparing gocroaring
(CRoaring 5.5.3) and roaring 2.30.0. Results of gocroaring operations are
freed explicitly; roaring relies on the garbage collector, whose cost is
included.

| operation                                   | gocroaring | roaring | roaring time / gocroaring time |
|---------------------------------------------|-----------:|--------:|------:|
| `Intersects` that stops early (runs)         |    8.4 ns  |  369 ns | 44    |
| Xor of two bitmaps made of runs              |   0.38 ms  | 4.25 ms | 11.1  |
| AndNot, small bitmap against a large one     |     82 µs  |  897 µs | 10.9  |
| Or of 200 sparse bitmaps (`FastOr`)          |    9.1 ms  | 89.5 ms |  9.9  |
| Flip of a sparse bitmap                      |   0.28 ms  | 2.72 ms |  9.8  |
| Or, small bitmap with a large one            |    194 µs  | 1.33 ms |  6.9  |
| And of two 64-bit bitmaps                    |   0.40 ms  |  2.6 ms |  6.5  |
| Deserialize (portable format)                |     19 µs  |   73 µs |  3.8  |
| And of two sparse bitmaps                    |    531 µs  | 1.87 ms |  3.5  |
| Serialize (portable format)                  |     47 µs  |  116 µs |  2.5  |
| `Contains`                                   |     52 ns  |  112 ns |  2.2  |
| `ToArray` (1M values)                        |    638 µs  |  936 µs |  1.5  |
| Buffered iteration (1M values)               |    837 µs  |  962 µs |  1.15 |
| `Intersects` that stops early (bitsets)      |    6.3 ns  |  5.2 ns |  0.83 |
| `Add` one value at a time (50K random)       |   2.36 ms  |  864 µs |  0.37 |
| Or of two very sparse 64-bit bitmaps         |    216 ms  |   61 ms |  0.28 |
| `Add` one value at a time (50K sorted)       |   2.00 ms  |  305 µs |  0.15 |

Over all 58 benchmarks in `benchmarks/compare`, gocroaring is faster in 46,
roaring in 11, and they are within 5% of each other in 1; the geometric mean
of the ratio is 1.9 in favor of gocroaring. Your results depend on your data
and hardware, so measure with your own workload. To run the comparison:

```
cd benchmarks/compare
go test -run '^$' -bench . -count 10 > results.txt
```

See also https://github.com/lemire/gobitmapbenchmark.

### References

-  Daniel Lemire, Owen Kaser, Nathan Kurz, Luca Deri, Chris O'Hara, François Saint-Jacques, Gregory Ssi-Yan-Kai, Roaring Bitmaps: Implementation of an Optimized Software Library [arXiv:1709.07821](https://arxiv.org/abs/1709.07821)
-  Samy Chambi, Daniel Lemire, Owen Kaser, Robert Godin,
Better bitmap performance with Roaring bitmaps,
Software: Practice and Experience Volume 46, Issue 5, pages 709–719, May 2016
http://arxiv.org/abs/1402.6407 This paper used data from http://lemire.me/data/realroaring2014.html
- Daniel Lemire, Gregory Ssi-Yan-Kai, Owen Kaser, Consistently faster and smaller compressed bitmaps with Roaring, Software: Practice and Experience (accepted in 2016, to appear) http://arxiv.org/abs/1603.06549



### Requirements

Go 1.24 or better, and a C compiler (cgo). The CRoaring sources are bundled
with the package, so there is nothing else to install:

  - go get github.com/RoaringBitmap/gocroaring

The bundled CRoaring version is reported by `gocroaring.CRoaringVersion`.

### 32-bit and 64-bit bitmaps

The package wraps both C APIs. `gocroaring.Bitmap` stores 32-bit integers and
wraps `roaring_bitmap_t`; `gocroaring.Bitmap64` stores 64-bit integers and
wraps `roaring64_bitmap_t`. The two types offer the same operations, with the
free functions of the 64-bit type carrying a `64` suffix (`Or64`, `And64`,
`Read64`, and so on).

```go
rb := gocroaring.New64()
rb.AddRange(1<<40, 1<<40+1000)
rb.RunOptimize()
fmt.Println(rb.Cardinality(), rb.Contains(1<<40+5))
```

### Memory management

Bitmaps and iterators hold memory allocated by C. That memory is released
automatically once the Go value becomes unreachable: we register a cleanup with
`runtime.AddCleanup` rather than a finalizer, which lets the garbage collector
reclaim a bitmap in a single cycle instead of two, and makes creating and
discarding bitmaps about 20% cheaper than the finalizer-based approach we used
previously. You may still call `Free` to release the C memory eagerly; doing so
cancels the cleanup, and calling `Free` twice is harmless.

Iteration reads values from C in blocks rather than one at a time, so walking a
bitmap through `Iterator()` is more than an order of magnitude faster than
paying for a crossing of the Go/C boundary per value. Use `NewIterator()` when
you need to move backwards or seek.

The CRoaring entry points are declared `#cgo nocallback` (none of them calls
back into Go) and, where the function does not keep the buffer it is given,
`#cgo noescape` (so the Go buffers we hand to C are not forced onto the heap).
The frozen views are deliberately left out of the `noescape` list, since they
do retain their buffer.


### Example

Here is a simplified but complete example:

```go
package main

import (
	"fmt"

	"github.com/RoaringBitmap/gocroaring"
)

func main() {
	// example inspired by https://github.com/fzandona/goroar
	fmt.Println("==roaring==")
	rb1 := gocroaring.New(1, 2, 3, 4, 5, 100, 1000)
	rb1.RunOptimize() // improves compression
	fmt.Println("Cardinality: ", rb1.Cardinality())
	fmt.Println("Contains 3? ", rb1.Contains(3))

	rb2 := gocroaring.New()
	rb2.Add(3, 4, 1000)
	rb2.RunOptimize() // improves compression

	rb1.And(rb2)
	// prints {3,4,1000}
	fmt.Println(rb1)

	rb3 := gocroaring.New(1, 5)
	rb3.Or(rb1)

        // prints 1, 3, 4, 5, 1000
        i := rb3.Iterator()
        for i.HasNext() {
          fmt.Println(i.Next())
        }
        fmt.Println()

	fmt.Println(rb3.ToArray())
	fmt.Println(rb3)

	rb4 := gocroaring.FastOr(rb1, rb2, rb3) // optimized way to compute unions between many bitmaps
	fmt.Println(rb4)

	// next we include an example of serialization
	buf := make([]byte, rb1.SerializedSizeInBytes())
	rb1.Write(buf) // we omit error handling
	newrb, _ := gocroaring.Read(buf)
	if rb1.Equals(newrb) {
		fmt.Println("I wrote the content to a byte stream and read it back.")
	}

	fmt.Println(rb1.Stats()) // show the cardinality and the numbers of each type of container used.
}
```

### Documentation

Current documentation is available at http://godoc.org/github.com/RoaringBitmap/gocroaring


### Mailing list/discussion group

https://groups.google.com/forum/#!forum/roaring-bitmaps

### Compatibility with Java RoaringBitmap library

You can read bitmaps in Go, Java, C, C++ that have been serialized in Go, Java, C, C++.
