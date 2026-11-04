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
one is faster depends on how much work each call does.

**gocroaring is faster on operations that do a lot of work per call.**
CRoaring uses SIMD instructions (AVX2, AVX-512, NEON) and allocates outside
the Go heap.
- 32-bit unions, intersections, differences and symmetric differences: 2x to 3x
  on average, and 10x to 17x on some shapes (runs, or a small bitmap against a
  large one). An in-place union of two bitmaps made of runs is over 100x
  faster.
- Union of many bitmaps (`FastOr`): 5x to 10x.
- Counting the size of a union without building it (`OrCardinality`): 1.5x to
  12x.
- `Flip`, `RunOptimize`, `Equals`, `Clone`, serialization and deserialization:
  1.3x to 10x. Opening a serialized bitmap without copying it
  (`ReadPortableFrozenView`, compared with roaring's `FromBuffer`): 10x.
- 64-bit set operations on clustered or dense values: 4x to 12x on x86-64 (but
  about even on the Apple M4).

**roaring is faster on operations that do little work per call.** Each call
into gocroaring crosses the Go/C boundary, which costs about 40 ns.
- Adding 32-bit values one at a time is 2.5x to 7x slower with gocroaring.
  Pass them all at once (`Add(values...)`, `New(values...)`): then gocroaring
  is as fast or up to 1.3x faster.
- Moving a `NewIterator()` one value per call (`Next`, `Previous`) is 5x to 20x
  slower. Read values in blocks instead (`Iterator()`, or `Read` and
  `ReadBackward` on a `NewIterator()`, or `ToArray`): then gocroaring is
  usually as fast as roaring or faster, though forward iteration over 32-bit
  values is 2x slower on the Apple M4.
- Several 64-bit queries are slower in gocroaring: `Rank` (4x to 14x),
  `Minimum`/`Maximum` (about 15x), `Intersects` (about 8x) and, on sparse
  data, `Select` (2x to 4x). CRoaring walks its 64-bit tree linearly for rank
  and select.
- 64-bit bitmaps with one value per 2^32 range (very sparse): most operations
  are 2x to 4x slower in gocroaring.

To reduce the cost of crossing into C, gocroaring answers short read-only
32-bit queries (`Contains`, `Rank`, `Minimum`, `Maximum`, `Select`,
`Intersect`) in Go, reading the CRoaring data structures directly, on amd64
and arm64. On the Xeon below, `Contains` is then about 2x faster than in
roaring. On the Apple M4, roaring remains up to 1.6x faster for `Contains`.

The numbers below come from the 190 benchmarks of `benchmarks/compare`.
Each one runs the same work in both libraries, on the same data. They
compare gocroaring (CRoaring 5.5.5) with roaring 2.30.0 on two machines:

- x86-64: one core of a Xeon Gold 6548N (Emerald Rapids, AVX-512), Linux,
  Go 1.26.10;
- arm64: an Apple M4 Max, macOS, Go 1.24.3.

Times are medians of 10 runs. Results of gocroaring operations are freed
explicitly; roaring relies on the garbage collector, whose cost is included.
The *ratio* is the roaring time divided by the gocroaring time: above 1,
gocroaring is faster (in bold); below 1, roaring is faster. The
wins–losses columns count the benchmarks where gocroaring is faster, and
slower, by more than 5%. Each category is summarized by the geometric mean
of its ratios; click a category below for its benchmarks.

| category | benchmarks | x86-64 geometric mean | x86-64 wins–losses | arm64 geometric mean | arm64 wins–losses |
|---|--:|--:|--:|--:|--:|
| 32-bit binary operations, new result | 24 | 2.73 | 24–0 | 2.38 | 23–1 |
| 32-bit in-place operations | 25 | 2.51 | 25–0 | 2.64 | 23–1 |
| 32-bit counts and intersection tests | 18 | 2.57 | 11–6 | 2.32 | 12–4 |
| Aggregating many bitmaps | 2 | 4.02 | 2–0 | 2.48 | 2–0 |
| 32-bit point queries | 18 | 1.43 | 13–2 | 1.08 | 6–9 |
| 32-bit whole-bitmap queries | 6 | 2.16 | 6–0 | 2.52 | 6–0 |
| 32-bit updates and construction | 12 | 1.04 | 7–4 | 0.78 | 5–6 |
| 32-bit iteration | 11 | 0.65 | 5–5 | 0.65 | 5–5 |
| 32-bit serialization and zero-copy views | 4 | 3.84 | 4–0 | 2.78 | 3–0 |
| 64-bit queries | 19 | 0.45 | 8–8 | 0.48 | 5–14 |
| 64-bit iteration, construction and serialization | 28 | 0.87 | 15–10 | 0.98 | 17–11 |
| 64-bit set operations | 23 | 1.75 | 13–10 | 0.63 | 9–9 |
| **all** | 190 | **1.47** | 133–45 | **1.23** | 116–60 |

<details>
<summary>32-bit binary operations, new result (24 benchmarks)</summary>

Each pair of bitmaps is one of six shapes: ArrayArray (200k values each over 2^28), SmallArrayBigArray (2k against 900k over 2^24), BitsetBitset (2M each over 2^22), ArrayBitset (30k against 2M over 2^22), RunRun (20k runs each over 2^26) and RunArray.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| And / ArrayArray | 532 µs | 1.86 ms | **3.50** | 932 µs | 1.07 ms | **1.15** |
| And / SmallArrayBigArray | 41.9 µs | 120 µs | **2.87** | 36 µs | 30.1 µs | 0.84 |
| And / BitsetBitset | 32.7 µs | 83.5 µs | **2.55** | 27.9 µs | 50.9 µs | **1.83** |
| And / ArrayBitset | 26.4 µs | 44.9 µs | **1.70** | 18 µs | 30.2 µs | **1.68** |
| And / RunRun | 314 µs | 641 µs | **2.04** | 95.5 µs | 343 µs | **3.59** |
| And / RunArray | 626 µs | 686 µs | **1.09** | 200 µs | 263 µs | **1.31** |
| Or / ArrayArray | 1.22 ms | 2.36 ms | **1.93** | 997 µs | 1.19 ms | **1.19** |
| Or / SmallArrayBigArray | 191 µs | 1.25 ms | **6.53** | 82.4 µs | 431 µs | **5.23** |
| Or / BitsetBitset | 31.2 µs | 79.4 µs | **2.55** | 21.5 µs | 45.9 µs | **2.13** |
| Or / ArrayBitset | 39.6 µs | 117 µs | **2.94** | 37 µs | 55.4 µs | **1.50** |
| Or / RunRun | 333 µs | 620 µs | **1.86** | 117 µs | 253 µs | **2.17** |
| Or / RunArray | 759 µs | 1.51 ms | **1.99** | 369 µs | 878 µs | **2.38** |
| Xor / ArrayArray | 1.21 ms | 2.15 ms | **1.78** | 1.03 ms | 1.41 ms | **1.37** |
| Xor / SmallArrayBigArray | 189 µs | 1.11 ms | **5.91** | 80.7 µs | 847 µs | **10** |
| Xor / BitsetBitset | 32.2 µs | 84.8 µs | **2.63** | 22.4 µs | 49.9 µs | **2.23** |
| Xor / ArrayBitset | 55.4 µs | 120 µs | **2.17** | 40.5 µs | 58.3 µs | **1.44** |
| Xor / RunRun | 380 µs | 4.3 ms | **11** | 118 µs | 2.02 ms | **17** |
| Xor / RunArray | 1.7 ms | 4.68 ms | **2.76** | 984 µs | 2.75 ms | **2.79** |
| AndNot / ArrayArray | 788 µs | 2.02 ms | **2.56** | 920 µs | 1.27 ms | **1.38** |
| AndNot / SmallArrayBigArray | 82.5 µs | 696 µs | **8.44** | 53.4 µs | 583 µs | **11** |
| AndNot / BitsetBitset | 30.2 µs | 82.8 µs | **2.74** | 21.9 µs | 51.9 µs | **2.37** |
| AndNot / ArrayBitset | 28.3 µs | 43.1 µs | **1.52** | 17.9 µs | 24.9 µs | **1.39** |
| AndNot / RunRun | 324 µs | 458 µs | **1.41** | 113 µs | 178 µs | **1.58** |
| AndNot / RunArray | 1.17 ms | 4.27 ms | **3.65** | 579 µs | 2.59 ms | **4.47** |

</details>

<details>
<summary>32-bit in-place operations (25 benchmarks)</summary>

The first bitmap is cloned, then modified in place; the clone is part of the measured work in both libraries. OrInPlace merges 500 values into a copy of 2M values.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| And / ArrayArray | 794 µs | 1.99 ms | **2.51** | 992 µs | 1.12 ms | **1.13** |
| Or / ArrayArray | 1.76 ms | 2.53 ms | **1.44** | 1.28 ms | 1.33 ms | 1.04 |
| Xor / ArrayArray | 1.52 ms | 2.34 ms | **1.53** | 1.17 ms | 1.52 ms | **1.30** |
| AndNot / ArrayArray | 1.64 ms | 2.07 ms | **1.26** | 963 µs | 1.33 ms | **1.38** |
| And / SmallArrayBigArray | 52 µs | 121 µs | **2.33** | 35.3 µs | 30.7 µs | 0.87 |
| Or / SmallArrayBigArray | 732 µs | 1.32 ms | **1.80** | 89.9 µs | 458 µs | **5.10** |
| Xor / SmallArrayBigArray | 298 µs | 1.13 ms | **3.79** | 86.5 µs | 830 µs | **9.60** |
| AndNot / SmallArrayBigArray | 83.8 µs | 691 µs | **8.24** | 52.4 µs | 581 µs | **11** |
| And / BitsetBitset | 41.5 µs | 92.6 µs | **2.23** | 31 µs | 53 µs | **1.71** |
| Or / BitsetBitset | 38.5 µs | 91.6 µs | **2.38** | 24.3 µs | 52 µs | **2.14** |
| Xor / BitsetBitset | 37.8 µs | 94.6 µs | **2.50** | 24.3 µs | 58.4 µs | **2.40** |
| AndNot / BitsetBitset | 38.1 µs | 94.1 µs | **2.47** | 24.2 µs | 59 µs | **2.43** |
| And / ArrayBitset | 30.2 µs | 42.7 µs | **1.41** | 20.6 µs | 25.9 µs | **1.25** |
| Or / ArrayBitset | 47.3 µs | 126 µs | **2.67** | 40.5 µs | 55.9 µs | **1.38** |
| Xor / ArrayBitset | 61.3 µs | 109 µs | **1.77** | 42.2 µs | 72.1 µs | **1.71** |
| AndNot / ArrayBitset | 33.4 µs | 40.9 µs | **1.22** | 18.1 µs | 25.6 µs | **1.41** |
| And / RunRun | 375 µs | 697 µs | **1.86** | 132 µs | 369 µs | **2.79** |
| Or / RunRun | 391 µs | 47.4 ms | **121** | 156 µs | 28 ms | **179** |
| Xor / RunRun | 451 µs | 3.04 ms | **6.76** | 142 µs | 1.55 ms | **11** |
| AndNot / RunRun | 390 µs | 503 µs | **1.29** | 143 µs | 200 µs | **1.40** |
| And / RunArray | 688 µs | 725 µs | **1.05** | 253 µs | 290 µs | **1.15** |
| Or / RunArray | 823 µs | 1.5 ms | **1.82** | 393 µs | 880 µs | **2.24** |
| Xor / RunArray | 1.82 ms | 3.52 ms | **1.93** | 1 ms | 2.53 ms | **2.52** |
| AndNot / RunArray | 1.31 ms | 6.09 ms | **4.64** | 617 µs | 4.17 ms | **6.75** |
| OrInPlace | 1.07 ms | 1.23 ms | **1.15** | 436 µs | 511 µs | **1.17** |

</details>

<details>
<summary>32-bit counts and intersection tests (18 benchmarks)</summary>

Same pairs as the binary operations. Intersects stops at the first common value.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| OrCardinality / ArrayArray | 426 µs | 2.23 ms | **5.24** | 762 µs | 1.17 ms | **1.54** |
| OrCardinality / SmallArrayBigArray | 102 µs | 1.21 ms | **12** | 55.1 µs | 434 µs | **7.87** |
| OrCardinality / BitsetBitset | 8.11 µs | 74.9 µs | **9.24** | 11.7 µs | 44.3 µs | **3.79** |
| OrCardinality / ArrayBitset | 19.8 µs | 111 µs | **5.61** | 11 µs | 52.2 µs | **4.73** |
| OrCardinality / RunRun | 235 µs | 624 µs | **2.66** | 74.6 µs | 267 µs | **3.58** |
| OrCardinality / RunArray | 546 µs | 1.55 ms | **2.84** | 116 µs | 951 µs | **8.20** |
| AndCardinality / ArrayArray | 410 µs | 1.67 ms | **4.06** | 751 µs | 933 µs | **1.24** |
| AndCardinality / SmallArrayBigArray | 102 µs | 98.7 µs | 0.97 | 54.2 µs | 22.7 µs | 0.42 |
| AndCardinality / BitsetBitset | 7.98 µs | 7.55 µs | 0.95 | 11.6 µs | 12.1 µs | 1.04 |
| AndCardinality / ArrayBitset | 19.8 µs | 21.6 µs | **1.09** | 10.9 µs | 15 µs | **1.37** |
| AndCardinality / RunRun | 202 µs | 548 µs | **2.72** | 62.8 µs | 277 µs | **4.41** |
| AndCardinality / RunArray | 516 µs | 397 µs | 0.77 | 116 µs | 181 µs | **1.57** |
| Intersects / ArrayArray | 178 ns | 138 ns | 0.77 | 117 ns | 69.3 ns | 0.59 |
| Intersects / SmallArrayBigArray | 69.2 ns | 44.7 ns | 0.65 | 50.2 ns | 25.5 ns | 0.51 |
| Intersects / BitsetBitset | 6.22 ns | 4.96 ns | 0.80 | 4.32 ns | 3.51 ns | 0.81 |
| Intersects / ArrayBitset | 6.77 ns | 6.19 ns | 0.92 | 4.47 ns | 4.64 ns | 1.04 |
| Intersects / RunRun | 8.54 ns | 361 ns | **42** | 5.76 ns | 233 ns | **40** |
| Intersects / RunArray | 27.3 ns | 210 ns | **7.67** | 15.6 ns | 177 ns | **11** |

</details>

<details>
<summary>Aggregating many bitmaps (2 benchmarks)</summary>

Union and symmetric difference of 200 bitmaps of 5000 values each.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| FastOr | 8.27 ms | 87.4 ms | **11** | 9.23 ms | 47.2 ms | **5.11** |
| FastXor | 44.4 ms | 68 ms | **1.53** | 35.2 ms | 42.4 ms | **1.20** |

</details>

<details>
<summary>32-bit point queries (18 benchmarks)</summary>

Small: 1000 values in 4 containers; Sparse: 1M values over 2^26 (array containers); Dense: 2M over 2^22 (bitset containers); Runs: 20k runs. Probes cycle through 4096 values, half present. The rows without a shape use 1M values over 2^26.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| Contains / Small | 19.2 ns | 47.8 ns | **2.49** | 12.1 ns | 11.6 ns | 0.96 |
| Rank / Small | 24.1 ns | 46.8 ns | **1.94** | 14.7 ns | 12.3 ns | 0.84 |
| Select / Small | 16.6 ns | 13.3 ns | 0.80 | 8.69 ns | 5.62 ns | 0.65 |
| MinMax / Small | 4.47 ns | 4.65 ns | 1.04 | 3.9 ns | 3.87 ns | 0.99 |
| Contains / Sparse | 52.8 ns | 121 ns | **2.29** | 42.5 ns | 26.3 ns | 0.62 |
| Rank / Sparse | 677 ns | 980 ns | **1.45** | 372 ns | 678 ns | **1.82** |
| Select / Sparse | 516 ns | 830 ns | **1.61** | 348 ns | 972 ns | **2.79** |
| MinMax / Sparse | 4.5 ns | 4.63 ns | 1.03 | 4.07 ns | 3.88 ns | 0.95 |
| Contains / Dense | 16.1 ns | 30.6 ns | **1.91** | 11.4 ns | 8.1 ns | 0.71 |
| Rank / Dense | 291 ns | 106 ns | 0.36 | 144 ns | 96.4 ns | 0.67 |
| Select / Dense | 275 ns | 306 ns | **1.12** | 162 ns | 340 ns | **2.10** |
| MinMax / Dense | 4.93 ns | 5.61 ns | **1.14** | 3.98 ns | 3.46 ns | 0.87 |
| Contains / Runs | 42 ns | 83.6 ns | **1.99** | 27.4 ns | 20.9 ns | 0.76 |
| Rank / Runs | 3.48 µs | 9.43 µs | **2.71** | 1.83 µs | 3.37 µs | **1.84** |
| Select / Runs | 3.47 µs | 6.53 µs | **1.88** | 1.84 µs | 3.32 µs | **1.80** |
| MinMax / Runs | 5.04 ns | 4.99 ns | 0.99 | 4.33 ns | 3.93 ns | 0.91 |
| Contains | 52.8 ns | 116 ns | **2.20** | 33.2 ns | 24.7 ns | 0.74 |
| Rank | 668 ns | 966 ns | **1.45** | 368 ns | 650 ns | **1.77** |

</details>

<details>
<summary>32-bit whole-bitmap queries (6 benchmarks)</summary>

Equals compares a bitmap with its clone.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| Equals / Small | 107 ns | 365 ns | **3.41** | 80.7 ns | 305 ns | **3.79** |
| Equals / Sparse | 158 µs | 337 µs | **2.13** | 70.1 µs | 277 µs | **3.95** |
| Equals / Dense | 7.97 µs | 25.6 µs | **3.22** | 12.4 µs | 18.6 µs | **1.51** |
| Equals / Runs | 12.3 µs | 20.9 µs | **1.70** | 4.76 µs | 12.7 µs | **2.67** |
| View / Cardinality | 235 ns | 385 ns | **1.63** | 122 ns | 233 ns | **1.91** |
| Cardinality | 939 ns | 1.49 µs | **1.58** | 385 ns | 859 ns | **2.23** |

</details>

<details>
<summary>32-bit updates and construction (12 benchmarks)</summary>

RemoveAdd removes a present value and adds it back. AddOneByOne adds 50k values with one call each; AddMany builds from a slice of 1M values in one call. AddRemoveRange adds and removes 100k consecutive values.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| RemoveAdd / Small | 132 ns | 105 ns | 0.80 | 127 ns | 37.7 ns | 0.30 |
| RemoveAdd / Sparse | 239 ns | 240 ns | 1.00 | 201 ns | 93.3 ns | 0.46 |
| RemoveAdd / Dense | 89.7 ns | 38.4 ns | 0.43 | 86.5 ns | 22.9 ns | 0.26 |
| RemoveAdd / Runs | 189 ns | 219 ns | **1.16** | 149 ns | 97.4 ns | 0.65 |
| Ranges32 / AddRemoveRange | 198 ns | 333 ns | **1.69** | 176 ns | 212 ns | **1.20** |
| AddOneByOne / Random | 2.41 ms | 940 µs | 0.39 | 1.93 ms | 505 µs | 0.26 |
| AddOneByOne / Sorted | 2 ms | 305 µs | 0.15 | 1.56 ms | 215 µs | 0.14 |
| AddMany / Random | 97.1 ms | 127 ms | **1.31** | 72.8 ms | 85.9 ms | **1.18** |
| AddMany / Sorted | 3.07 ms | 3.33 ms | **1.08** | 2.21 ms | 2.19 ms | 0.99 |
| Flip | 277 µs | 2.67 ms | **9.65** | 171 µs | 1.19 ms | **6.94** |
| RunOptimize | 2.59 ms | 6 ms | **2.32** | 1.22 ms | 4.06 ms | **3.34** |
| Clone | 303 µs | 406 µs | **1.34** | 85.6 µs | 145 µs | **1.69** |

</details>

<details>
<summary>32-bit iteration (11 benchmarks)</summary>

IterateBuffered and ReverseIterate read values in blocks of 256 (gocroaring Iterator / ReadBackward, roaring ManyIterator / ReverseIterator). ReverseIterateStepwise moves one value per call (gocroaring Iterator.Previous).

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| ReverseIterate / Small | 2.62 µs | 4.99 µs | **1.90** | 1.48 µs | 2.88 µs | **1.95** |
| ReverseIterateStepwise / Small | 38.8 µs | 4.98 µs | 0.13 | 30.8 µs | 2.88 µs | 0.09 |
| ReverseIterate / Sparse | 2.15 ms | 4.9 ms | **2.28** | 1.15 ms | 2.77 ms | **2.40** |
| ReverseIterateStepwise / Sparse | 38.3 ms | 4.88 ms | 0.13 | 30.9 ms | 2.75 ms | 0.09 |
| ReverseIterate / Dense | 6.88 ms | 10.6 ms | **1.54** | 2.83 ms | 7.88 ms | **2.79** |
| ReverseIterateStepwise / Dense | 60.2 ms | 10.6 ms | 0.18 | 49.9 ms | 7.9 ms | 0.16 |
| ReverseIterate / Runs | 11.7 ms | 25.5 ms | **2.17** | 4.08 ms | 15.2 ms | **3.72** |
| ReverseIterateStepwise / Runs | 181 ms | 25.3 ms | 0.14 | 153 ms | 15.2 ms | 0.10 |
| IterateBuffered | 830 µs | 853 µs | 1.03 | 1.36 ms | 642 µs | 0.47 |
| IterateCallback | 2.27 ms | 2.06 ms | 0.90 | 1.15 ms | 1.12 ms | 0.97 |
| ToArray | 621 µs | 946 µs | **1.52** | 106 µs | 342 µs | **3.23** |

</details>

<details>
<summary>32-bit serialization and zero-copy views (4 benchmarks)</summary>

Both libraries write the same portable format (checked byte for byte). Views open a serialized bitmap without copying it: ReadPortableFrozenView in gocroaring, FromBuffer in roaring.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| View / Open | 629 ns | 6.37 µs | **10** | 351 ns | 3.83 µs | **11** |
| View / Contains | 16.7 ns | 37.7 ns | **2.26** | 10.5 ns | 10.2 ns | 0.98 |
| Serialize | 48.9 µs | 118 µs | **2.41** | 16 µs | 34.4 µs | **2.15** |
| Deserialize | 18.9 µs | 74.6 µs | **3.96** | 12.8 µs | 33.4 µs | **2.61** |

</details>

<details>
<summary>64-bit queries (19 benchmarks)</summary>

Sparse: 1M values over 2^48 (one value per container); Clustered: 1M values in 64 clusters over 2^40; Dense: 1M values over 2^26. 64Contains uses 300k values over 2^40.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| Contains / Sparse | 105 ns | 166 ns | **1.58** | 88.3 ns | 40.2 ns | 0.46 |
| Rank / Sparse | 44.9 ms | 2.96 ms | 0.07 | 30.6 ms | 3.19 ms | 0.10 |
| Select / Sparse | 38.8 ms | 10.8 ms | 0.28 | 27.6 ms | 11.8 ms | 0.43 |
| MinMax / Sparse | 115 ns | 7.23 ns | 0.06 | 80.8 ns | 5.6 ns | 0.07 |
| RemoveAdd / Sparse | 318 ns | 344 ns | **1.08** | 223 ns | 153 ns | 0.69 |
| Cardinality / Sparse | 3.56 ms | 6.62 ms | **1.86** | 595 µs | 8.36 ms | **14** |
| Contains / Clustered | 67.9 ns | 70.8 ns | 1.04 | 61 ns | 16.1 ns | 0.26 |
| Rank / Clustered | 9.62 µs | 868 ns | 0.09 | 3.54 µs | 638 ns | 0.18 |
| Select / Clustered | 5.63 µs | 2.69 µs | 0.48 | 3.21 µs | 1.91 µs | 0.59 |
| MinMax / Clustered | 114 ns | 7.09 ns | 0.06 | 74.6 ns | 5.6 ns | 0.08 |
| RemoveAdd / Clustered | 234 ns | 238 ns | 1.02 | 193 ns | 97.6 ns | 0.51 |
| Cardinality / Clustered | 1.26 µs | 1.71 µs | **1.36** | 460 ns | 1.24 µs | **2.70** |
| Contains / Dense | 95 ns | 126 ns | **1.33** | 88.7 ns | 30.8 ns | 0.35 |
| Rank / Dense | 9.2 µs | 960 ns | 0.10 | 2.93 µs | 648 ns | 0.22 |
| Select / Dense | 4.27 µs | 4.4 µs | 1.03 | 2.88 µs | 3.98 µs | **1.38** |
| MinMax / Dense | 112 ns | 7.17 ns | 0.06 | 75.6 ns | 5.63 ns | 0.07 |
| RemoveAdd / Dense | 217 ns | 246 ns | **1.13** | 189 ns | 99.2 ns | 0.53 |
| Cardinality / Dense | 1.28 µs | 1.78 µs | **1.39** | 459 ns | 1.8 µs | **3.91** |
| 64Contains | 90 ns | 110 ns | **1.23** | 71.2 ns | 80.5 ns | **1.13** |

</details>

<details>
<summary>64-bit iteration, construction and serialization (28 benchmarks)</summary>

Iterate is buffered in both libraries; IterateStepwise uses the gocroaring Iterator64, one call per value. AddOneByOne adds 100k values; AddMany builds from all 1M. The 64-bit serialized bytes are identical in both libraries.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| Iterate / Sparse | 130 ms | 32.3 ms | 0.25 | 74.7 ms | 34.6 ms | 0.46 |
| IterateStepwise / Sparse | 346 ms | 32.2 ms | 0.09 | 285 ms | 33.6 ms | 0.12 |
| IterateCallback / Sparse | 128 ms | 31.4 ms | 0.24 | 79.9 ms | 34.5 ms | 0.43 |
| ToArray / Sparse | 118 ms | 59.8 ms | 0.51 | 64.2 ms | 51.6 ms | 0.80 |
| Clone / Sparse | 432 ms | 135 ms | 0.31 | 245 ms | 69.5 ms | 0.28 |
| AddOneByOne / Sparse | 26.2 ms | 177 ms | **6.73** | 13.6 ms | 144 ms | **11** |
| AddMany / Sparse | 466 ms | 547 ms | **1.17** | 264 ms | 435 ms | **1.65** |
| Serialize / Sparse | 128 ms | 58.8 ms | 0.46 | 74.8 ms | 32.9 ms | 0.44 |
| Deserialize / Sparse | 160 ms | 81.7 ms | 0.51 | 74.1 ms | 37.6 ms | 0.51 |
| Iterate / Clustered | 2.07 ms | 6.95 ms | **3.35** | 770 µs | 4.09 ms | **5.30** |
| IterateStepwise / Clustered | 101 ms | 6.92 ms | 0.07 | 89 ms | 4.07 ms | 0.05 |
| IterateCallback / Clustered | 2.18 ms | 6.92 ms | **3.18** | 1.45 ms | 4.08 ms | **2.81** |
| ToArray / Clustered | 1.33 ms | 1.37 ms | 1.03 | 187 µs | 439 µs | **2.35** |
| Clone / Clustered | 346 µs | 428 µs | **1.24** | 137 µs | 155 µs | **1.13** |
| AddOneByOne / Clustered | 10.6 ms | 9.91 ms | 0.94 | 8.01 ms | 5.56 ms | 0.69 |
| AddMany / Clustered | 102 ms | 133 ms | **1.31** | 78.6 ms | 94.9 ms | **1.21** |
| Serialize / Clustered | 372 µs | 712 µs | **1.91** | 74.5 µs | 136 µs | **1.83** |
| Deserialize / Clustered | 325 µs | 447 µs | **1.38** | 110 µs | 157 µs | **1.43** |
| Iterate / Dense | 2.1 ms | 6.92 ms | **3.30** | 770 µs | 4.1 ms | **5.32** |
| IterateStepwise / Dense | 101 ms | 6.92 ms | 0.07 | 89.1 ms | 4.09 ms | 0.05 |
| IterateCallback / Dense | 2.16 ms | 6.92 ms | **3.20** | 1.45 ms | 4.1 ms | **2.82** |
| ToArray / Dense | 1.32 ms | 1.38 ms | 1.04 | 186 µs | 450 µs | **2.42** |
| Clone / Dense | 327 µs | 423 µs | **1.29** | 118 µs | 146 µs | **1.23** |
| AddOneByOne / Dense | 10.1 ms | 10.6 ms | 1.05 | 7.81 ms | 6.46 ms | 0.83 |
| AddMany / Dense | 96.5 ms | 132 ms | **1.37** | 76.9 ms | 90.9 ms | **1.18** |
| Serialize / Dense | 367 µs | 701 µs | **1.91** | 75.3 µs | 135 µs | **1.79** |
| Deserialize / Dense | 309 µs | 428 µs | **1.38** | 99.5 µs | 146 µs | **1.47** |
| 64AddMany | 86 ms | 99.2 ms | **1.15** | 50.8 ms | 66.4 ms | **1.31** |

</details>

<details>
<summary>64-bit set operations (23 benchmarks)</summary>

The second bitmap shares about half of the values of the first. 64Or uses two sets of 300k values over 2^40, 64And over 2^26.

| benchmark | x86-64 gocroaring | x86-64 roaring | ratio | arm64 gocroaring | arm64 roaring | ratio |
|---|--:|--:|--:|--:|--:|--:|
| Or / Sparse | 555 ms | 234 ms | 0.42 | 358 ms | 132 ms | 0.37 |
| And / Sparse | 476 ms | 205 ms | 0.43 | 305 ms | 121 ms | 0.40 |
| Xor / Sparse | 460 ms | 199 ms | 0.43 | 308 ms | 104 ms | 0.34 |
| AndNot / Sparse | 437 ms | 238 ms | 0.54 | 269 ms | 125 ms | 0.46 |
| AndCardinality / Sparse | 254 ms | 65.4 ms | 0.26 | 135 ms | 43.7 ms | 0.32 |
| OrCardinality / Sparse | 263 ms | 224 ms | 0.85 | 132 ms | 136 ms | 1.03 |
| Intersects / Sparse | 75.8 ns | 8.85 ns | 0.12 | 52.9 ns | 6.06 ns | 0.11 |
| Or / Clustered | 1.23 ms | 6.56 ms | **5.35** | 3.39 ms | 3.89 ms | **1.15** |
| And / Clustered | 825 µs | 5.3 ms | **6.42** | 3.21 ms | 3.16 ms | 0.98 |
| Xor / Clustered | 1.61 ms | 6.62 ms | **4.11** | 3.2 ms | 3.99 ms | **1.25** |
| AndNot / Clustered | 809 µs | 5.82 ms | **7.19** | 3.23 ms | 3.52 ms | **1.09** |
| AndCardinality / Clustered | 568 µs | 4.8 ms | **8.45** | 3.09 ms | 3.09 ms | 1.00 |
| OrCardinality / Clustered | 564 µs | 6.59 ms | **12** | 3.23 ms | 4.1 ms | **1.27** |
| Intersects / Clustered | 72.7 ns | 10.2 ns | 0.14 | 47.1 ns | 7.11 ns | 0.15 |
| Or / Dense | 1.2 ms | 6.39 ms | **5.32** | 3.23 ms | 3.86 ms | **1.20** |
| And / Dense | 800 µs | 5.2 ms | **6.50** | 3.17 ms | 3.12 ms | 0.98 |
| Xor / Dense | 1.56 ms | 6.46 ms | **4.13** | 3.2 ms | 4 ms | **1.25** |
| AndNot / Dense | 792 µs | 5.68 ms | **7.18** | 3.29 ms | 3.5 ms | **1.06** |
| AndCardinality / Dense | 574 µs | 4.79 ms | **8.34** | 3.17 ms | 3.08 ms | 0.97 |
| OrCardinality / Dense | 570 µs | 6.46 ms | **11** | 3.19 ms | 3.89 ms | **1.22** |
| Intersects / Dense | 73.3 ns | 8.79 ns | 0.12 | 47.1 ns | 6.03 ns | 0.13 |
| 64Or | 214 ms | 67.5 ms | 0.32 | 103 ms | 30.5 ms | 0.30 |
| 64And | 399 µs | 2.61 ms | **6.54** | 1.24 ms | 1.51 ms | **1.22** |

</details>

Your results depend on your data and hardware, so measure with your own
workload. To run the comparison:

```
cd benchmarks/compare
go test -run '^$' -bench . -benchmem -count 10 > results.txt
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
