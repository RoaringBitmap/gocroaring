# gocroaring  [![Build Status](https://travis-ci.org/RoaringBitmap/gocroaring.png)](https://travis-ci.org/RoaringBitmap/gocroaring) [![GoDoc](https://godoc.org/github.com/RoaringBitmap/gocroaring?status.svg)](https://godoc.org/github.com/RoaringBitmap/gocroaring) ![Tests (Ubuntu, macOS)](https://github.com/RoaringBitmap/gocroaring/workflows/Tests%20(Ubuntu,%20macOS)/badge.svg)

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


### Benchmarking

See https://github.com/lemire/gobitmapbenchmark for a comparison between this wrapper and the Go native version.

### References

-  Daniel Lemire, Owen Kaser, Nathan Kurz, Luca Deri, Chris O'Hara, François Saint-Jacques, Gregory Ssi-Yan-Kai, Roaring Bitmaps: Implementation of an Optimized Software Library [arXiv:1709.07821](https://arxiv.org/abs/1709.07821)
-  Samy Chambi, Daniel Lemire, Owen Kaser, Robert Godin,
Better bitmap performance with Roaring bitmaps,
Software: Practice and Experience Volume 46, Issue 5, pages 709–719, May 2016
http://arxiv.org/abs/1402.6407 This paper used data from http://lemire.me/data/realroaring2014.html
- Daniel Lemire, Gregory Ssi-Yan-Kai, Owen Kaser, Consistently faster and smaller compressed bitmaps with Roaring, Software: Practice and Experience (accepted in 2016, to appear) http://arxiv.org/abs/1603.06549



### Dependencies

None in particular.

Naturally, you also need to grab the roaring code itself:
  - go get github.com/RoaringBitmap/gocroaring


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
