module github.com/RoaringBitmap/gocroaring/benchmarks/compare

go 1.24.0

require (
	github.com/RoaringBitmap/gocroaring v1.1.0
	github.com/RoaringBitmap/roaring/v2 v2.30.0
)

require (
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/RoaringBitmap/gocroaring => ../..
