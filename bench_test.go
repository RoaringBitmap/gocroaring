package gocroaring_test

import (
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/gocroaring"
)

var ordered []uint32
var random []uint32

func init() {
	var i uint32
	for i = 0; i < 50000; i++ {
		ordered = append(ordered, i)
		random = append(random, uint32(rand.Int31n(1e6)/200))
	}
}

func benchmarkAdd(b *testing.B, sl []uint32) {
	for n := 0; n < b.N; n++ {
		rb1 := gocroaring.New()
		for _, i := range sl {
			rb1.Add(i)
		}
	}
}

func benchmarkAddMany(b *testing.B, sl []uint32) {
	for n := 0; n < b.N; n++ {
		rb1 := gocroaring.New()
		rb1.Add(sl...)
	}
}

func benchmarkNewFromPtr(b *testing.B, sl []uint32) {
	for n := 0; n < b.N; n++ {
		rb := gocroaring.New(sl...)
		_ = rb
	}

}

func BenchmarkAddRandom(b *testing.B)  { benchmarkAdd(b, random) }
func BenchmarkAddOrdered(b *testing.B) { benchmarkAdd(b, ordered) }

func BenchmarkAddRandomArity(b *testing.B)  { benchmarkAddMany(b, random) }
func BenchmarkAddOrderedArity(b *testing.B) { benchmarkAddMany(b, ordered) }

func BenchmarkRandomNewFromPtr(b *testing.B)  { benchmarkNewFromPtr(b, random) }
func BenchmarkOrderedNewFromPtr(b *testing.B) { benchmarkNewFromPtr(b, ordered) }

func benchmarkBitmap() *gocroaring.Bitmap {
	rb := gocroaring.New()
	rb.AddRange(0, 1000000)
	rb.RunOptimize()
	return rb
}

// BenchmarkIterateBuffered walks the bitmap through the buffered iterator,
// which pulls values from C in blocks.
func BenchmarkIterateBuffered(b *testing.B) {
	rb := benchmarkBitmap()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var sum uint64
		it := rb.Iterator()
		for it.HasNext() {
			sum += uint64(it.Next())
		}
		_ = sum
	}
}

// BenchmarkIterateOneByOne walks the bitmap one value at a time, paying for a
// crossing of the Go/C boundary per value.
func BenchmarkIterateOneByOne(b *testing.B) {
	rb := benchmarkBitmap()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var sum uint64
		it := rb.NewIterator()
		for it.HasValue() {
			sum += uint64(it.Value())
			it.Next()
		}
		_ = sum
	}
}

func BenchmarkToArray(b *testing.B) {
	rb := benchmarkBitmap()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = rb.ToArray()
	}
}

// BenchmarkCreateAndDiscard measures the cost of handing bitmaps to the
// garbage collector, which is what the cleanup machinery pays for.
func BenchmarkCreateAndDiscard(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rb := gocroaring.New()
		rb.Add(1, 2, 3)
	}
}

func BenchmarkCreateAndFree(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rb := gocroaring.New()
		rb.Add(1, 2, 3)
		rb.Free()
	}
}

func BenchmarkAdd64(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rb := gocroaring.New64()
		for _, i := range ordered {
			rb.Add(uint64(i))
		}
	}
}

func BenchmarkIterateBuffered64(b *testing.B) {
	rb := gocroaring.New64()
	rb.AddRange(0, 1000000)
	rb.RunOptimize()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var sum uint64
		it := rb.Iterator()
		for it.HasNext() {
			sum += it.Next()
		}
		_ = sum
	}
}

func BenchmarkIterateOneByOne64(b *testing.B) {
	rb := gocroaring.New64()
	rb.AddRange(0, 1000000)
	rb.RunOptimize()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var sum uint64
		it := rb.NewIterator()
		for it.HasValue() {
			sum += it.Value()
			it.Next()
		}
		_ = sum
	}
}
