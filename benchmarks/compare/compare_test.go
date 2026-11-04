package compare

// Side-by-side benchmarks of gocroaring (CRoaring through cgo) and roaring
// (pure Go), on identical data. Each benchmark has a /CRoaring and a /Go
// sub-benchmark. Results of gocroaring operations are freed explicitly; the
// pure Go library relies on the garbage collector.

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/gocroaring"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func randValues(seed int64, n int, universe int32) []uint32 {
	r := rand.New(rand.NewSource(seed))
	vals := make([]uint32, n)
	for i := range vals {
		vals[i] = uint32(r.Int31n(universe))
	}
	return vals
}

func runValues(seed int64, nruns int, universe int32) []uint32 {
	r := rand.New(rand.NewSource(seed))
	var vals []uint32
	for i := 0; i < nruns; i++ {
		s := uint32(r.Int31n(universe))
		l := uint32(r.Int31n(500) + 1)
		for v := s; v < s+l; v++ {
			vals = append(vals, v)
		}
	}
	return vals
}

type dataset struct {
	name string
	a, b []uint32
	runs bool
}

var datasets = []dataset{
	{"ArrayArray", randValues(1, 200000, 1<<28), randValues(2, 200000, 1<<28), false},
	{"SmallArrayBigArray", randValues(3, 2000, 1<<24), randValues(4, 900000, 1<<24), false},
	{"BitsetBitset", randValues(5, 2000000, 1<<22), randValues(6, 2000000, 1<<22), false},
	{"ArrayBitset", randValues(7, 30000, 1<<22), randValues(8, 2000000, 1<<22), false},
	{"RunRun", runValues(9, 20000, 1<<26), runValues(10, 20000, 1<<26), true},
	{"RunArray", runValues(11, 20000, 1<<26), randValues(12, 200000, 1<<26), false},
}

func cBitmap(vals []uint32, runs bool) *gocroaring.Bitmap {
	bm := gocroaring.New(vals...)
	if runs {
		bm.RunOptimize()
	}
	return bm
}

func goBitmap(vals []uint32, runs bool) *roaring.Bitmap {
	bm := roaring.BitmapOf(vals...)
	if runs {
		bm.RunOptimize()
	}
	return bm
}

// The run dataset "b" of RunArray is an array: only "a" gets RunOptimize.
func pairs(d dataset) (*gocroaring.Bitmap, *gocroaring.Bitmap, *roaring.Bitmap, *roaring.Bitmap) {
	aRuns := d.runs || d.name == "RunArray"
	return cBitmap(d.a, aRuns), cBitmap(d.b, d.runs), goBitmap(d.a, aRuns), goBitmap(d.b, d.runs)
}

func benchBinary(b *testing.B,
	cop func(x, y *gocroaring.Bitmap),
	gop func(x, y *roaring.Bitmap)) {
	for _, d := range datasets {
		ca, cb, ga, gb := pairs(d)
		b.Run(d.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				cop(ca, cb)
			}
		})
		b.Run(d.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				gop(ga, gb)
			}
		})
	}
}

func BenchmarkAnd(b *testing.B) {
	benchBinary(b, func(x, y *gocroaring.Bitmap) { gocroaring.And(x, y).Free() },
		func(x, y *roaring.Bitmap) { _ = roaring.And(x, y) })
}

func BenchmarkOr(b *testing.B) {
	benchBinary(b, func(x, y *gocroaring.Bitmap) { gocroaring.Or(x, y).Free() },
		func(x, y *roaring.Bitmap) { _ = roaring.Or(x, y) })
}

func BenchmarkXor(b *testing.B) {
	benchBinary(b, func(x, y *gocroaring.Bitmap) { gocroaring.Xor(x, y).Free() },
		func(x, y *roaring.Bitmap) { _ = roaring.Xor(x, y) })
}

func BenchmarkAndNot(b *testing.B) {
	benchBinary(b, func(x, y *gocroaring.Bitmap) { gocroaring.AndNot(x, y).Free() },
		func(x, y *roaring.Bitmap) { _ = roaring.AndNot(x, y) })
}

func BenchmarkAndCardinality(b *testing.B) {
	benchBinary(b, func(x, y *gocroaring.Bitmap) { _ = x.AndCardinality(y) },
		func(x, y *roaring.Bitmap) { _ = x.AndCardinality(y) })
}

func BenchmarkIntersects(b *testing.B) {
	benchBinary(b, func(x, y *gocroaring.Bitmap) { _ = x.Intersect(y) },
		func(x, y *roaring.Bitmap) { _ = x.Intersects(y) })
}

// In-place union of a small bitmap into a copy of a big one.
func BenchmarkOrInPlace(b *testing.B) {
	big, small := randValues(20, 2000000, 1<<28), randValues(21, 500, 1<<28)
	cbig, csmall := gocroaring.New(big...), gocroaring.New(small...)
	gbig, gsmall := roaring.BitmapOf(big...), roaring.BitmapOf(small...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			c := cbig.Clone()
			c.Or(csmall)
			c.Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			c := gbig.Clone()
			c.Or(gsmall)
		}
	})
}

func manyValues() [][]uint32 {
	var out [][]uint32
	for i := 0; i < 200; i++ {
		out = append(out, randValues(int64(100+i), 5000, 1<<26))
	}
	return out
}

func BenchmarkFastOr(b *testing.B) {
	vals := manyValues()
	var cs []*gocroaring.Bitmap
	var gs []*roaring.Bitmap
	for _, v := range vals {
		cs = append(cs, gocroaring.New(v...))
		gs = append(gs, roaring.BitmapOf(v...))
	}
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			gocroaring.FastOr(cs...).Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = roaring.FastOr(gs...)
		}
	})
}

func BenchmarkFastXor(b *testing.B) {
	vals := manyValues()
	var cs []*gocroaring.Bitmap
	var gs []*roaring.Bitmap
	for _, v := range vals {
		cs = append(cs, gocroaring.New(v...))
		gs = append(gs, roaring.BitmapOf(v...))
	}
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			gocroaring.FastXor(cs...).Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = roaring.HeapXor(gs...)
		}
	})
}

// Building a bitmap value by value, from random and from sorted input.
func BenchmarkAddOneByOne(b *testing.B) {
	for _, in := range []struct {
		name string
		vals []uint32
	}{{"Random", randValues(30, 50000, 5000)}, {"Sorted", seq(50000)}} {
		b.Run(in.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				bm := gocroaring.New()
				for _, v := range in.vals {
					bm.Add(v)
				}
				bm.Free()
			}
		})
		b.Run(in.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				bm := roaring.New()
				for _, v := range in.vals {
					bm.Add(v)
				}
			}
		})
	}
}

func seq(n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(i)
	}
	return out
}

// Building a bitmap from a slice in one call.
func BenchmarkAddMany(b *testing.B) {
	for _, in := range []struct {
		name string
		vals []uint32
	}{{"Random", randValues(31, 1000000, 1<<28)}, {"Sorted", seq(1000000)}} {
		b.Run(in.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				gocroaring.New(in.vals...).Free()
			}
		})
		b.Run(in.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = roaring.BitmapOf(in.vals...)
			}
		})
	}
}

func BenchmarkContains(b *testing.B) {
	vals := randValues(40, 1000000, 1<<26)
	probes := randValues(41, 4096, 1<<26)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = c.Contains(probes[n&4095])
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = g.Contains(probes[n&4095])
		}
	})
}

func BenchmarkRank(b *testing.B) {
	vals := randValues(42, 1000000, 1<<26)
	probes := randValues(43, 4096, 1<<26)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = c.Rank(probes[n&4095])
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = g.Rank(probes[n&4095])
		}
	})
}

func BenchmarkCardinality(b *testing.B) {
	vals := randValues(44, 1000000, 1<<26)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = c.Cardinality()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = g.GetCardinality()
		}
	})
}

func iterData() []uint32 { return randValues(50, 1000000, 1<<26) }

// Buffered iteration: values handed over in blocks.
func BenchmarkIterateBuffered(b *testing.B) {
	vals := iterData()
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			var sum uint64
			it := c.Iterator()
			for it.HasNext() {
				sum += uint64(it.Next())
			}
			_ = sum
		}
	})
	b.Run("Go", func(b *testing.B) {
		buf := make([]uint32, 256)
		for n := 0; n < b.N; n++ {
			var sum uint64
			it := g.ManyIterator()
			for k := it.NextMany(buf); k > 0; k = it.NextMany(buf) {
				for _, v := range buf[:k] {
					sum += uint64(v)
				}
			}
			_ = sum
		}
	})
}

// Callback iteration over every value.
func BenchmarkIterateCallback(b *testing.B) {
	vals := iterData()
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			var sum uint64
			c.Iterate(func(x uint32) bool { sum += uint64(x); return true })
			_ = sum
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			var sum uint64
			g.Iterate(func(x uint32) bool { sum += uint64(x); return true })
			_ = sum
		}
	})
}

func BenchmarkToArray(b *testing.B) {
	vals := iterData()
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = c.ToArray()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = g.ToArray()
		}
	})
}

func BenchmarkFlip(b *testing.B) {
	vals := randValues(30, 100000, 1<<26)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			gocroaring.Flip(c, 0, 1<<26).Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = roaring.Flip(g, 0, 1<<26)
		}
	})
}

func BenchmarkRunOptimize(b *testing.B) {
	vals := runValues(60, 20000, 1<<26)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			x := c.Clone()
			x.RunOptimize()
			x.Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			x := g.Clone()
			x.RunOptimize()
		}
	})
}

func BenchmarkClone(b *testing.B) {
	vals := randValues(61, 1000000, 1<<26)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			c.Clone().Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = g.Clone()
		}
	})
}

// Both libraries use the same portable serialization format.
func BenchmarkSerialize(b *testing.B) {
	vals := randValues(8, 2000000, 1<<22)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = c.ToBytes()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = g.ToBytes()
		}
	})
}

func BenchmarkDeserialize(b *testing.B) {
	vals := randValues(8, 2000000, 1<<22)
	buf, _ := roaring.BitmapOf(vals...).ToBytes()
	if !bytes.Equal(buf, gocroaring.New(vals...).ToBytes()) {
		b.Fatal("the two libraries serialize differently")
	}
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			x, _ := gocroaring.Read(buf)
			x.Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			x := roaring.New()
			_ = x.UnmarshalBinary(buf)
		}
	})
}

func pair64(seed int64, universe int64) ([]uint64, []uint64) {
	r := rand.New(rand.NewSource(seed))
	a, c := make([]uint64, 300000), make([]uint64, 300000)
	for i := range a {
		a[i] = uint64(r.Int63n(universe))
		c[i] = uint64(r.Int63n(universe))
	}
	return a, c
}

func Benchmark64Or(b *testing.B) {
	a, c := pair64(70, 1<<40)
	ca, cc := gocroaring.New64(a...), gocroaring.New64(c...)
	ga, gc := roaring64.BitmapOf(a...), roaring64.BitmapOf(c...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			gocroaring.Or64(ca, cc).Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = roaring64.Or(ga, gc)
		}
	})
}

func Benchmark64And(b *testing.B) {
	a, c := pair64(71, 1<<26)
	ca, cc := gocroaring.New64(a...), gocroaring.New64(c...)
	ga, gc := roaring64.BitmapOf(a...), roaring64.BitmapOf(c...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			gocroaring.And64(ca, cc).Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = roaring64.And(ga, gc)
		}
	})
}

func Benchmark64Contains(b *testing.B) {
	a, _ := pair64(72, 1<<40)
	probes, _ := pair64(73, 1<<40)
	ca, ga := gocroaring.New64(a...), roaring64.BitmapOf(a...)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = ca.Contains(probes[n%len(probes)])
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = ga.Contains(probes[n%len(probes)])
		}
	})
}

func Benchmark64AddMany(b *testing.B) {
	a, _ := pair64(74, 1<<40)
	b.Run("CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			gocroaring.New64(a...).Free()
		}
	})
	b.Run("Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_ = roaring64.BitmapOf(a...)
		}
	})
}
