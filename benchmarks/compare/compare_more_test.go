package compare

// More side-by-side benchmarks: queries and updates over several bitmap
// shapes, in-place operations, zero-copy views, and a 64-bit suite. Queries
// cycle through 4096 probes so that the branch predictor cannot learn them.

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/gocroaring"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const nprobes = 4096

var sink uint64

type shape32 struct {
	name string
	vals []uint32
	runs bool
}

// Small: 1000 values in 4 containers; Sparse: array containers;
// Dense: bitset containers; Runs: run containers.
func shapes32() []shape32 {
	return []shape32{
		{"Small", randValues(80, 1000, 1<<18), false},
		{"Sparse", randValues(81, 1000000, 1<<26), false},
		{"Dense", randValues(82, 2000000, 1<<22), false},
		{"Runs", runValues(83, 20000, 1<<26), true},
	}
}

// Half of the probes are present values, half are random in the range.
func probes32(vals []uint32, seed int64) []uint32 {
	r := rand.New(rand.NewSource(seed))
	var max uint32
	for _, v := range vals {
		if v > max {
			max = v
		}
	}
	out := make([]uint32, nprobes)
	for i := range out {
		if i%2 == 0 {
			out[i] = vals[r.Intn(len(vals))]
		} else {
			out[i] = uint32(r.Int63n(int64(max) + 1))
		}
	}
	return out
}

func BenchmarkQueries32(b *testing.B) {
	for _, s := range shapes32() {
		c, g := cBitmap(s.vals, s.runs), goBitmap(s.vals, s.runs)
		probes := probes32(s.vals, 84)
		card := g.GetCardinality()
		r := rand.New(rand.NewSource(85))
		ranks := make([]uint32, nprobes)
		for i := range ranks {
			ranks[i] = uint32(r.Int63n(int64(card)))
		}
		const m = nprobes - 1
		b.Run("Contains/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if c.Contains(probes[n&m]) {
					sink++
				}
			}
		})
		b.Run("Contains/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if g.Contains(probes[n&m]) {
					sink++
				}
			}
		})
		b.Run("Rank/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += c.Rank(probes[n&m])
			}
		})
		b.Run("Rank/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += g.Rank(probes[n&m])
			}
		})
		b.Run("Select/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v, _ := c.Select(ranks[n&m])
				sink += uint64(v)
			}
		})
		b.Run("Select/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v, _ := g.Select(ranks[n&m])
				sink += uint64(v)
			}
		})
		b.Run("MinMax/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += uint64(c.Minimum()) + uint64(c.Maximum())
			}
		})
		b.Run("MinMax/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += uint64(g.Minimum()) + uint64(g.Maximum())
			}
		})
		// Remove a present value and add it back.
		b.Run("RemoveAdd/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v := probes[(n&m)&^1]
				c.Remove(v)
				c.Add(v)
			}
		})
		b.Run("RemoveAdd/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v := probes[(n&m)&^1]
				g.Remove(v)
				g.Add(v)
			}
		})
		b.Run("Equals/"+s.name+"/CRoaring", func(b *testing.B) {
			c2 := c.Clone()
			for n := 0; n < b.N; n++ {
				if c.Equals(c2) {
					sink++
				}
			}
			c2.Free()
		})
		b.Run("Equals/"+s.name+"/Go", func(b *testing.B) {
			g2 := g.Clone()
			for n := 0; n < b.N; n++ {
				if g.Equals(g2) {
					sink++
				}
			}
		})
		// Reverse iteration in blocks (ReadBackward), and value by value.
		b.Run("ReverseIterate/"+s.name+"/CRoaring", func(b *testing.B) {
			buf := make([]uint32, 256)
			for n := 0; n < b.N; n++ {
				it := c.ReverseIterator()
				for k := it.ReadBackward(buf); k > 0; k = it.ReadBackward(buf) {
					for _, v := range buf[:k] {
						sink += uint64(v)
					}
				}
				it.Free()
			}
		})
		b.Run("ReverseIterate/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := g.ReverseIterator()
				for it.HasNext() {
					sink += uint64(it.Next())
				}
			}
		})
		b.Run("ReverseIterateStepwise/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := c.ReverseIterator()
				for it.HasValue() {
					sink += uint64(it.Value())
					it.Previous()
				}
				it.Free()
			}
		})
		b.Run("ReverseIterateStepwise/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := g.ReverseIterator()
				for it.HasNext() {
					sink += uint64(it.Next())
				}
			}
		})
		c.Free()
	}
}

// Ranges: add and remove a range of 100000 values starting at a random offset.
func BenchmarkRanges32(b *testing.B) {
	vals := randValues(86, 1000000, 1<<26)
	c, g := gocroaring.New(vals...), roaring.BitmapOf(vals...)
	r := rand.New(rand.NewSource(87))
	starts := make([]uint64, nprobes)
	for i := range starts {
		starts[i] = uint64(r.Int63n(1<<26 - 100000))
	}
	b.Run("AddRemoveRange/CRoaring", func(b *testing.B) {
		x := c.Clone()
		for n := 0; n < b.N; n++ {
			s := starts[n&(nprobes-1)]
			x.AddRange(s, s+100000)
			x.RemoveRange(s, s+100000)
		}
		x.Free()
	})
	b.Run("AddRemoveRange/Go", func(b *testing.B) {
		x := g.Clone()
		for n := 0; n < b.N; n++ {
			s := starts[n&(nprobes-1)]
			x.AddRange(s, s+100000)
			x.RemoveRange(s, s+100000)
		}
	})
}

// In-place binary operations on a clone of the first bitmap; the clone is
// part of the measured work in both libraries.
func BenchmarkInPlace(b *testing.B) {
	ops := []struct {
		name string
		c    func(x, y *gocroaring.Bitmap)
		g    func(x, y *roaring.Bitmap)
	}{
		{"And", (*gocroaring.Bitmap).And, (*roaring.Bitmap).And},
		{"Or", (*gocroaring.Bitmap).Or, (*roaring.Bitmap).Or},
		{"Xor", (*gocroaring.Bitmap).Xor, (*roaring.Bitmap).Xor},
		{"AndNot", (*gocroaring.Bitmap).AndNot, (*roaring.Bitmap).AndNot},
	}
	for _, d := range datasets {
		ca, cb, ga, gb := pairs(d)
		for _, op := range ops {
			b.Run(op.name+"/"+d.name+"/CRoaring", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					x := ca.Clone()
					op.c(x, cb)
					x.Free()
				}
			})
			b.Run(op.name+"/"+d.name+"/Go", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					x := ga.Clone()
					op.g(x, gb)
				}
			})
		}
	}
}

func BenchmarkOrCardinality(b *testing.B) {
	benchBinary(b, func(x, y *gocroaring.Bitmap) { sink += x.OrCardinality(y) },
		func(x, y *roaring.Bitmap) { sink += x.OrCardinality(y) })
}

// Zero-copy views of a serialized bitmap: open the view, then query it.
func BenchmarkView(b *testing.B) {
	vals := randValues(88, 2000000, 1<<24)
	buf := gocroaring.New(vals...).ToBytes()
	gbuf, _ := roaring.BitmapOf(vals...).ToBytes()
	if !bytes.Equal(buf, gbuf) {
		b.Fatal("the two libraries serialize differently")
	}
	probes := probes32(vals, 89)
	b.Run("Open/CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			x, _ := gocroaring.ReadPortableFrozenView(buf)
			x.Free()
		}
	})
	b.Run("Open/Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			x := roaring.New()
			_, _ = x.FromBuffer(buf)
		}
	})
	cv, _ := gocroaring.ReadPortableFrozenView(buf)
	gv := roaring.New()
	_, _ = gv.FromBuffer(buf)
	b.Run("Contains/CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			if cv.Contains(probes[n&(nprobes-1)]) {
				sink++
			}
		}
	})
	b.Run("Contains/Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			if gv.Contains(probes[n&(nprobes-1)]) {
				sink++
			}
		}
	})
	b.Run("Cardinality/CRoaring", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sink += cv.Cardinality()
		}
	})
	b.Run("Cardinality/Go", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sink += gv.GetCardinality()
		}
	})
}

// 64-bit suite over three shapes of 1M values: "Sparse" over 2^48 (one value
// per 32-bit high part), "Clustered" in 64 clusters over 2^40, "Dense" over
// 2^26.
type shape64 struct {
	name string
	vals []uint64
}

func shapes64() []shape64 {
	r := rand.New(rand.NewSource(90))
	gen := func(f func() uint64) []uint64 {
		out := make([]uint64, 1000000)
		for i := range out {
			out[i] = f()
		}
		return out
	}
	return []shape64{
		{"Sparse", gen(func() uint64 { return uint64(r.Int63n(1 << 48)) })},
		{"Clustered", gen(func() uint64 { return uint64(r.Intn(64))<<34 | uint64(r.Intn(1<<20)) })},
		{"Dense", gen(func() uint64 { return uint64(r.Int63n(1 << 26)) })},
	}
}

// A second bitmap sharing about half of the values of the first.
func other64(vals []uint64, seed int64) []uint64 {
	r := rand.New(rand.NewSource(seed))
	out := make([]uint64, len(vals))
	for i, v := range vals {
		if r.Intn(2) == 0 {
			out[i] = v
		} else {
			out[i] = v ^ 1
		}
	}
	return out
}

func BenchmarkQueries64(b *testing.B) {
	for _, s := range shapes64() {
		c, g := gocroaring.New64(s.vals...), roaring64.BitmapOf(s.vals...)
		present := g.ToArray()
		r := rand.New(rand.NewSource(91))
		max := present[len(present)-1]
		probes := make([]uint64, nprobes)
		for i := range probes {
			if i%2 == 0 {
				probes[i] = present[r.Intn(len(present))]
			} else {
				probes[i] = uint64(r.Int63n(int64(max) + 1))
			}
		}
		ranks := make([]uint64, nprobes)
		for i := range ranks {
			ranks[i] = uint64(r.Intn(len(present)))
		}
		const m = nprobes - 1
		b.Run("Contains/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if c.Contains(probes[n&m]) {
					sink++
				}
			}
		})
		b.Run("Contains/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if g.Contains(probes[n&m]) {
					sink++
				}
			}
		})
		b.Run("Rank/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += c.Rank(probes[n&m])
			}
		})
		b.Run("Rank/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += g.Rank(probes[n&m])
			}
		})
		b.Run("Select/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v, _ := c.Select(ranks[n&m])
				sink += v
			}
		})
		b.Run("Select/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v, _ := g.Select(ranks[n&m])
				sink += v
			}
		})
		b.Run("MinMax/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += c.Minimum() + c.Maximum()
			}
		})
		b.Run("MinMax/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += g.Minimum() + g.Maximum()
			}
		})
		b.Run("RemoveAdd/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v := probes[(n&m)&^1]
				c.Remove(v)
				c.Add(v)
			}
		})
		b.Run("RemoveAdd/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v := probes[(n&m)&^1]
				g.Remove(v)
				g.Add(v)
			}
		})
		b.Run("Cardinality/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += c.Cardinality()
			}
		})
		b.Run("Cardinality/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += g.GetCardinality()
			}
		})
		b.Run("Iterate/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := c.Iterator()
				for it.HasNext() {
					sink += it.Next()
				}
			}
		})
		b.Run("Iterate/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := g.Iterator()
				for it.HasNext() {
					sink += it.Next()
				}
			}
		})
		b.Run("IterateStepwise/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := c.NewIterator()
				for it.HasValue() {
					sink += it.Value()
					it.Next()
				}
				it.Free()
			}
		})
		b.Run("IterateStepwise/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := g.Iterator()
				for it.HasNext() {
					sink += it.Next()
				}
			}
		})
		b.Run("IterateCallback/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				c.Iterate(func(x uint64) bool { sink += x; return true })
			}
		})
		b.Run("IterateCallback/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				for x := range roaring64.Values(g) {
					sink += x
				}
			}
		})
		b.Run("ToArray/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += uint64(len(c.ToArray()))
			}
		})
		b.Run("ToArray/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += uint64(len(g.ToArray()))
			}
		})
		b.Run("Clone/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				c.Clone().Free()
			}
		})
		b.Run("Clone/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = g.Clone()
			}
		})
		b.Run("AddOneByOne/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				x := gocroaring.New64()
				for _, v := range s.vals[:100000] {
					x.Add(v)
				}
				x.Free()
			}
		})
		b.Run("AddOneByOne/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				x := roaring64.New()
				for _, v := range s.vals[:100000] {
					x.Add(v)
				}
			}
		})
		b.Run("AddMany/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				gocroaring.New64(s.vals...).Free()
			}
		})
		b.Run("AddMany/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = roaring64.BitmapOf(s.vals...)
			}
		})
		cbuf := c.ToBytes()
		gbuf, _ := g.ToBytes()
		same := bytes.Equal(cbuf, gbuf)
		b.Run("Serialize/"+s.name+"/CRoaring", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += uint64(len(c.ToBytes()))
			}
		})
		b.Run("Serialize/"+s.name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				buf, _ := g.ToBytes()
				sink += uint64(len(buf))
			}
		})
		if same {
			b.Run("Deserialize/"+s.name+"/CRoaring", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					x, _ := gocroaring.Read64(cbuf)
					x.Free()
				}
			})
			b.Run("Deserialize/"+s.name+"/Go", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					x := roaring64.New()
					_ = x.UnmarshalBinary(gbuf)
				}
			})
		} else {
			b.Logf("%s: the two libraries serialize 64-bit bitmaps differently", s.name)
		}

		// Binary operations against a bitmap that shares half the values.
		o := other64(s.vals, 92)
		co, gob := gocroaring.New64(o...), roaring64.BitmapOf(o...)
		bin := []struct {
			name string
			c    func()
			g    func()
		}{
			{"Or", func() { gocroaring.Or64(c, co).Free() }, func() { _ = roaring64.Or(g, gob) }},
			{"And", func() { gocroaring.And64(c, co).Free() }, func() { _ = roaring64.And(g, gob) }},
			{"Xor", func() { gocroaring.Xor64(c, co).Free() }, func() { _ = roaring64.Xor(g, gob) }},
			{"AndNot", func() { gocroaring.AndNot64(c, co).Free() }, func() { _ = roaring64.AndNot(g, gob) }},
			{"AndCardinality", func() { sink += c.AndCardinality(co) }, func() { sink += g.AndCardinality(gob) }},
			{"OrCardinality", func() { sink += c.OrCardinality(co) }, func() { sink += g.OrCardinality(gob) }},
			{"Intersects", func() {
				if c.Intersect(co) {
					sink++
				}
			}, func() {
				if g.Intersects(gob) {
					sink++
				}
			}},
		}
		for _, op := range bin {
			b.Run(op.name+"/"+s.name+"/CRoaring", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					op.c()
				}
			})
			b.Run(op.name+"/"+s.name+"/Go", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					op.g()
				}
			})
		}
		co.Free()
		c.Free()
	}
}
