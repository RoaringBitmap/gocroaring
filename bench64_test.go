package gocroaring_test

import (
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/gocroaring"
)

// Benchmarks of the 64-bit bitmaps. The queries cycle through many probes, so
// that the branch predictor cannot learn the answers.

const probes64 = 4096

type bench64Data struct {
	name string
	bm   *gocroaring.Bitmap64
	vals []uint64 // the values of bm
}

// bench64Bitmaps returns bitmaps of 1M values: "sparse" spreads them over
// 2^48 (one value per 32-bit high part, so the ART is deep and wide),
// "clustered" puts them in 64 dense clusters over 2^40, and "dense" over
// 2^26.
func bench64Bitmaps() []bench64Data {
	r := rand.New(rand.NewSource(64))
	make64 := func(name string, gen func() uint64) bench64Data {
		bm := gocroaring.New64()
		for i := 0; i < 1000000; i++ {
			bm.Add(gen())
		}
		return bench64Data{name, bm, bm.ToArray()}
	}
	return []bench64Data{
		make64("sparse", func() uint64 { return uint64(r.Int63n(1 << 48)) }),
		make64("clustered", func() uint64 {
			return uint64(r.Intn(64))<<34 | uint64(r.Intn(1<<20))
		}),
		make64("dense", func() uint64 { return uint64(r.Int63n(1 << 26)) }),
	}
}

// mixedProbes64 returns probes that are half present values and half random
// values in the range of the bitmap.
func mixedProbes64(d bench64Data) []uint64 {
	r := rand.New(rand.NewSource(65))
	max := d.vals[len(d.vals)-1]
	out := make([]uint64, probes64)
	for i := range out {
		if i%2 == 0 {
			out[i] = d.vals[r.Intn(len(d.vals))]
		} else {
			out[i] = uint64(r.Int63n(int64(max) + 1))
		}
	}
	return out
}

func Benchmark64Queries(b *testing.B) {
	var sink uint64
	for _, d := range bench64Bitmaps() {
		bm, probes := d.bm, mixedProbes64(d)
		r := rand.New(rand.NewSource(66))
		ranks := make([]uint64, probes64)
		for i := range ranks {
			ranks[i] = uint64(r.Intn(len(d.vals)))
		}
		const mask = probes64 - 1
		b.Run("Contains/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if bm.Contains(probes[n&mask]) {
					sink++
				}
			}
		})
		b.Run("Rank/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += bm.Rank(probes[n&mask])
			}
		})
		b.Run("Select/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v, _ := bm.Select(ranks[n&mask])
				sink += v
			}
		})
		b.Run("RemoveAdd/"+d.name, func(b *testing.B) {
			// Remove a present value and put it back: the bitmap does not
			// change, but both updates do their work.
			for n := 0; n < b.N; n++ {
				v := probes[(n&mask)&^1]
				bm.Remove(v)
				bm.Add(v)
			}
		})
		b.Run("AddNew/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if n%len(d.vals) == 0 {
					b.StopTimer()
					bm = gocroaring.New64()
					b.StartTimer()
				}
				bm.Add(d.vals[n%len(d.vals)] ^ 0x5555)
			}
			bm = d.bm
		})
		b.Run("Iterate/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				it := bm.NewIterator()
				for it.HasValue() {
					sink += it.Value()
					it.Next()
				}
			}
		})
		b.Run("Cardinality/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += bm.Cardinality()
			}
		})
	}
	_ = sink
}

func Benchmark64SetOps(b *testing.B) {
	bms := bench64Bitmaps()
	r := rand.New(rand.NewSource(67))
	var sink uint64
	for _, d := range bms {
		// A second bitmap that shares about half of the values of the first.
		other := gocroaring.New64()
		for _, v := range d.vals {
			if r.Intn(2) == 0 {
				other.Add(v)
			} else {
				other.Add(v ^ 1)
			}
		}
		a := d.bm
		b.Run("Or/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				x := gocroaring.Or64(a, other)
				sink += x.Cardinality()
				x.Free()
			}
		})
		b.Run("And/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				x := gocroaring.And64(a, other)
				sink += x.Cardinality()
				x.Free()
			}
		})
		b.Run("AndCardinality/"+d.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += a.AndCardinality(other)
			}
		})
	}
	_ = sink
}
