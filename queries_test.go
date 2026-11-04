package gocroaring

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// The read-only queries are implemented twice: in Go (queries_go.go) and as
// calls into CRoaring (the ...C methods). These tests check that both agree.

// containerBitmap returns a bitmap whose containers, at keys 0, 3 and 4, are
// of the requested kind.
func containerBitmap(r *rand.Rand, kind string) *Bitmap {
	bm := New()
	for _, key := range []uint64{0, 3, 4} {
		base := key << 16
		switch kind {
		case "array":
			for i := 0; i < 1+r.Intn(3000); i++ {
				bm.Add(uint32(base) + uint32(r.Intn(1<<16)))
			}
		case "tinyarray":
			for i := 0; i < 1+r.Intn(12); i++ {
				bm.Add(uint32(base) + uint32(r.Intn(1<<16)))
			}
		case "bitset":
			for i := 0; i < 5000+r.Intn(40000); i++ {
				bm.Add(uint32(base) + uint32(r.Intn(1<<16)))
			}
		case "run":
			for i := 0; i < 1+r.Intn(60); i++ {
				s := base + uint64(r.Intn(1<<16))
				e := s + 1 + uint64(r.Intn(700))
				if e > base+1<<16 {
					e = base + 1<<16
				}
				bm.AddRange(s, e)
			}
			bm.RunOptimize()
		}
	}
	return bm
}

var containerKinds = []string{"array", "tinyarray", "bitset", "run"}

func queryBitmaps() map[string]*Bitmap {
	r := rand.New(rand.NewSource(1))
	out := map[string]*Bitmap{}
	sparse := New()
	for i := 0; i < 20000; i++ {
		sparse.Add(uint32(r.Int31n(1 << 26)))
	}
	out["array"] = sparse
	dense := New()
	for i := 0; i < 300000; i++ {
		dense.Add(uint32(r.Int31n(1 << 20)))
	}
	out["bitset"] = dense
	runs := New()
	for i := 0; i < 2000; i++ {
		s := uint64(r.Int31n(1 << 24))
		runs.AddRange(s, s+uint64(r.Int31n(300)+1))
	}
	runs.RunOptimize()
	out["run"] = runs
	mixed := Or(Or(sparse, dense), runs)
	mixed.Add(math.MaxUint32)
	mixed.RunOptimize()
	out["mixed"] = mixed
	cow := mixed.Clone()
	cow.SetCopyOnWrite(true)
	out["shared"] = cow.Clone() // shares its containers with cow
	out["empty"] = New()
	out["full"] = FromRange(0, 1<<17, 1)
	out["top"] = FromRange(math.MaxUint32-70000, math.MaxUint32+1, 1)
	single := New()
	single.Add(1 << 20)
	out["single"] = single
	for _, kind := range containerKinds {
		out["kind-"+kind] = containerBitmap(r, kind)
	}
	buf := AlignedBuffer(mixed.FrozenSizeInBytes())
	if err := mixed.WriteFrozen(buf); err != nil {
		panic(err)
	}
	fv, err := ReadFrozenView(buf)
	if err != nil {
		panic(err)
	}
	out["frozen"] = fv
	// An odd offset makes the arrays of the portable frozen view unaligned.
	pb := mixed.ToBytes()
	odd := make([]byte, len(pb)+1)
	copy(odd[1:], pb)
	pfv, err := ReadPortableFrozenView(odd[1:])
	if err != nil {
		panic(err)
	}
	out["portablefrozen"] = pfv
	return out
}

func valueProbes(r *rand.Rand, bm *Bitmap) []uint32 {
	vals := bm.ToArray()
	if len(vals) > 5000 {
		r.Shuffle(len(vals), func(i, j int) { vals[i], vals[j] = vals[j], vals[i] })
		vals = vals[:5000]
	}
	probes := []uint32{0, 1, 65535, 65536, math.MaxUint32, math.MaxUint32 - 1}
	probes = append(probes, vals...)
	for _, v := range vals {
		probes = append(probes, v-1, v+1)
	}
	for i := 0; i < 5000; i++ {
		probes = append(probes, uint32(r.Int63n(1<<32)), uint32(r.Int31n(1<<26)))
	}
	return probes
}

// withBudgets runs f with the default work budget, and then with the largest
// budget (so that long queries stay in Go), when the Go queries are built.
func withBudgets(t *testing.T, f func(t *testing.T)) {
	t.Run("default", f)
	if setWorkBudget != nil {
		defer setWorkBudget(math.MaxInt)()
		t.Run("unlimited", f)
	}
}

func TestQueriesMatchC(t *testing.T) { withBudgets(t, testQueriesMatchC) }

func TestIntersectMatchesC(t *testing.T) { withBudgets(t, testIntersectMatchesC) }

func testQueriesMatchC(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for name, bm := range queryBitmaps() {
		if got, want := bm.Minimum(), bm.minimumC(); got != want {
			t.Errorf("%s: Minimum() = %d, want %d", name, got, want)
		}
		if got, want := bm.Maximum(), bm.maximumC(); got != want {
			t.Errorf("%s: Maximum() = %d, want %d", name, got, want)
		}
		for _, p := range valueProbes(r, bm) {
			if got, want := bm.Contains(p), bm.containsC(p); got != want {
				t.Fatalf("%s: Contains(%d) = %v, want %v", name, p, got, want)
			}
			if got, want := bm.Rank(p), bm.rankC(p); got != want {
				t.Fatalf("%s: Rank(%d) = %d, want %d", name, p, got, want)
			}
		}
		card := bm.Cardinality()
		ranks := []uint32{0, 1, uint32(card), uint32(card) + 1, math.MaxUint32}
		if card > 0 {
			ranks = append(ranks, uint32(card-1))
		}
		for i := 0; i < 5000 && card > 0; i++ {
			ranks = append(ranks, uint32(r.Int63n(int64(card))))
		}
		for _, k := range ranks {
			got, gotOK := bm.selectValue(k)
			want, wantOK := bm.selectC(k)
			if gotOK != wantOK || (gotOK && got != want) {
				t.Fatalf("%s: select(%d) = %d, %v, want %d, %v", name, k, got, gotOK, want, wantOK)
			}
		}
	}
}

func checkIntersect(t *testing.T, label string, a, b *Bitmap) {
	t.Helper()
	want := a.intersectC(b)
	if got := a.Intersect(b); got != want {
		t.Fatalf("%s: a.Intersect(b) = %v, want %v", label, got, want)
	}
	if got := b.Intersect(a); got != want {
		t.Fatalf("%s: b.Intersect(a) = %v, want %v", label, got, want)
	}
}

func testIntersectMatchesC(t *testing.T) {
	bms := queryBitmaps()
	for na, a := range bms {
		for nb, b := range bms {
			checkIntersect(t, na+"/"+nb, a, b)
		}
	}
	// For every pair of container kinds: disjoint containers, then the same
	// containers with a single value in common, placed at random.
	r := rand.New(rand.NewSource(3))
	for _, k1 := range containerKinds {
		for _, k2 := range containerKinds {
			for trial := 0; trial < 150; trial++ {
				label := fmt.Sprintf("%s/%s/%d", k1, k2, trial)
				a := containerBitmap(r, k1)
				b := AndNot(containerBitmap(r, k2), a)
				b.RunOptimize()
				checkIntersect(t, label+"/disjoint", a, b)
				if a.IsEmpty() {
					continue
				}
				sel, err := a.Select(uint32(r.Int63n(int64(a.Cardinality()))))
				if err != nil {
					t.Fatal(err)
				}
				b.Add(sel)
				checkIntersect(t, label+"/one", a, b)
				if !a.Intersect(b) {
					t.Fatalf("%s: shared value %d not found", label, sel)
				}
			}
		}
	}
}

// Query benchmarks cycle through many probes, so that the branch predictor
// cannot learn the answers.

func queryProbes(bm *Bitmap, n int) []uint32 {
	r := rand.New(rand.NewSource(3))
	vals := bm.ToArray()
	max := bm.Maximum()
	out := make([]uint32, n)
	for i := range out {
		if i%2 == 0 {
			out[i] = vals[r.Intn(len(vals))]
		} else {
			out[i] = uint32(r.Int63n(int64(max) + 1))
		}
	}
	return out
}

func benchBitmaps() (map[string]*Bitmap, []string) {
	bms := queryBitmaps()
	// "large": 1M random values over 2^26, as in benchmarks/compare.
	r := rand.New(rand.NewSource(40))
	large := New()
	for i := 0; i < 1000000; i++ {
		large.Add(uint32(r.Int31n(1 << 26)))
	}
	bms["large"] = large
	return bms, []string{"kind-array", "kind-run", "array", "bitset", "run", "mixed", "shared", "large"}
}

func BenchmarkQueries(b *testing.B) {
	bms, names := benchBitmaps()
	var sink uint64
	for _, name := range names {
		bm := bms[name]
		probes := queryProbes(bm, 4096)
		card := bm.Cardinality()
		ranks := make([]uint32, 4096)
		r := rand.New(rand.NewSource(5))
		for i := range ranks {
			ranks[i] = uint32(r.Int63n(int64(card)))
		}
		const mask = 4095
		b.Run("Contains/"+name+"/C", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if bm.containsC(probes[n&mask]) {
					sink++
				}
			}
		})
		b.Run("Contains/"+name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				if bm.Contains(probes[n&mask]) {
					sink++
				}
			}
		})
		b.Run("Rank/"+name+"/C", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += bm.rankC(probes[n&mask])
			}
		})
		b.Run("Rank/"+name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += bm.Rank(probes[n&mask])
			}
		})
		b.Run("Select/"+name+"/C", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v, _ := bm.selectC(ranks[n&mask])
				sink += uint64(v)
			}
		})
		b.Run("Select/"+name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				v, _ := bm.Select(ranks[n&mask])
				sink += uint64(v)
			}
		})
		b.Run("MinMax/"+name+"/C", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += uint64(bm.minimumC()) + uint64(bm.maximumC())
			}
		})
		b.Run("MinMax/"+name+"/Go", func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				sink += uint64(bm.Minimum()) + uint64(bm.Maximum())
			}
		})
	}
	_ = sink
}

func BenchmarkIntersect(b *testing.B) {
	r := rand.New(rand.NewSource(6))
	// Pairs of bitmaps, cycled through: "early" pairs share a value in their
	// first container; "disjoint" pairs share none, so every common container
	// is examined.
	type pair struct{ a, b *Bitmap }
	makePairs := func(disjoint bool, kinds [2]string) []pair {
		ps := make([]pair, 64)
		for i := range ps {
			a := containerBitmap(r, kinds[0])
			c := AndNot(containerBitmap(r, kinds[1]), a)
			if !disjoint {
				c.Add(a.Minimum())
			}
			c.RunOptimize()
			ps[i] = pair{a, c}
		}
		return ps
	}
	var sink int
	for _, disjoint := range []bool{false, true} {
		for _, kinds := range [][2]string{{"array", "array"}, {"tinyarray", "array"}, {"bitset", "bitset"}, {"array", "bitset"}, {"run", "run"}, {"array", "run"}, {"bitset", "run"}} {
			ps := makePairs(disjoint, kinds)
			label := "early"
			if disjoint {
				label = "disjoint"
			}
			label += "/" + kinds[0] + "-" + kinds[1]
			b.Run(label+"/C", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					p := ps[n&63]
					if p.a.intersectC(p.b) {
						sink++
					}
				}
			})
			b.Run(label+"/Go", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					p := ps[n&63]
					if p.a.Intersect(p.b) {
						sink++
					}
				}
			})
		}
	}
	_ = sink
}

// setWorkBudget sets the work budgets of the Go queries and returns a function
// that restores them; it is nil when the queries are built in C.
var setWorkBudget func(int) (restore func())
