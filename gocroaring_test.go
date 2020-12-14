package gocroaring

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
)

func TestDisplayVersion(t *testing.T) {
	fmt.Printf("CRoaring %v.%v.%v\n", CRoaringMajor, CRoaringMinor, CRoaringRevision)
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

// go test -run StressMemory
func TestStressMemory(t *testing.T) {
	for i := 0; i < 10; i++ {
		r0 := New()
		var j uint32
		for k := 0; k < 10000000; k++ {
			j = uint32(rand.Intn(10000000))
			r0.Add(j)
		}
		r0.RunOptimize() // improves compression
		buf0 := make([]byte, r0.SerializedSizeInBytes())
		r0.Write(buf0) // we omit error handling
		PrintMemUsage()
	}
	fmt.Println("ok")
}

// go test -run MemoryUsage
func TestMemoryUsage(t *testing.T) {
	bitmap := New()
	for i := 0; i < 1000000; i++ {
		bitmap.Add(uint32(i) * 10)
	}
	sb := bitmap.SerializedSizeInBytes()
	memoryAlloc := 8 * 1024 * 1024
	howmany := (memoryAlloc + sb - 1) / sb
	fmt.Println("size in kB of one bitmap ", sb/(1024), "; number of copies = ", howmany, "; total alloc: ", howmany*sb/(1024), "kB")
	for i := 0; i < howmany; i++ {
		y := bitmap.Clone()
		_ = y
	}
}

func TestSimpleCard(t *testing.T) {
	bitmap := New()
	for i := 100; i < 1000; i++ {
		bitmap.Add(uint32(i))
	}
	c := bitmap.Cardinality()
	bitmap.Printf()
	fmt.Println("cardinality: ", c)
	if c != 900 {
		t.Error("Expected ", 900, ", got ", c)
	}
	bitmap.RunOptimize()
	if c != 900 {
		t.Error("Expected ", 900, ", got ", c)
	}
}

func TestNewWithVals(t *testing.T) {
	vals := []uint32{1, 2, 3, 6, 7, 8, 20, 44444}
	rb := New(vals...)
	for _, v := range vals {
		if !rb.Contains(v) {
			t.Errorf("expected %d from initialized values\n", v)
		}
	}
}

func TestAddMany(t *testing.T) {
	rb1 := New()
	sl := []uint32{1, 2, 3, 6, 7, 8, 20, 44444}
	rb1.Add(sl...)

	if int(rb1.Cardinality()) != len(sl) {
		t.Errorf("cardinality: expected %d, got %d", len(sl), rb1.Cardinality())
	}
	if rb1.Contains(5) {
		t.Error("didn't expect to contain 5")
	}
	for _, v := range sl {
		if !rb1.Contains(v) {
			t.Errorf("expected to contain %d", v)
		}
	}
}

func TestAddRange(t *testing.T) {
	rb1 := New()
	rb1.AddRange(1, 5)

	if int(rb1.Cardinality()) != 4 {
		t.Errorf("cardinality: expected %d, got %d", 4, rb1.Cardinality())
	}
	if rb1.Contains(5) {
		t.Error("didn't expect to contain 5")
	}
	for _, v := range []uint32{1, 2, 3, 4} {
		if !rb1.Contains(v) {
			t.Errorf("expected to contain %d", v)
		}
	}
}

func TestRemoveRange(t *testing.T) {
	rb1 := New()
	rb1.AddRange(1, 5)
	rb1.RemoveRange(2, 4)

	if int(rb1.Cardinality()) != 2 {
		t.Errorf("cardinality: expected %d, got %d", 2, rb1.Cardinality())
	}
	if rb1.Contains(5) {
		t.Error("didn't expect to contain 5")
	}
	if rb1.Contains(3) {
		t.Error("didn't expect to contain 3")
	}
	for _, v := range []uint32{1, 4} {
		if !rb1.Contains(v) {
			t.Errorf("expected to contain %d", v)
		}
	}
}

func TestFancier(t *testing.T) {
	rb1 := New()
	rb1.Add(1)
	rb1.Add(2)
	rb1.Add(3)
	rb1.Add(4)
	rb1.Add(5)
	rb1.Add(100)
	rb1.Add(1000)
	rb1.RunOptimize()
	rb2 := New()
	rb2.Add(3)
	rb2.Add(4)
	rb2.Add(1000)
	rb2.RunOptimize()
	rb3 := New()
	fmt.Println("Cardinality: ", rb1.Cardinality())
	if rb1.Cardinality() != 7 {
		t.Error("Bad card")
	}
	if !rb1.Contains(3) {
		t.Error("should contain it")
	}
	rb1.And(rb2)
	fmt.Println(rb1)
	rb3.Add(5)
	rb3.Or(rb1)
	// prints 3, 4, 5, 1000
	i := rb3.Iterator()
	for i.HasNext() {
		fmt.Println(i.Next())
	}
	fmt.Println()
	fmt.Println(rb3.ToArray())
	fmt.Println(rb3)
	rb4 := FastOr(rb1, rb2, rb3)
	fmt.Println(rb4)
	// next we include an example of serialization
	buf := make([]byte, rb1.SerializedSizeInBytes())
	err := rb1.Write(buf)
	if err != nil {
		t.Error("Write failed", err)
	}
	newrb, err := Read(buf)
	if err != nil {
		t.Error("Read failed", err)
	}
	if rb1.Equals(newrb) {
		fmt.Println("I wrote the content to a byte stream and read it back.")
	} else {
		t.Error("Bad read")
	}
}

func TestString(t *testing.T) {
	ans := New(1, 2, 3).String()
	fmt.Println(ans)
	if ans != "{1,2,3}" {
		t.Errorf("bad string ")
	}
	ans = New().String()
	fmt.Println(ans)
	if ans != "{}" {
		t.Errorf("bad string ")
	}
}

func TestStats(t *testing.T) {

	rb := New()
	rb.Add(1, 2, 3, 4, 6, 7)
	rb.Add(999991, 999992, 999993, 999994, 999996, 999997)

	stats := rb.Stats()
	if stats["cardinality"] != rb.Cardinality() {
		t.Errorf("cardinality: expected %d got %d\n", rb.Cardinality(), stats["cardinality"])
	}

	if stats["n_containers"] != 2 {
		t.Errorf("n_containers: expected %d got %d\n", 2, stats["n_containers"])
	}
	if stats["n_array_containers"] != 2 {
		t.Errorf("n_array_containers: expected %d got %d\n", 2, stats["n_array_containers"])
	}
	for _, c := range []string{"n_run_containers", "n_bitmap_containers"} {
		if stats[c] != 0 {
			t.Errorf("%s: expected 0 got %d\n", c, stats[c])
		}
	}
}

func TestWriteFrozen(t *testing.T) {
	for i := 0; i < 10; i++ {
		rb := New()
		var j uint32
		for k := 0; k < 10000000; k++ {
			j = uint32(rand.Intn(10000000))
			rb.Add(j)
		}

		// frozen serialization
		buf := make([]byte, rb.FrozenSizeInBytes())
		rb.WriteFrozen(buf) // we omit error handling

		newrb, err := ReadFrozenView(buf)
		if err != nil {
			t.Error("ReadFrozenView failed", err)
		}
		if rb.Equals(newrb) {
			PrintMemUsage()
		} else {
			t.Error("Bad read")
		}
		runtime.KeepAlive(buf)
	}
	fmt.Println("ok")
}

func TestStatsStruct(t *testing.T) {
	t.Run("Test Stats with empty bitmap", func(t *testing.T) {
		expectedStats := Statistics{}
		rr := New()
		if !reflect.DeepEqual(expectedStats, rr.StatsStruct()) {
			t.Errorf("expected %#v, got %#v", expectedStats, rr.StatsStruct())
		}
	})

	t.Run("Test Stats with Bitmap Container", func(t *testing.T) {
		// Given a bitmap that should have a single bitmap container
		expectedStats := Statistics{
			Cardinality: 60000,
			Containers:  1,

			BitmapContainers:      1,
			BitmapContainerValues: 60000,
			BitmapContainerBytes:  8192,
		}
		rr := New()
		for i := uint32(0); i < 60000; i++ {
			rr.Add(i)
		}
		if !reflect.DeepEqual(expectedStats, rr.StatsStruct()) {
			t.Errorf("expected %#v, got %#v", expectedStats, rr.StatsStruct())
		}
	})

	t.Run("Test Stats with Array Container", func(t *testing.T) {
		// Given a bitmap that should have a single array container
		expectedStats := Statistics{
			Cardinality: 2,
			Containers:  1,

			ArrayContainers:      1,
			ArrayContainerValues: 2,
			ArrayContainerBytes:  4,
		}
		rr := New()
		rr.Add(2)
		rr.Add(4)
		if !reflect.DeepEqual(expectedStats, rr.StatsStruct()) {
			t.Errorf("expected %#v, got %#v", expectedStats, rr.StatsStruct())
		}
	})

	t.Run("no run containers", func(t *testing.T) {
		rb := New()
		rb.Add(1, 2, 3, 4, 6, 7)
		rb.Add(999991, 999992, 999993, 999994, 999996, 999997)

		stats := rb.StatsStruct()
		if stats.Cardinality != rb.Cardinality() {
			t.Errorf("cardinality: expected %d got %d\n", rb.Cardinality(), stats.Cardinality)
		}

		if stats.Containers != 2 {
			t.Errorf("n_containers: expected %d got %d\n", 2, stats.Containers)
		}
		if stats.ArrayContainers != 2 {
			t.Errorf("n_array_containers: expected %d got %d\n", 2, stats.ArrayContainers)
		}
		if stats.RunContainers != 0 {
			t.Errorf("expected 0 got %d\n", stats.RunContainers)
		}
		if stats.BitmapContainers != 0 {
			t.Errorf("expected 0 got %d\n", stats.BitmapContainers)
		}
	})
}

func TestAssign(t *testing.T) {
	rb1 := New()
	for i := 0; i < 1000000; i++ {
		rb1.Add(uint32(i) * 10)
	}
	rb2 := New()
	rb2.Assign(rb1)
	if !rb1.Equals(rb2) {
		t.Error("should equal")
	}
}
