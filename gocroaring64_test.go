package gocroaring

import (
	"math"
	"reflect"
	"testing"
)

const big = uint64(1) << 40

func TestNew64(t *testing.T) {
	rb := New64()
	if !rb.IsEmpty() {
		t.Error("expected an empty bitmap")
	}
	rb = New64(1, 2, big, big+1)
	if rb.Cardinality() != 4 {
		t.Errorf("expected 4, got %d", rb.Cardinality())
	}
	for _, v := range []uint64{1, 2, big, big + 1} {
		if !rb.Contains(v) {
			t.Errorf("expected to contain %d", v)
		}
	}
	if rb.Contains(3) {
		t.Error("did not expect to contain 3")
	}
	if got, want := rb.Minimum(), uint64(1); got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
	if got, want := rb.Maximum(), big+1; got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
}

func TestNew64Empty(t *testing.T) {
	rb := New64()
	if rb.Minimum() != math.MaxUint64 {
		t.Errorf("expected MaxUint64, got %d", rb.Minimum())
	}
	if rb.Maximum() != 0 {
		t.Errorf("expected 0, got %d", rb.Maximum())
	}
}

func TestFromRange64(t *testing.T) {
	rb := FromRange64(big, big+10, 3)
	want := []uint64{big, big + 3, big + 6, big + 9}
	if got := rb.ToArray(); !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	defer func() {
		if recover() == nil {
			t.Error("expected a panic on a zero step")
		}
	}()
	FromRange64(0, 10, 0)
}

func TestMoveFrom32(t *testing.T) {
	src := New(1, 2, 3)
	rb := MoveFrom32(src)
	if got, want := rb.ToArray(), []uint64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	// The source is emptied but stays usable.
	if !src.IsEmpty() {
		t.Error("expected the source bitmap to be emptied")
	}
	src.Add(9)
	if !src.Contains(9) {
		t.Error("expected the source bitmap to remain usable")
	}
}

func TestAddRemoveChecked64(t *testing.T) {
	rb := New64()
	if !rb.AddChecked(big) {
		t.Error("expected the value to be new")
	}
	if rb.AddChecked(big) {
		t.Error("expected the value to already be present")
	}
	if !rb.RemoveChecked(big) {
		t.Error("expected the value to be removed")
	}
	if rb.RemoveChecked(big) {
		t.Error("expected the value to already be gone")
	}
}

func TestRanges64(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+5)
	if rb.Cardinality() != 5 {
		t.Errorf("expected 5, got %d", rb.Cardinality())
	}
	if !rb.ContainsRange(big, big+5) {
		t.Error("expected to contain the range")
	}
	if rb.ContainsRange(big, big+6) {
		t.Error("did not expect to contain the wider range")
	}
	rb.RemoveRange(big+1, big+3)
	if got, want := rb.ToArray(), []uint64{big, big + 3, big + 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}

	closed := New64()
	closed.AddRangeClosed(big, big+4)
	if !closed.ContainsRangeClosed(big, big+4) {
		t.Error("expected to contain the closed range")
	}
	if got := closed.RangeCardinalityClosed(big, big+2); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
	if got := closed.RangeCardinality(big, big+2); got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
	closed.RemoveRangeClosed(big+1, big+3)
	if got, want := closed.ToArray(), []uint64{big, big + 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if !closed.IntersectWithRange(big, big+1) {
		t.Error("expected an intersection")
	}
	if closed.IntersectWithRange(big+1, big+4) {
		t.Error("did not expect an intersection")
	}
}

func TestRemoveMany64(t *testing.T) {
	rb := New64(1, 2, 3, big, big+1)
	rb.RemoveMany(2, big)
	if got, want := rb.ToArray(), []uint64{1, 3, big + 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	rb.RemoveMany()
	if rb.Cardinality() != 3 {
		t.Errorf("expected 3, got %d", rb.Cardinality())
	}
}

func TestBulkContext64(t *testing.T) {
	rb := New64()
	ctx := NewBulkContext64()
	for i := uint64(0); i < 100000; i += 3 {
		rb.AddBulk(ctx, big+i)
	}
	if rb.Cardinality() != 33334 {
		t.Errorf("expected 33334, got %d", rb.Cardinality())
	}
	ctx = NewBulkContext64()
	for i := uint64(0); i < 100000; i += 3 {
		if !rb.ContainsBulk(ctx, big+i) {
			t.Fatalf("expected to contain %d", big+i)
		}
	}
	ctx = NewBulkContext64()
	for i := uint64(0); i < 100000; i += 3 {
		rb.RemoveBulk(ctx, big+i)
	}
	if !rb.IsEmpty() {
		t.Errorf("expected an empty bitmap, got %d values", rb.Cardinality())
	}
}

func TestRankSelectIndex64(t *testing.T) {
	rb := New64(2, 4, big, big+8)
	if got := rb.Rank(big); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
	if got := rb.GetIndex(big); got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
	if got := rb.GetIndex(big + 1); got != -1 {
		t.Errorf("expected -1, got %d", got)
	}
	if got, err := rb.Select(2); err != nil || got != big {
		t.Errorf("expected %d, got %d (%v)", big, got, err)
	}
	if _, err := rb.Select(4); err != ErrNoSuchElement {
		t.Errorf("expected ErrNoSuchElement, got %v", err)
	}
}

func TestSetOperations64(t *testing.T) {
	rb1 := New64(1, 2, big)
	rb2 := New64(2, big, big+1)

	if got, want := And64(rb1, rb2).ToArray(), []uint64{2, big}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := Or64(rb1, rb2).ToArray(), []uint64{1, 2, big, big + 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := Xor64(rb1, rb2).ToArray(), []uint64{1, big + 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := AndNot64(rb1, rb2).ToArray(), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}

	if got, want := rb1.AndCardinality(rb2), uint64(2); got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
	if got, want := rb1.OrCardinality(rb2), uint64(4); got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
	if got, want := rb1.XorCardinality(rb2), uint64(2); got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
	if got, want := rb1.AndNotCardinality(rb2), uint64(1); got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
	if got, want := rb1.JaccardIndex(rb2), 0.5; got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
	if !rb1.Intersect(rb2) {
		t.Error("expected an intersection")
	}

	inplace := rb1.Clone()
	inplace.Or(rb2)
	if got, want := inplace.ToArray(), []uint64{1, 2, big, big + 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	inplace.And(rb1)
	if !inplace.Equals(rb1) {
		t.Error("expected the intersection to be rb1")
	}
	inplace.Xor(rb1)
	if !inplace.IsEmpty() {
		t.Error("expected an empty bitmap")
	}
	inplace.Or(rb1)
	inplace.AndNot(rb1)
	if !inplace.IsEmpty() {
		t.Error("expected an empty bitmap")
	}
}

func TestSubsets64(t *testing.T) {
	rb := New64(1, 2, big)
	sub := New64(1, big)
	if !sub.IsSubset(rb) || !sub.IsStrictSubset(rb) {
		t.Error("expected sub to be a strict subset of rb")
	}
	if !rb.IsSubset(rb) || rb.IsStrictSubset(rb) {
		t.Error("a bitmap is a subset of itself, but not a strict one")
	}
	if rb.Equals("not a bitmap") {
		t.Error("expected a bitmap not to equal a string")
	}
}

func TestFlip64(t *testing.T) {
	rb := New64(big, big+1, big+2)
	if got, want := Flip64(rb, big+1, big+4).ToArray(), []uint64{big, big + 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := FlipClosed64(rb, big+1, big+3).ToArray(), []uint64{big, big + 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	inplace := rb.Clone()
	inplace.Flip(big+1, big+4)
	if got, want := inplace.ToArray(), []uint64{big, big + 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	inplace = rb.Clone()
	inplace.FlipClosed(big+1, big+3)
	if got, want := inplace.ToArray(), []uint64{big, big + 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestAddOffset64(t *testing.T) {
	rb := New64(big, big+1)
	if got, want := rb.AddOffset(10).ToArray(), []uint64{big + 10, big + 11}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := rb.AddOffset(-10).ToArray(), []uint64{big - 10, big - 9}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	// A negative offset larger than any value drops everything.
	if got := New64(1, 2).AddOffset(math.MinInt64); !got.IsEmpty() {
		t.Errorf("expected an empty bitmap, got %v", got.ToArray())
	}
}

func TestOptimize64(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+100000)
	if !rb.RunOptimize() {
		t.Error("expected run optimization to change the bitmap")
	}
	stats := rb.StatsStruct()
	if stats.RunContainers == 0 {
		t.Error("expected at least one run container")
	}
	if stats.Cardinality != rb.Cardinality() {
		t.Errorf("expected %d, got %d", rb.Cardinality(), stats.Cardinality)
	}
	if stats.MinValue != big || stats.MaxValue != big+99999 {
		t.Errorf("unexpected min/max: %d, %d", stats.MinValue, stats.MaxValue)
	}
	if m := rb.Stats(); m["cardinality"] != rb.Cardinality() {
		t.Errorf("expected %d, got %d", rb.Cardinality(), m["cardinality"])
	}
	if !rb.RemoveRunCompression() {
		t.Error("expected run compression to be removed")
	}
	rb.ShrinkToFit()
	if err := rb.InternalValidate(); err != nil {
		t.Errorf("expected a valid bitmap, got %v", err)
	}
}

func TestAssignClearFree64(t *testing.T) {
	rb := New64(1, 2, big)
	other := New64(9)
	other.Assign(rb)
	if !other.Equals(rb) {
		t.Error("expected the two bitmaps to be equal")
	}
	other.Clear()
	if !other.IsEmpty() {
		t.Error("expected an empty bitmap")
	}
	other.Free()
	other.Free()
}

func TestString64(t *testing.T) {
	if got, want := New64(1, 2, 3).String(), "{1,2,3}"; got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
	if got, want := New64().String(), "{}"; got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

func TestSerialization64(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+100000)
	rb.Add(1, 2, 3)
	rb.RunOptimize()

	buf := rb.ToBytes()
	if got := PortableDeserializeSize64(buf); got != len(buf) {
		t.Errorf("expected %d, got %d", len(buf), got)
	}
	back, err := Read64(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(back) {
		t.Error("round trip through the portable format failed")
	}

	validated, err := ReadValidated64(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(validated) {
		t.Error("round trip through ReadValidated64 failed")
	}
	if _, err := ReadValidated64([]byte{1, 2, 3, 4, 5, 6, 7, 8}); err == nil {
		t.Error("expected garbage to be rejected")
	}

	explicit := make([]byte, rb.SerializedSizeInBytes())
	if err := rb.Write(explicit); err != nil {
		t.Fatal(err)
	}
	if err := rb.Write(explicit[:1]); err != ErrNotEnoughSpace {
		t.Errorf("expected ErrNotEnoughSpace, got %v", err)
	}
	if _, err := Read64(nil); err != ErrEmptyBuffer {
		t.Errorf("expected ErrEmptyBuffer, got %v", err)
	}
	if PortableDeserializeSize64(nil) != 0 {
		t.Error("expected 0 for an empty buffer")
	}
}

func TestFrozenViews64(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+100000)
	rb.RunOptimize()

	// The 64-bit frozen format requires a shrunken bitmap.
	if err := rb.WriteFrozen(make([]byte, 1024)); err != ErrNotShrunken {
		t.Errorf("expected ErrNotShrunken, got %v", err)
	}
	rb.ShrinkToFit()

	frozen := AlignedBuffer64(rb.FrozenSizeInBytes())
	if err := rb.WriteFrozen(frozen); err != nil {
		t.Fatal(err)
	}
	view, err := ReadFrozenView64(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(view) {
		t.Error("the frozen view differs from the original")
	}
	if err := rb.WriteFrozen(frozen[:1]); err != ErrNotEnoughSpace {
		t.Errorf("expected ErrNotEnoughSpace, got %v", err)
	}

	misaligned := make([]byte, len(frozen)+1)
	copy(misaligned[1:], frozen)
	if _, err := ReadFrozenView64(misaligned[1:]); err == nil {
		t.Error("expected a misaligned buffer to be rejected")
	}
	if _, err := ReadFrozenView64(nil); err != ErrEmptyBuffer {
		t.Errorf("expected ErrEmptyBuffer, got %v", err)
	}

	pview, err := ReadPortableFrozenView64(rb.ToBytes())
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(pview) {
		t.Error("the portable frozen view differs from the original")
	}
	if _, err := ReadPortableFrozenView64(nil); err != ErrEmptyBuffer {
		t.Errorf("expected ErrEmptyBuffer, got %v", err)
	}
}

func TestAlignedBuffer64(t *testing.T) {
	if AlignedBuffer64(0) != nil {
		t.Error("expected nil for a zero-sized buffer")
	}
	for size := 1; size < 200; size++ {
		b := AlignedBuffer64(size)
		if len(b) != size {
			t.Fatalf("expected a buffer of size %d, got %d", size, len(b))
		}
		if !isAligned64(b) {
			t.Fatalf("buffer of size %d is not aligned", size)
		}
	}
}

func TestIterate64(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+5000)
	var collected []uint64
	rb.Iterate(func(x uint64) bool {
		collected = append(collected, x)
		return true
	})
	if !reflect.DeepEqual(collected, rb.ToArray()) {
		t.Error("Iterate disagrees with ToArray")
	}

	count := 0
	rb.Iterate(func(x uint64) bool {
		count++
		return count < 10
	})
	if count != 10 {
		t.Errorf("expected the iteration to stop after 10 values, got %d", count)
	}
}

func TestBufferedIterator64CrossesBlocks(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+uint64(iterBufferSize)*3+7)
	expected := rb.ToArray()
	var got []uint64
	it := rb.Iterator()
	for it.HasNext() {
		got = append(got, it.Next())
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %d values, got %d", len(expected), len(got))
	}
	if it.HasNext() {
		t.Error("expected the iterator to be exhausted")
	}
}

func TestIterator64Navigation(t *testing.T) {
	rb := New64(1, 2, 3, big, big+1000)

	it := rb.NewIterator()
	if !it.HasValue() || it.Value() != 1 {
		t.Fatalf("expected to start at 1, got %d", it.Value())
	}
	if !it.Next() || it.Value() != 2 {
		t.Fatalf("expected 2, got %d", it.Value())
	}
	if !it.AdvanceIfNeeded(big) || it.Value() != big {
		t.Fatalf("expected %d, got %d", big, it.Value())
	}
	if !it.Previous() || it.Value() != 3 {
		t.Fatalf("expected 3, got %d", it.Value())
	}

	clone := it.Clone()
	if clone.Value() != it.Value() {
		t.Error("the clone should point at the same value")
	}
	clone.Free()
	clone.Free()

	it.Reset()
	if it.Value() != 1 {
		t.Errorf("expected 1 after Reset, got %d", it.Value())
	}
	it.ResetToLast()
	if it.Value() != big+1000 {
		t.Errorf("expected %d after ResetToLast, got %d", big+1000, it.Value())
	}

	rev := rb.ReverseIterator()
	var backwards []uint64
	for rev.HasValue() {
		backwards = append(backwards, rev.Value())
		rev.Previous()
	}
	if !reflect.DeepEqual(backwards, []uint64{big + 1000, big, 3, 2, 1}) {
		t.Errorf("unexpected reverse iteration: %v", backwards)
	}
}

func TestIterator64Read(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+1000)

	it := rb.NewIterator()
	buf := make([]uint64, 300)
	total := 0
	for {
		n := it.Read(buf)
		if n == 0 {
			break
		}
		total += n
	}
	if total != 1000 {
		t.Errorf("expected 1000 values, got %d", total)
	}
	if it.Read(nil) != 0 {
		t.Error("expected zero values for an empty buffer")
	}

	rev := rb.ReverseIterator()
	if n := rev.ReadBackward(buf); n != 300 || buf[0] != big+999 {
		t.Errorf("expected to read 300 values starting at %d, got %d starting at %d", big+999, n, buf[0])
	}
	if rev.ReadBackward(nil) != 0 {
		t.Error("expected zero values for an empty buffer")
	}
}

func TestIterator64ReadRanges(t *testing.T) {
	rb := New64()
	rb.AddRange(big, big+10)
	rb.AddRange(big+100, big+110)
	rb.RunOptimize()

	it := rb.NewIterator()
	ranges := make([]Range64, 4)
	if n := it.ReadRanges(ranges); n != 2 {
		t.Fatalf("expected 2 ranges, got %d", n)
	}
	want := []Range64{{big, big + 9}, {big + 100, big + 109}}
	if !reflect.DeepEqual(ranges[:2], want) {
		t.Errorf("expected %v, got %v", want, ranges[:2])
	}
	if it.ReadRanges(nil) != 0 {
		t.Error("expected zero ranges for an empty buffer")
	}

	rev := rb.ReverseIterator()
	if n := rev.ReadPreviousRanges(ranges); n != 2 {
		t.Fatalf("expected 2 ranges, got %d", n)
	}
	want = []Range64{{big + 100, big + 109}, {big, big + 9}}
	if !reflect.DeepEqual(ranges[:2], want) {
		t.Errorf("expected %v, got %v", want, ranges[:2])
	}
	if rev.ReadPreviousRanges(nil) != 0 {
		t.Error("expected zero ranges for an empty buffer")
	}
}
