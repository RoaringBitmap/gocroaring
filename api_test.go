package gocroaring

import (
	"math"
	"reflect"
	"testing"
)

func TestVersion(t *testing.T) {
	if CRoaringMajor < 5 {
		t.Errorf("expected CRoaring 5 or better, got %d", CRoaringMajor)
	}
	expected := "5.1.0"
	if CRoaringVersion != expected {
		t.Errorf("expected version %s, got %s", expected, CRoaringVersion)
	}
}

func TestNewWithCapacity(t *testing.T) {
	rb := NewWithCapacity(16)
	if !rb.IsEmpty() {
		t.Error("expected an empty bitmap")
	}
	rb.Add(1, 2, 3)
	if rb.Cardinality() != 3 {
		t.Errorf("expected 3, got %d", rb.Cardinality())
	}
}

func TestFromRange(t *testing.T) {
	rb := FromRange(0, 10, 3)
	if got, want := rb.ToArray(), []uint32{0, 3, 6, 9}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	defer func() {
		if recover() == nil {
			t.Error("expected a panic on a zero step")
		}
	}()
	FromRange(0, 10, 0)
}

func TestAddRemoveChecked(t *testing.T) {
	rb := New()
	if !rb.AddChecked(7) {
		t.Error("expected 7 to be new")
	}
	if rb.AddChecked(7) {
		t.Error("expected 7 to already be present")
	}
	if !rb.RemoveChecked(7) {
		t.Error("expected 7 to be removed")
	}
	if rb.RemoveChecked(7) {
		t.Error("expected 7 to already be gone")
	}
}

func TestRemoveMany(t *testing.T) {
	rb := New(1, 2, 3, 4, 5)
	rb.RemoveMany(2, 4)
	if got, want := rb.ToArray(), []uint32{1, 3, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestClosedRanges(t *testing.T) {
	rb := New()
	rb.AddRangeClosed(1, 5)
	if got, want := rb.ToArray(), []uint32{1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if !rb.ContainsRangeClosed(1, 5) {
		t.Error("expected to contain [1, 5]")
	}
	if rb.ContainsRangeClosed(1, 6) {
		t.Error("did not expect to contain [1, 6]")
	}
	if got := rb.RangeCardinalityClosed(2, 4); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
	if got := rb.RangeCardinality(2, 4); got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
	rb.RemoveRangeClosed(2, 4)
	if got, want := rb.ToArray(), []uint32{1, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestBulkContext(t *testing.T) {
	rb := New()
	ctx := NewBulkContext()
	for i := uint32(0); i < 100000; i += 3 {
		rb.AddBulk(ctx, i)
	}
	if rb.Cardinality() != 33334 {
		t.Errorf("expected 33334, got %d", rb.Cardinality())
	}
	ctx = NewBulkContext()
	for i := uint32(0); i < 100000; i += 3 {
		if !rb.ContainsBulk(ctx, i) {
			t.Fatalf("expected to contain %d", i)
		}
	}
}

func TestRankSelectIndex(t *testing.T) {
	rb := New(2, 4, 8, 16)
	if got := rb.Rank(8); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
	if got := rb.GetIndex(8); got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
	if got := rb.GetIndex(9); got != -1 {
		t.Errorf("expected -1, got %d", got)
	}
	if got, err := rb.Select(2); err != nil || got != 8 {
		t.Errorf("expected 8, got %d (%v)", got, err)
	}
	if _, err := rb.Select(4); err != ErrNoSuchElement {
		t.Errorf("expected ErrNoSuchElement, got %v", err)
	}
	if got, want := rb.RankMany([]uint32{2, 4, 8, 16}), []uint64{1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got := rb.RankMany(nil); len(got) != 0 {
		t.Errorf("expected an empty result, got %v", got)
	}
}

func TestSubsets(t *testing.T) {
	rb := New(1, 2, 3)
	sub := New(1, 3)
	if !sub.IsSubset(rb) || !sub.IsStrictSubset(rb) {
		t.Error("expected sub to be a strict subset of rb")
	}
	if !rb.IsSubset(rb) {
		t.Error("expected rb to be a subset of itself")
	}
	if rb.IsStrictSubset(rb) {
		t.Error("did not expect rb to be a strict subset of itself")
	}
}

func TestIntersectWithRange(t *testing.T) {
	rb := New(10, 20)
	if !rb.IntersectWithRange(5, 15) {
		t.Error("expected an intersection with [5, 15)")
	}
	if rb.IntersectWithRange(11, 20) {
		t.Error("did not expect an intersection with [11, 20)")
	}
}

func TestCopyOnWrite(t *testing.T) {
	rb := New(1, 2, 3)
	if rb.GetCopyOnWrite() {
		t.Error("expected copy-on-write to be off by default")
	}
	rb.SetCopyOnWrite(true)
	if !rb.GetCopyOnWrite() {
		t.Error("expected copy-on-write to be on")
	}
	clone := rb.Clone()
	clone.Add(4)
	if rb.Contains(4) {
		t.Error("the clone should not have modified the original")
	}
	rb.SetCopyOnWrite(false)
	if rb.GetCopyOnWrite() {
		t.Error("expected copy-on-write to be off")
	}
}

func TestLazyOperations(t *testing.T) {
	rb1 := New(1, 2, 3)
	rb2 := New(3, 4, 5)

	lazy := LazyOr(rb1, rb2, false)
	lazy.RepairAfterLazy()
	if got, want := lazy.ToArray(), []uint32{1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}

	lazyXor := LazyXor(rb1, rb2)
	lazyXor.RepairAfterLazy()
	if got, want := lazyXor.ToArray(), []uint32{1, 2, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}

	inplace := rb1.Clone()
	inplace.LazyOrInplace(rb2, true)
	inplace.RepairAfterLazy()
	if got, want := inplace.ToArray(), []uint32{1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}

	inplaceXor := rb1.Clone()
	inplaceXor.LazyXorInplace(rb2)
	inplaceXor.RepairAfterLazy()
	if got, want := inplaceXor.ToArray(), []uint32{1, 2, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestFastOperations(t *testing.T) {
	rb1 := New(1, 2)
	rb2 := New(2, 3)
	rb3 := New(3, 4)

	if got, want := FastOr(rb1, rb2, rb3).ToArray(), []uint32{1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := FastOrHeap(rb1, rb2, rb3).ToArray(), []uint32{1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := FastXor(rb1, rb2, rb3).ToArray(), []uint32{1, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if !FastOr().IsEmpty() || !FastOrHeap().IsEmpty() || !FastXor().IsEmpty() {
		t.Error("expected empty bitmaps for empty inputs")
	}
}

func TestFlipClosed(t *testing.T) {
	rb := New(1, 2, 3)
	flipped := FlipClosed(rb, 2, 4)
	if got, want := flipped.ToArray(), []uint32{1, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	rb.FlipClosed(2, 4)
	if got, want := rb.ToArray(), []uint32{1, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestAddOffset(t *testing.T) {
	rb := New(1, 2, 3)
	if got, want := rb.AddOffset(10).ToArray(), []uint32{11, 12, 13}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got, want := rb.AddOffset(-2).ToArray(), []uint32{0, 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestShrinkToFitAndValidate(t *testing.T) {
	rb := NewWithCapacity(1024)
	rb.Add(1, 2, 3)
	rb.ShrinkToFit()
	if err := rb.InternalValidate(); err != nil {
		t.Errorf("expected a valid bitmap, got %v", err)
	}
}

func TestNativeSerialization(t *testing.T) {
	rb := New(1, 2, 3, 400000)
	buf := make([]byte, rb.NativeSerializedSizeInBytes())
	if err := rb.WriteNative(buf); err != nil {
		t.Fatal(err)
	}
	back, err := ReadNative(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(back) {
		t.Error("round trip through the native format failed")
	}
	if err := rb.WriteNative(buf[:1]); err != ErrNotEnoughSpace {
		t.Errorf("expected ErrNotEnoughSpace, got %v", err)
	}
	if _, err := ReadNative(nil); err != ErrEmptyBuffer {
		t.Errorf("expected ErrEmptyBuffer, got %v", err)
	}
}

func TestPortableSerialization(t *testing.T) {
	rb := New(1, 2, 3, 400000)
	buf := rb.ToBytes()
	if got := PortableDeserializeSize(buf); got != len(buf) {
		t.Errorf("expected %d, got %d", len(buf), got)
	}
	back, err := Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(back) {
		t.Error("round trip through the portable format failed")
	}
	validated, err := ReadValidated(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(validated) {
		t.Error("round trip through ReadValidated failed")
	}
	if _, err := ReadValidated([]byte{1, 2, 3, 4, 5, 6, 7, 8}); err == nil {
		t.Error("expected garbage to be rejected")
	}
	if _, err := Read(nil); err != ErrEmptyBuffer {
		t.Errorf("expected ErrEmptyBuffer, got %v", err)
	}
}

func TestFrozenViews(t *testing.T) {
	rb := New()
	rb.AddRange(0, 100000)
	rb.RunOptimize()

	frozen := AlignedBuffer(rb.FrozenSizeInBytes())
	if err := rb.WriteFrozen(frozen); err != nil {
		t.Fatal(err)
	}
	view, err := ReadFrozenView(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(view) {
		t.Error("the frozen view differs from the original")
	}

	// A misaligned buffer must be reported as such rather than crashing.
	misaligned := make([]byte, len(frozen)+1)
	copy(misaligned[1:], frozen)
	if _, err := ReadFrozenView(misaligned[1:]); err == nil {
		t.Error("expected a misaligned buffer to be rejected")
	}

	portable := rb.ToBytes()
	pview, err := ReadPortableFrozenView(portable)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Equals(pview) {
		t.Error("the portable frozen view differs from the original")
	}
}

func TestAlignedBuffer(t *testing.T) {
	if AlignedBuffer(0) != nil {
		t.Error("expected nil for a zero-sized buffer")
	}
	for size := 1; size < 200; size++ {
		b := AlignedBuffer(size)
		if len(b) != size {
			t.Fatalf("expected a buffer of size %d, got %d", size, len(b))
		}
		if !isAligned(b) {
			t.Fatalf("buffer of size %d is not aligned", size)
		}
	}
}

func TestRangeToArray(t *testing.T) {
	rb := New(1, 2, 3, 4, 5)
	if got, want := rb.RangeToArray(1, 3), []uint32{2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
	if got := rb.RangeToArray(3, 100); len(got) != 2 {
		t.Errorf("expected 2 values, got %v", got)
	}
	if got := rb.RangeToArray(10, 1); len(got) != 0 {
		t.Errorf("expected no values, got %v", got)
	}
}

func TestToDenseBitset(t *testing.T) {
	rb := New(0, 1, 64, 130)
	words, err := rb.ToDenseBitset()
	if err != nil {
		t.Fatal(err)
	}
	if len(words) < 3 {
		t.Fatalf("expected at least 3 words, got %d", len(words))
	}
	for _, v := range []uint32{0, 1, 64, 130} {
		if words[v/64]&(1<<(v%64)) == 0 {
			t.Errorf("expected bit %d to be set", v)
		}
	}
	if words[0]&(1<<2) != 0 {
		t.Error("did not expect bit 2 to be set")
	}
}

func TestAssignAndClear(t *testing.T) {
	rb := New(1, 2, 3)
	other := New(9)
	if !other.Assign(rb) {
		t.Error("Assign failed")
	}
	if !other.Equals(rb) {
		t.Error("expected the two bitmaps to be equal")
	}
	other.Clear()
	if !other.IsEmpty() {
		t.Error("expected an empty bitmap")
	}
}

func TestEqualsWithOtherType(t *testing.T) {
	if New(1).Equals("not a bitmap") {
		t.Error("expected a bitmap not to equal a string")
	}
}

func TestMinimumMaximumEmpty(t *testing.T) {
	rb := New()
	if rb.Minimum() != math.MaxUint32 {
		t.Errorf("expected MaxUint32, got %d", rb.Minimum())
	}
	if rb.Maximum() != 0 {
		t.Errorf("expected 0, got %d", rb.Maximum())
	}
}

func TestFreeIsIdempotent(t *testing.T) {
	rb := New(1, 2, 3)
	rb.Free()
	rb.Free()
}

func TestIterate(t *testing.T) {
	rb := New()
	rb.AddRange(0, 5000)
	var collected []uint32
	rb.Iterate(func(x uint32) bool {
		collected = append(collected, x)
		return true
	})
	if len(collected) != 5000 {
		t.Fatalf("expected 5000 values, got %d", len(collected))
	}
	if !reflect.DeepEqual(collected, rb.ToArray()) {
		t.Error("Iterate disagrees with ToArray")
	}

	count := 0
	rb.Iterate(func(x uint32) bool {
		count++
		return count < 10
	})
	if count != 10 {
		t.Errorf("expected the iteration to stop after 10 values, got %d", count)
	}
}

func TestBufferedIteratorCrossesBlocks(t *testing.T) {
	rb := New()
	// More values than the internal buffer holds, so that refills are covered.
	rb.AddRange(0, uint64(iterBufferSize)*3+7)
	expected := rb.ToArray()
	var got []uint32
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

func TestIteratorNavigation(t *testing.T) {
	rb := New(1, 2, 3, 100, 1000)

	it := rb.NewIterator()
	if !it.HasValue() || it.Value() != 1 {
		t.Fatalf("expected to start at 1, got %d", it.Value())
	}
	if !it.Next() || it.Value() != 2 {
		t.Fatalf("expected 2, got %d", it.Value())
	}
	if !it.AdvanceIfNeeded(100) || it.Value() != 100 {
		t.Fatalf("expected 100, got %d", it.Value())
	}
	if !it.Previous() || it.Value() != 3 {
		t.Fatalf("expected 3, got %d", it.Value())
	}
	if n := it.Skip(2); n != 2 || it.Value() != 1000 {
		t.Fatalf("expected to skip to 1000, got %d after %d skips", it.Value(), n)
	}
	if n := it.SkipBackward(1); n != 1 || it.Value() != 100 {
		t.Fatalf("expected to move back to 100, got %d after %d skips", it.Value(), n)
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
	if it.Value() != 1000 {
		t.Errorf("expected 1000 after ResetToLast, got %d", it.Value())
	}

	rev := rb.ReverseIterator()
	if !rev.HasValue() || rev.Value() != 1000 {
		t.Fatalf("expected the reverse iterator to start at 1000, got %d", rev.Value())
	}
	var backwards []uint32
	for rev.HasValue() {
		backwards = append(backwards, rev.Value())
		rev.Previous()
	}
	if !reflect.DeepEqual(backwards, []uint32{1000, 100, 3, 2, 1}) {
		t.Errorf("unexpected reverse iteration: %v", backwards)
	}
}

func TestIteratorRead(t *testing.T) {
	rb := New()
	rb.AddRange(0, 1000)

	it := rb.NewIterator()
	buf := make([]uint32, 300)
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
	if n := rev.ReadBackward(buf); n != 300 || buf[0] != 999 {
		t.Errorf("expected to read 300 values starting at 999, got %d starting at %d", n, buf[0])
	}
	if rev.ReadBackward(nil) != 0 {
		t.Error("expected zero values for an empty buffer")
	}
}

func TestIteratorReadRanges(t *testing.T) {
	rb := New()
	rb.AddRange(0, 10)
	rb.AddRange(100, 110)
	rb.RunOptimize()

	it := rb.NewIterator()
	ranges := make([]Range, 4)
	n := it.ReadRanges(ranges)
	if n != 2 {
		t.Fatalf("expected 2 ranges, got %d", n)
	}
	want := []Range{{0, 9}, {100, 109}}
	if !reflect.DeepEqual(ranges[:2], want) {
		t.Errorf("expected %v, got %v", want, ranges[:2])
	}
	if it.ReadRanges(nil) != 0 {
		t.Error("expected zero ranges for an empty buffer")
	}

	rev := rb.ReverseIterator()
	n = rev.ReadPreviousRanges(ranges)
	if n != 2 {
		t.Fatalf("expected 2 ranges, got %d", n)
	}
	want = []Range{{100, 109}, {0, 9}}
	if !reflect.DeepEqual(ranges[:2], want) {
		t.Errorf("expected %v, got %v", want, ranges[:2])
	}
	if rev.ReadPreviousRanges(nil) != 0 {
		t.Error("expected zero ranges for an empty buffer")
	}
}
