// Package gocroaring is a wrapper for CRoaring in go.
// It provides a fast compressed bitmap data structure.
// See http://roaringbitmap.org for details.
//
// The package exposes two types: Bitmap, which stores 32-bit integers and
// wraps the roaring_bitmap_t C type, and Bitmap64, which stores 64-bit
// integers and wraps the roaring64_bitmap_t C type.
//
// Bitmaps hold memory allocated by C. That memory is released automatically
// once the Go value becomes unreachable (we rely on runtime.AddCleanup), but
// you may call Free explicitly to release it eagerly.
package gocroaring

/*
#cgo CFLAGS: -O3 -std=c11

// None of the CRoaring entry points below calls back into Go, and none of them
// retains a pointer to the memory it is handed. Saying so lets cgo use the
// cheaper calling convention and keeps the Go buffers we pass from escaping to
// the heap.
//
// The frozen views are the exception: they keep the buffer they are given, so
// they are deliberately absent from the noescape list.
#cgo noescape bitset_create
#cgo noescape bitset_free
#cgo noescape bitset_size_in_words
#cgo noescape gocroaring_deserialize_validate
#cgo noescape roaring_bitmap_add
#cgo noescape roaring_bitmap_add_bulk
#cgo noescape roaring_bitmap_add_checked
#cgo noescape roaring_bitmap_add_many
#cgo noescape roaring_bitmap_add_offset
#cgo noescape roaring_bitmap_add_range
#cgo noescape roaring_bitmap_add_range_closed
#cgo noescape roaring_bitmap_and
#cgo noescape roaring_bitmap_and_cardinality
#cgo noescape roaring_bitmap_and_inplace
#cgo noescape roaring_bitmap_andnot
#cgo noescape roaring_bitmap_andnot_cardinality
#cgo noescape roaring_bitmap_andnot_inplace
#cgo noescape roaring_bitmap_clear
#cgo noescape roaring_bitmap_contains
#cgo noescape roaring_bitmap_contains_bulk
#cgo noescape roaring_bitmap_contains_range
#cgo noescape roaring_bitmap_contains_range_closed
#cgo noescape roaring_bitmap_copy
#cgo noescape roaring_bitmap_create
#cgo noescape roaring_bitmap_create_with_capacity
#cgo noescape roaring_bitmap_deserialize_safe
#cgo noescape roaring_bitmap_equals
#cgo noescape roaring_bitmap_flip
#cgo noescape roaring_bitmap_flip_closed
#cgo noescape roaring_bitmap_flip_inplace
#cgo noescape roaring_bitmap_flip_inplace_closed
#cgo noescape roaring_bitmap_free
#cgo noescape roaring_bitmap_from_range
#cgo noescape roaring_bitmap_frozen_serialize
#cgo noescape roaring_bitmap_frozen_size_in_bytes
#cgo noescape roaring_bitmap_get_cardinality
#cgo noescape roaring_bitmap_get_copy_on_write
#cgo noescape roaring_bitmap_get_index
#cgo noescape roaring_bitmap_internal_validate
#cgo noescape roaring_bitmap_intersect
#cgo noescape roaring_bitmap_intersect_with_range
#cgo noescape roaring_bitmap_is_empty
#cgo noescape roaring_bitmap_is_strict_subset
#cgo noescape roaring_bitmap_is_subset
#cgo noescape roaring_bitmap_jaccard_index
#cgo noescape roaring_bitmap_lazy_or
#cgo noescape roaring_bitmap_lazy_or_inplace
#cgo noescape roaring_bitmap_lazy_xor
#cgo noescape roaring_bitmap_lazy_xor_inplace
#cgo noescape roaring_bitmap_maximum
#cgo noescape roaring_bitmap_minimum
#cgo noescape roaring_bitmap_of_ptr
#cgo noescape roaring_bitmap_or
#cgo noescape roaring_bitmap_or_cardinality
#cgo noescape roaring_bitmap_or_inplace
#cgo noescape roaring_bitmap_or_many
#cgo noescape roaring_bitmap_or_many_heap
#cgo noescape roaring_bitmap_overwrite
#cgo noescape roaring_bitmap_portable_deserialize_safe
#cgo noescape roaring_bitmap_portable_deserialize_size
#cgo noescape roaring_bitmap_portable_serialize
#cgo noescape roaring_bitmap_portable_size_in_bytes
#cgo noescape roaring_bitmap_range_cardinality
#cgo noescape roaring_bitmap_range_cardinality_closed
#cgo noescape roaring_bitmap_range_uint32_array
#cgo noescape roaring_bitmap_rank
#cgo noescape roaring_bitmap_rank_many
#cgo noescape roaring_bitmap_remove
#cgo noescape roaring_bitmap_remove_checked
#cgo noescape roaring_bitmap_remove_many
#cgo noescape roaring_bitmap_remove_range
#cgo noescape roaring_bitmap_remove_range_closed
#cgo noescape roaring_bitmap_remove_run_compression
#cgo noescape roaring_bitmap_repair_after_lazy
#cgo noescape roaring_bitmap_run_optimize
#cgo noescape roaring_bitmap_select
#cgo noescape roaring_bitmap_serialize
#cgo noescape roaring_bitmap_set_copy_on_write
#cgo noescape roaring_bitmap_shrink_to_fit
#cgo noescape roaring_bitmap_size_in_bytes
#cgo noescape roaring_bitmap_statistics
#cgo noescape roaring_bitmap_to_bitset
#cgo noescape roaring_bitmap_to_uint32_array
#cgo noescape roaring_bitmap_xor
#cgo noescape roaring_bitmap_xor_cardinality
#cgo noescape roaring_bitmap_xor_inplace
#cgo noescape roaring_bitmap_xor_many

#cgo nocallback bitset_create
#cgo nocallback bitset_free
#cgo nocallback bitset_size_in_words
#cgo nocallback gocroaring_deserialize_validate
#cgo nocallback roaring_bitmap_add
#cgo nocallback roaring_bitmap_add_bulk
#cgo nocallback roaring_bitmap_add_checked
#cgo nocallback roaring_bitmap_add_many
#cgo nocallback roaring_bitmap_add_offset
#cgo nocallback roaring_bitmap_add_range
#cgo nocallback roaring_bitmap_add_range_closed
#cgo nocallback roaring_bitmap_and
#cgo nocallback roaring_bitmap_and_cardinality
#cgo nocallback roaring_bitmap_and_inplace
#cgo nocallback roaring_bitmap_andnot
#cgo nocallback roaring_bitmap_andnot_cardinality
#cgo nocallback roaring_bitmap_andnot_inplace
#cgo nocallback roaring_bitmap_clear
#cgo nocallback roaring_bitmap_contains
#cgo nocallback roaring_bitmap_contains_bulk
#cgo nocallback roaring_bitmap_contains_range
#cgo nocallback roaring_bitmap_contains_range_closed
#cgo nocallback roaring_bitmap_copy
#cgo nocallback roaring_bitmap_create
#cgo nocallback roaring_bitmap_create_with_capacity
#cgo nocallback roaring_bitmap_deserialize_safe
#cgo nocallback roaring_bitmap_equals
#cgo nocallback roaring_bitmap_flip
#cgo nocallback roaring_bitmap_flip_closed
#cgo nocallback roaring_bitmap_flip_inplace
#cgo nocallback roaring_bitmap_flip_inplace_closed
#cgo nocallback roaring_bitmap_free
#cgo nocallback roaring_bitmap_from_range
#cgo nocallback roaring_bitmap_frozen_serialize
#cgo nocallback roaring_bitmap_frozen_size_in_bytes
#cgo nocallback roaring_bitmap_frozen_view
#cgo nocallback roaring_bitmap_get_cardinality
#cgo nocallback roaring_bitmap_get_copy_on_write
#cgo nocallback roaring_bitmap_get_index
#cgo nocallback roaring_bitmap_internal_validate
#cgo nocallback roaring_bitmap_intersect
#cgo nocallback roaring_bitmap_intersect_with_range
#cgo nocallback roaring_bitmap_is_empty
#cgo nocallback roaring_bitmap_is_strict_subset
#cgo nocallback roaring_bitmap_is_subset
#cgo nocallback roaring_bitmap_jaccard_index
#cgo nocallback roaring_bitmap_lazy_or
#cgo nocallback roaring_bitmap_lazy_or_inplace
#cgo nocallback roaring_bitmap_lazy_xor
#cgo nocallback roaring_bitmap_lazy_xor_inplace
#cgo nocallback roaring_bitmap_maximum
#cgo nocallback roaring_bitmap_minimum
#cgo nocallback roaring_bitmap_of_ptr
#cgo nocallback roaring_bitmap_or
#cgo nocallback roaring_bitmap_or_cardinality
#cgo nocallback roaring_bitmap_or_inplace
#cgo nocallback roaring_bitmap_or_many
#cgo nocallback roaring_bitmap_or_many_heap
#cgo nocallback roaring_bitmap_overwrite
#cgo nocallback roaring_bitmap_portable_deserialize_frozen
#cgo nocallback roaring_bitmap_portable_deserialize_safe
#cgo nocallback roaring_bitmap_portable_deserialize_size
#cgo nocallback roaring_bitmap_portable_serialize
#cgo nocallback roaring_bitmap_portable_size_in_bytes
#cgo nocallback roaring_bitmap_range_cardinality
#cgo nocallback roaring_bitmap_range_cardinality_closed
#cgo nocallback roaring_bitmap_range_uint32_array
#cgo nocallback roaring_bitmap_rank
#cgo nocallback roaring_bitmap_rank_many
#cgo nocallback roaring_bitmap_remove
#cgo nocallback roaring_bitmap_remove_checked
#cgo nocallback roaring_bitmap_remove_many
#cgo nocallback roaring_bitmap_remove_range
#cgo nocallback roaring_bitmap_remove_range_closed
#cgo nocallback roaring_bitmap_remove_run_compression
#cgo nocallback roaring_bitmap_repair_after_lazy
#cgo nocallback roaring_bitmap_run_optimize
#cgo nocallback roaring_bitmap_select
#cgo nocallback roaring_bitmap_serialize
#cgo nocallback roaring_bitmap_set_copy_on_write
#cgo nocallback roaring_bitmap_shrink_to_fit
#cgo nocallback roaring_bitmap_size_in_bytes
#cgo nocallback roaring_bitmap_statistics
#cgo nocallback roaring_bitmap_to_bitset
#cgo nocallback roaring_bitmap_to_uint32_array
#cgo nocallback roaring_bitmap_xor
#cgo nocallback roaring_bitmap_xor_cardinality
#cgo nocallback roaring_bitmap_xor_inplace
#cgo nocallback roaring_bitmap_xor_many
#include "roaring.h"

// Deserialize and validate in a single cgo crossing: two calls across the
// Go/C boundary would cost roughly twice as much as one.
static inline roaring_bitmap_t *gocroaring_deserialize_validate(
    const char *buf, size_t maxbytes, const char **reason) {
    roaring_bitmap_t *r = roaring_bitmap_portable_deserialize_safe(buf, maxbytes);
    if (r == NULL) {
        *reason = "deserialization failed";
        return NULL;
    }
    if (!roaring_bitmap_internal_validate(r, reason)) {
        roaring_bitmap_free(r);
        return NULL;
    }
    return r;
}
*/
import "C"
import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"unsafe"
)

// CRoaringMajor, CRoaringMinor and CRoaringRevision report the version of the
// bundled CRoaring library.
const CRoaringMajor = C.ROARING_VERSION_MAJOR
const CRoaringMinor = C.ROARING_VERSION_MINOR
const CRoaringRevision = C.ROARING_VERSION_REVISION

// CRoaringVersion is the version of the bundled CRoaring library, as a string.
const CRoaringVersion = C.ROARING_VERSION

// FrozenAlignment is the alignment that a buffer must have before it can back
// a frozen view. See AlignedBuffer and ReadFrozenView.
const FrozenAlignment = 32

var (
	// ErrNotEnoughSpace is returned by the serialization routines when the
	// provided buffer is too small.
	ErrNotEnoughSpace = errors.New("not enough space")
	// ErrEmptyBuffer is returned when a deserialization routine is handed an
	// empty buffer.
	ErrEmptyBuffer = errors.New("empty buffer")
	// ErrDeserialize is returned when a buffer does not contain a valid bitmap.
	ErrDeserialize = errors.New("failed to read roaring bitmap")
	// ErrMisaligned is returned by the frozen view routines when the buffer is
	// not aligned on a FrozenAlignment boundary.
	ErrMisaligned = fmt.Errorf("buffer is not aligned on a %d-byte boundary", FrozenAlignment)
	// ErrNoSuchElement is returned by Select when the rank is out of range.
	ErrNoSuchElement = errors.New("no such element")
)

// Bitmap is a compressed bitmap of 32-bit integers.
//
// A Bitmap is not safe for concurrent modification.
type Bitmap struct {
	cpointer *C.roaring_bitmap_t
	cleanup  runtime.Cleanup
	// pinned keeps the buffer backing a frozen view alive for as long as the
	// bitmap is alive. It is nil for ordinary bitmaps.
	pinned *byte
}

// wrap takes ownership of a C bitmap and arranges for it to be freed once the
// returned Bitmap becomes unreachable. It panics if p is nil.
func wrap(p *C.roaring_bitmap_t) *Bitmap {
	if p == nil {
		panic("C code returned a null pointer.")
	}
	rb := &Bitmap{cpointer: p}
	// runtime.AddCleanup is cheaper than runtime.SetFinalizer: the bitmap is
	// reclaimed in a single garbage collection cycle and it is never
	// resurrected. The C pointer is passed as the cleanup argument so that the
	// closure does not capture rb, which would keep it alive forever.
	rb.cleanup = runtime.AddCleanup(rb, func(p *C.roaring_bitmap_t) {
		C.roaring_bitmap_free(p)
	}, p)
	return rb
}

// Free releases the memory held by the bitmap. Using the bitmap afterwards is
// a mistake. Calling Free more than once is harmless.
func (rb *Bitmap) Free() {
	if rb.cpointer == nil {
		return
	}
	rb.cleanup.Stop()
	C.roaring_bitmap_free(rb.cpointer)
	rb.cpointer = nil
	rb.pinned = nil
}

// New creates a new Bitmap with any number of initial values.
// This function may panic if the allocation failed.
func New(x ...uint32) *Bitmap {
	if len(x) == 0 {
		return wrap(C.roaring_bitmap_create())
	}
	rb := wrap(C.roaring_bitmap_of_ptr(C.size_t(len(x)), (*C.uint32_t)(unsafe.Pointer(&x[0]))))
	runtime.KeepAlive(x)
	return rb
}

// NewWithCapacity creates a new Bitmap with room for the given number of
// containers, avoiding some reallocations when the bitmap is populated.
// This function may panic if the allocation failed.
func NewWithCapacity(capacity uint32) *Bitmap {
	return wrap(C.roaring_bitmap_create_with_capacity(C.uint32_t(capacity)))
}

// FromRange creates a bitmap containing min, min+step, min+2*step... up to but
// not including max. The step must be strictly positive.
// This function may panic if the allocation failed.
func FromRange(min, max uint64, step uint32) *Bitmap {
	if step == 0 {
		panic("gocroaring: FromRange requires a strictly positive step")
	}
	return wrap(C.roaring_bitmap_from_range(C.uint64_t(min), C.uint64_t(max), C.uint32_t(step)))
}

// Clone creates a copy of the Bitmap.
// This function may panic if the allocation failed.
func (rb *Bitmap) Clone() *Bitmap {
	b := wrap(C.roaring_bitmap_copy(rb.cpointer))
	runtime.KeepAlive(rb)
	return b
}

// Assign copies x2 over rb, returning false if the copy failed.
func (rb *Bitmap) Assign(x2 *Bitmap) bool {
	answer := bool(C.roaring_bitmap_overwrite(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// Printf writes a description of the bitmap to stdout.
func (rb *Bitmap) Printf() {
	fmt.Print("{")
	i := rb.Iterator()
	counter := 30
	for i.HasNext() {
		counter = counter - 1
		if counter == 0 {
			fmt.Print("...")
		}
		fmt.Print(i.Next())
		if i.HasNext() {
			fmt.Print(",")
		}
	}
	fmt.Print("}")
}

// String creates a string representation of the Bitmap.
func (rb *Bitmap) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	i := rb.Iterator()
	counter := 0
	for i.HasNext() {
		// to avoid exhausting the memory
		if counter > 0x40000 {
			buffer.WriteString("...")
			break
		}
		if counter > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(strconv.FormatUint(uint64(i.Next()), 10))
		counter++
	}
	buffer.WriteString("}")
	return buffer.String()
}

////////////////////////////////////////////////////////////////////////////////
// Adding and removing values
////////////////////////////////////////////////////////////////////////////////

// Add the integer(s) x to the bitmap.
func (rb *Bitmap) Add(x ...uint32) {
	switch len(x) {
	case 0:
		return
	case 1:
		C.roaring_bitmap_add(rb.cpointer, C.uint32_t(x[0]))
	default:
		C.roaring_bitmap_add_many(rb.cpointer, C.size_t(len(x)), (*C.uint32_t)(unsafe.Pointer(&x[0])))
		runtime.KeepAlive(x)
	}
	runtime.KeepAlive(rb)
}

// AddChecked adds the integer x to the bitmap and reports whether a new value
// was actually added.
func (rb *Bitmap) AddChecked(x uint32) bool {
	answer := bool(C.roaring_bitmap_add_checked(rb.cpointer, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// AddRange adds all values in the range [min, max).
func (rb *Bitmap) AddRange(min, max uint64) {
	C.roaring_bitmap_add_range(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// AddRangeClosed adds all values in the range [min, max].
func (rb *Bitmap) AddRangeClosed(min, max uint32) {
	C.roaring_bitmap_add_range_closed(rb.cpointer, C.uint32_t(min), C.uint32_t(max))
	runtime.KeepAlive(rb)
}

// Remove the integer x from the bitmap.
func (rb *Bitmap) Remove(x uint32) {
	C.roaring_bitmap_remove(rb.cpointer, C.uint32_t(x))
	runtime.KeepAlive(rb)
}

// RemoveChecked removes the integer x from the bitmap and reports whether a
// value was actually removed.
func (rb *Bitmap) RemoveChecked(x uint32) bool {
	answer := bool(C.roaring_bitmap_remove_checked(rb.cpointer, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// RemoveMany removes the integer(s) x from the bitmap.
func (rb *Bitmap) RemoveMany(x ...uint32) {
	if len(x) == 0 {
		return
	}
	C.roaring_bitmap_remove_many(rb.cpointer, C.size_t(len(x)), (*C.uint32_t)(unsafe.Pointer(&x[0])))
	runtime.KeepAlive(x)
	runtime.KeepAlive(rb)
}

// RemoveRange removes all values in the range [min, max).
func (rb *Bitmap) RemoveRange(min, max uint64) {
	C.roaring_bitmap_remove_range(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// RemoveRangeClosed removes all values in the range [min, max].
func (rb *Bitmap) RemoveRangeClosed(min, max uint32) {
	C.roaring_bitmap_remove_range_closed(rb.cpointer, C.uint32_t(min), C.uint32_t(max))
	runtime.KeepAlive(rb)
}

// Clear removes all elements from the bitmap.
func (rb *Bitmap) Clear() {
	C.roaring_bitmap_clear(rb.cpointer)
	runtime.KeepAlive(rb)
}

// BulkContext accelerates repeated accesses to a bitmap when the values are
// provided in ascending order. A context is tied to the bitmap it was last
// used with: it must not be reused across bitmaps, and it must be discarded
// whenever the bitmap is modified by anything other than AddBulk.
type BulkContext struct {
	ctx C.roaring_bulk_context_t
}

// NewBulkContext returns a fresh context for use with AddBulk and
// ContainsBulk.
func NewBulkContext() *BulkContext {
	return &BulkContext{}
}

// AddBulk adds the integer x to the bitmap, using ctx to remember the last
// container visited. Values should be provided in ascending order.
func (rb *Bitmap) AddBulk(ctx *BulkContext, x uint32) {
	C.roaring_bitmap_add_bulk(rb.cpointer, &ctx.ctx, C.uint32_t(x))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(ctx)
}

// ContainsBulk reports whether the integer x is in the bitmap, using ctx to
// remember the last container visited. Values should be provided in ascending
// order.
func (rb *Bitmap) ContainsBulk(ctx *BulkContext, x uint32) bool {
	answer := bool(C.roaring_bitmap_contains_bulk(rb.cpointer, &ctx.ctx, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(ctx)
	return answer
}

////////////////////////////////////////////////////////////////////////////////
// Queries
////////////////////////////////////////////////////////////////////////////////

// Contains returns true if the integer is contained in the bitmap.
func (rb *Bitmap) Contains(x uint32) bool {
	answer := bool(C.roaring_bitmap_contains(rb.cpointer, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// ContainsRange returns true if all the integers in the range [x, y) are
// contained in the bitmap.
func (rb *Bitmap) ContainsRange(x, y uint64) bool {
	answer := bool(C.roaring_bitmap_contains_range(rb.cpointer, C.uint64_t(x), C.uint64_t(y)))
	runtime.KeepAlive(rb)
	return answer
}

// ContainsRangeClosed returns true if all the integers in the range [x, y] are
// contained in the bitmap.
func (rb *Bitmap) ContainsRangeClosed(x, y uint32) bool {
	answer := bool(C.roaring_bitmap_contains_range_closed(rb.cpointer, C.uint32_t(x), C.uint32_t(y)))
	runtime.KeepAlive(rb)
	return answer
}

// Cardinality returns the number of integers contained in the bitmap.
func (rb *Bitmap) Cardinality() uint64 {
	answer := uint64(C.roaring_bitmap_get_cardinality(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// GetCardinality returns the number of integers contained in the bitmap.
func (rb *Bitmap) GetCardinality() uint64 {
	return rb.Cardinality()
}

// RangeCardinality returns the number of integers in the bitmap that fall in
// the range [min, max).
func (rb *Bitmap) RangeCardinality(min, max uint64) uint64 {
	answer := uint64(C.roaring_bitmap_range_cardinality(rb.cpointer, C.uint64_t(min), C.uint64_t(max)))
	runtime.KeepAlive(rb)
	return answer
}

// RangeCardinalityClosed returns the number of integers in the bitmap that
// fall in the range [min, max].
func (rb *Bitmap) RangeCardinalityClosed(min, max uint32) uint64 {
	answer := uint64(C.roaring_bitmap_range_cardinality_closed(rb.cpointer, C.uint32_t(min), C.uint32_t(max)))
	runtime.KeepAlive(rb)
	return answer
}

// IsEmpty returns true if the Bitmap is empty (it is faster than doing
// Cardinality() == 0).
func (rb *Bitmap) IsEmpty() bool {
	answer := bool(C.roaring_bitmap_is_empty(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Maximum returns the largest of the integers contained in the bitmap,
// or 0 if the bitmap is empty.
func (rb *Bitmap) Maximum() uint32 {
	answer := uint32(C.roaring_bitmap_maximum(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Minimum returns the smallest of the integers contained in the bitmap,
// or math.MaxUint32 if the bitmap is empty.
func (rb *Bitmap) Minimum() uint32 {
	answer := uint32(C.roaring_bitmap_minimum(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Rank returns the number of values smaller or equal to x.
func (rb *Bitmap) Rank(x uint32) uint64 {
	answer := uint64(C.roaring_bitmap_rank(rb.cpointer, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// RankMany returns the rank of each value in vals. The values must be sorted
// in ascending order. It is faster than calling Rank repeatedly.
func (rb *Bitmap) RankMany(vals []uint32) []uint64 {
	answer := make([]uint64, len(vals))
	if len(vals) == 0 {
		return answer
	}
	begin := (*C.uint32_t)(unsafe.Pointer(&vals[0]))
	end := (*C.uint32_t)(unsafe.Pointer(&vals[len(vals)-1]))
	// The C API takes a one-past-the-end pointer.
	end = (*C.uint32_t)(unsafe.Add(unsafe.Pointer(end), unsafe.Sizeof(vals[0])))
	C.roaring_bitmap_rank_many(rb.cpointer, begin, end, (*C.uint64_t)(unsafe.Pointer(&answer[0])))
	runtime.KeepAlive(vals)
	runtime.KeepAlive(answer)
	runtime.KeepAlive(rb)
	return answer
}

// GetIndex returns the index of x in the bitmap, or -1 if x is not present.
// Unlike Rank, it distinguishes a missing value from a value of rank zero.
func (rb *Bitmap) GetIndex(x uint32) int64 {
	answer := int64(C.roaring_bitmap_get_index(rb.cpointer, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// Select returns the element having the designated rank, if it exists.
func (rb *Bitmap) Select(rank uint32) (uint32, error) {
	var element C.uint32_t
	exists := bool(C.roaring_bitmap_select(rb.cpointer, C.uint32_t(rank), &element))
	runtime.KeepAlive(rb)
	if !exists {
		return 0, ErrNoSuchElement
	}
	return uint32(element), nil
}

// Equals returns true if the two bitmaps contain the same integers.
func (rb *Bitmap) Equals(o interface{}) bool {
	srb, ok := o.(*Bitmap)
	if !ok {
		return false
	}
	answer := bool(C.roaring_bitmap_equals(rb.cpointer, srb.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(srb)
	return answer
}

// IsSubset returns true if every integer of rb is also in x2.
func (rb *Bitmap) IsSubset(x2 *Bitmap) bool {
	answer := bool(C.roaring_bitmap_is_subset(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// IsStrictSubset returns true if every integer of rb is also in x2 and the two
// bitmaps differ.
func (rb *Bitmap) IsStrictSubset(x2 *Bitmap) bool {
	answer := bool(C.roaring_bitmap_is_strict_subset(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// Intersect checks whether the two bitmaps intersect.
func (rb *Bitmap) Intersect(x2 *Bitmap) bool {
	answer := bool(C.roaring_bitmap_intersect(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// IntersectWithRange checks whether the bitmap intersects the range [x, y).
func (rb *Bitmap) IntersectWithRange(x, y uint64) bool {
	answer := bool(C.roaring_bitmap_intersect_with_range(rb.cpointer, C.uint64_t(x), C.uint64_t(y)))
	runtime.KeepAlive(rb)
	return answer
}

// AndCardinality computes the size of the intersection between two bitmaps.
func (rb *Bitmap) AndCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_and_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// OrCardinality computes the size of the union between two bitmaps.
func (rb *Bitmap) OrCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_or_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// XorCardinality computes the size of the symmetric difference between two
// bitmaps.
func (rb *Bitmap) XorCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_xor_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// AndNotCardinality computes the size of the difference between two bitmaps.
func (rb *Bitmap) AndNotCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_andnot_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// JaccardIndex computes the Jaccard index between two bitmaps.
func (rb *Bitmap) JaccardIndex(x2 *Bitmap) float64 {
	answer := float64(C.roaring_bitmap_jaccard_index(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// InternalValidate performs internal consistency checks. It is useful after
// deserializing bitmaps from untrusted sources. It returns nil if the bitmap
// is consistent, and an error describing the problem otherwise.
func (rb *Bitmap) InternalValidate() error {
	var reason *C.char
	ok := bool(C.roaring_bitmap_internal_validate(rb.cpointer, &reason))
	runtime.KeepAlive(rb)
	if ok {
		return nil
	}
	return errors.New(C.GoString(reason))
}

// GetCopyOnWrite reports whether the bitmap uses copy-on-write containers.
func (rb *Bitmap) GetCopyOnWrite() bool {
	answer := bool(C.roaring_bitmap_get_copy_on_write(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// SetCopyOnWrite turns copy-on-write on or off. Copy-on-write saves memory and
// avoids copies, but it requires more care in a threaded context. If you
// enable it, enable it on all of your bitmaps: mixing bitmaps with and without
// copy-on-write is unsafe.
func (rb *Bitmap) SetCopyOnWrite(cow bool) {
	C.roaring_bitmap_set_copy_on_write(rb.cpointer, C.bool(cow))
	runtime.KeepAlive(rb)
}

////////////////////////////////////////////////////////////////////////////////
// In-place set operations
////////////////////////////////////////////////////////////////////////////////

// And computes the intersection between two bitmaps and stores the result in
// the current bitmap.
func (rb *Bitmap) And(x2 *Bitmap) {
	C.roaring_bitmap_and_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Xor computes the symmetric difference between two bitmaps and stores the
// result in the current bitmap.
func (rb *Bitmap) Xor(x2 *Bitmap) {
	C.roaring_bitmap_xor_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Or computes the union between two bitmaps and stores the result in the
// current bitmap.
func (rb *Bitmap) Or(x2 *Bitmap) {
	C.roaring_bitmap_or_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// AndNot computes the difference between two bitmaps and stores the result in
// the current bitmap.
func (rb *Bitmap) AndNot(x2 *Bitmap) {
	C.roaring_bitmap_andnot_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// LazyOrInplace computes the union with x2 in place, leaving the bitmap in an
// invalid state until RepairAfterLazy is called. Set bitsetconversion to true
// to eagerly convert containers to bitsets when it might help.
func (rb *Bitmap) LazyOrInplace(x2 *Bitmap, bitsetconversion bool) {
	C.roaring_bitmap_lazy_or_inplace(rb.cpointer, x2.cpointer, C.bool(bitsetconversion))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// LazyXorInplace computes the symmetric difference with x2 in place, leaving
// the bitmap in an invalid state until RepairAfterLazy is called. The two
// bitmaps must be distinct.
func (rb *Bitmap) LazyXorInplace(x2 *Bitmap) {
	C.roaring_bitmap_lazy_xor_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// RepairAfterLazy restores a bitmap produced by the lazy operations to a valid
// state. It must be called before any other operation.
func (rb *Bitmap) RepairAfterLazy() {
	C.roaring_bitmap_repair_after_lazy(rb.cpointer)
	runtime.KeepAlive(rb)
}

// Flip negates the bits in the given range (i.e., [rangeStart, rangeEnd)): any
// integer present in this range and in the bitmap is removed, and any integer
// in the range that was absent is added.
func (rb *Bitmap) Flip(rangeStart, rangeEnd uint64) {
	C.roaring_bitmap_flip_inplace(rb.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd))
	runtime.KeepAlive(rb)
}

// FlipClosed negates the bits in the given range (i.e., [rangeStart, rangeEnd]).
func (rb *Bitmap) FlipClosed(rangeStart, rangeEnd uint32) {
	C.roaring_bitmap_flip_inplace_closed(rb.cpointer, C.uint32_t(rangeStart), C.uint32_t(rangeEnd))
	runtime.KeepAlive(rb)
}

// RunOptimize improves the compression of the bitmap (call this after
// populating a new bitmap); it returns true if the bitmap was modified.
func (rb *Bitmap) RunOptimize() bool {
	answer := bool(C.roaring_bitmap_run_optimize(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// RemoveRunCompression removes run-length encoding even when it is more space
// efficient; it returns whether a change was applied.
func (rb *Bitmap) RemoveRunCompression() bool {
	answer := bool(C.roaring_bitmap_remove_run_compression(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// ShrinkToFit releases unused memory and returns how many bytes were freed.
func (rb *Bitmap) ShrinkToFit() int {
	answer := int(C.roaring_bitmap_shrink_to_fit(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

////////////////////////////////////////////////////////////////////////////////
// Set operations returning a new bitmap
////////////////////////////////////////////////////////////////////////////////

// Or computes the union between two bitmaps and returns the result.
// This function may panic if the allocation failed.
func Or(x1, x2 *Bitmap) *Bitmap {
	b := wrap(C.roaring_bitmap_or(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// And computes the intersection between two bitmaps and returns the result.
// This function may panic if the allocation failed.
func And(x1, x2 *Bitmap) *Bitmap {
	b := wrap(C.roaring_bitmap_and(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// Xor computes the symmetric difference between two bitmaps and returns the
// result.
// This function may panic if the allocation failed.
func Xor(x1, x2 *Bitmap) *Bitmap {
	b := wrap(C.roaring_bitmap_xor(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// AndNot computes the difference between two bitmaps and returns the result.
// This function may panic if the allocation failed.
func AndNot(x1, x2 *Bitmap) *Bitmap {
	b := wrap(C.roaring_bitmap_andnot(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// LazyOr computes the union between two bitmaps, leaving the result in an
// invalid state until RepairAfterLazy is called on it. Set bitsetconversion to
// true to eagerly convert containers to bitsets when it might help.
// This function may panic if the allocation failed.
func LazyOr(x1, x2 *Bitmap, bitsetconversion bool) *Bitmap {
	b := wrap(C.roaring_bitmap_lazy_or(x1.cpointer, x2.cpointer, C.bool(bitsetconversion)))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// LazyXor computes the symmetric difference between two bitmaps, leaving the
// result in an invalid state until RepairAfterLazy is called on it.
// This function may panic if the allocation failed.
func LazyXor(x1, x2 *Bitmap) *Bitmap {
	b := wrap(C.roaring_bitmap_lazy_xor(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// Flip negates the bits in the given range (i.e., [rangeStart, rangeEnd)) and
// returns the result.
// This function may panic if the allocation failed.
func Flip(bm *Bitmap, rangeStart, rangeEnd uint64) *Bitmap {
	b := wrap(C.roaring_bitmap_flip(bm.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd)))
	runtime.KeepAlive(bm)
	return b
}

// FlipClosed negates the bits in the given range (i.e., [rangeStart, rangeEnd])
// and returns the result.
// This function may panic if the allocation failed.
func FlipClosed(bm *Bitmap, rangeStart, rangeEnd uint32) *Bitmap {
	b := wrap(C.roaring_bitmap_flip_closed(bm.cpointer, C.uint32_t(rangeStart), C.uint32_t(rangeEnd)))
	runtime.KeepAlive(bm)
	return b
}

// AddOffset returns a copy of the bitmap with the given (possibly negative)
// offset added to every value. Values that fall outside the 32-bit range are
// dropped.
// This function may panic if the allocation failed.
func (rb *Bitmap) AddOffset(offset int64) *Bitmap {
	b := wrap(C.roaring_bitmap_add_offset(rb.cpointer, C.int64_t(offset)))
	runtime.KeepAlive(rb)
	return b
}

// cpointers collects the C pointers of a slice of bitmaps.
func cpointers(bitmaps []*Bitmap) []*C.roaring_bitmap_t {
	po := make([]*C.roaring_bitmap_t, len(bitmaps))
	for i, v := range bitmaps {
		po[i] = v.cpointer
	}
	return po
}

// FastOr computes the union between many bitmaps quickly, as opposed to having
// to call Or repeatedly.
// This function may panic if the allocation failed.
func FastOr(bitmaps ...*Bitmap) *Bitmap {
	if len(bitmaps) == 0 {
		return New()
	}
	po := cpointers(bitmaps)
	b := wrap(C.roaring_bitmap_or_many(C.size_t(len(po)), (**C.roaring_bitmap_t)(unsafe.Pointer(&po[0]))))
	runtime.KeepAlive(bitmaps)
	runtime.KeepAlive(po)
	return b
}

// FastOrHeap computes the union between many bitmaps using a heap. It can be
// faster than FastOr when the bitmaps are numerous and of uneven sizes.
// This function may panic if the allocation failed.
func FastOrHeap(bitmaps ...*Bitmap) *Bitmap {
	if len(bitmaps) == 0 {
		return New()
	}
	po := cpointers(bitmaps)
	b := wrap(C.roaring_bitmap_or_many_heap(C.uint32_t(len(po)), (**C.roaring_bitmap_t)(unsafe.Pointer(&po[0]))))
	runtime.KeepAlive(bitmaps)
	runtime.KeepAlive(po)
	return b
}

// FastXor computes the symmetric difference between many bitmaps quickly, as
// opposed to having to call Xor repeatedly.
// This function may panic if the allocation failed.
func FastXor(bitmaps ...*Bitmap) *Bitmap {
	if len(bitmaps) == 0 {
		return New()
	}
	po := cpointers(bitmaps)
	b := wrap(C.roaring_bitmap_xor_many(C.size_t(len(po)), (**C.roaring_bitmap_t)(unsafe.Pointer(&po[0]))))
	runtime.KeepAlive(bitmaps)
	runtime.KeepAlive(po)
	return b
}

////////////////////////////////////////////////////////////////////////////////
// Serialization
////////////////////////////////////////////////////////////////////////////////

// SerializedSizeInBytes computes the serialized size in bytes of the Bitmap,
// using the portable format.
func (rb *Bitmap) SerializedSizeInBytes() int {
	answer := int(C.roaring_bitmap_portable_size_in_bytes(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Write writes a serialized version of this bitmap to b, using the portable
// format. The buffer must be at least SerializedSizeInBytes long.
func (rb *Bitmap) Write(b []byte) error {
	if len(b) < rb.SerializedSizeInBytes() {
		return ErrNotEnoughSpace
	}
	if len(b) == 0 {
		return ErrNotEnoughSpace
	}
	C.roaring_bitmap_portable_serialize(rb.cpointer, (*C.char)(unsafe.Pointer(&b[0])))
	runtime.KeepAlive(b)
	runtime.KeepAlive(rb)
	return nil
}

// ToBytes returns a serialized version of this bitmap, using the portable
// format.
func (rb *Bitmap) ToBytes() []byte {
	b := make([]byte, rb.SerializedSizeInBytes())
	if len(b) > 0 {
		C.roaring_bitmap_portable_serialize(rb.cpointer, (*C.char)(unsafe.Pointer(&b[0])))
		runtime.KeepAlive(b)
	}
	runtime.KeepAlive(rb)
	return b
}

// Read reads a serialized version of the bitmap, in the portable format. The
// buffer is not retained. If the data comes from an untrusted source, prefer
// ReadValidated.
func Read(b []byte) (*Bitmap, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	p := C.roaring_bitmap_portable_deserialize_safe((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)))
	runtime.KeepAlive(b)
	if p == nil {
		return nil, ErrDeserialize
	}
	return wrap(p), nil
}

// ReadValidated reads a serialized version of the bitmap, in the portable
// format, and checks its internal consistency. Use it when the data comes from
// an untrusted source. The deserialization and the validation are performed in
// a single crossing of the Go/C boundary.
func ReadValidated(b []byte) (*Bitmap, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	var reason *C.char
	p := C.gocroaring_deserialize_validate((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)), &reason)
	runtime.KeepAlive(b)
	if p == nil {
		return nil, fmt.Errorf("%w: %s", ErrDeserialize, C.GoString(reason))
	}
	return wrap(p), nil
}

// PortableDeserializeSize returns how many bytes would be read from b by Read,
// or zero if b does not start with a valid bitmap.
func PortableDeserializeSize(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	answer := int(C.roaring_bitmap_portable_deserialize_size((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b))))
	runtime.KeepAlive(b)
	return answer
}

// NativeSerializedSizeInBytes computes the serialized size in bytes of the
// Bitmap, using the non-portable native format. The native format is not
// compatible with the Java and Go implementations; prefer the portable format
// unless you know what you are doing.
func (rb *Bitmap) NativeSerializedSizeInBytes() int {
	answer := int(C.roaring_bitmap_size_in_bytes(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// WriteNative writes a serialized version of this bitmap to b, using the
// non-portable native format.
func (rb *Bitmap) WriteNative(b []byte) error {
	if len(b) < rb.NativeSerializedSizeInBytes() {
		return ErrNotEnoughSpace
	}
	if len(b) == 0 {
		return ErrNotEnoughSpace
	}
	C.roaring_bitmap_serialize(rb.cpointer, (*C.char)(unsafe.Pointer(&b[0])))
	runtime.KeepAlive(b)
	runtime.KeepAlive(rb)
	return nil
}

// ReadNative reads a bitmap written by WriteNative. The buffer is not
// retained.
func ReadNative(b []byte) (*Bitmap, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	p := C.roaring_bitmap_deserialize_safe(unsafe.Pointer(&b[0]), C.size_t(len(b)))
	runtime.KeepAlive(b)
	if p == nil {
		return nil, ErrDeserialize
	}
	return wrap(p), nil
}

// FrozenSizeInBytes computes the frozen serialized size in bytes.
func (rb *Bitmap) FrozenSizeInBytes() int {
	answer := int(C.roaring_bitmap_frozen_size_in_bytes(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// WriteFrozen writes a serialized version of the bitmap to b in the frozen
// format. The buffer must be at least FrozenSizeInBytes long. The frozen
// format is endian-sensitive and version-specific; it is meant for fast
// reloading of data you produced yourself, not for interchange.
func (rb *Bitmap) WriteFrozen(b []byte) error {
	if len(b) < rb.FrozenSizeInBytes() {
		return ErrNotEnoughSpace
	}
	if len(b) == 0 {
		return ErrNotEnoughSpace
	}
	C.roaring_bitmap_frozen_serialize(rb.cpointer, (*C.char)(unsafe.Pointer(&b[0])))
	runtime.KeepAlive(b)
	runtime.KeepAlive(rb)
	return nil
}

// AlignedBuffer returns a byte slice of the requested size whose first byte is
// aligned on a FrozenAlignment boundary, as required by ReadFrozenView.
func AlignedBuffer(size int) []byte {
	if size == 0 {
		return nil
	}
	b := make([]byte, size+FrozenAlignment)
	offset := int(uintptr(unsafe.Pointer(&b[0])) % FrozenAlignment)
	if offset != 0 {
		offset = FrozenAlignment - offset
	}
	return b[offset : offset+size : offset+size]
}

// isAligned reports whether the first byte of b sits on a FrozenAlignment
// boundary.
func isAligned(b []byte) bool {
	return uintptr(unsafe.Pointer(&b[0]))%FrozenAlignment == 0
}

// ReadFrozenView reads a frozen serialized version of the bitmap, as written
// by WriteFrozen. The result is immutable: attempting to mutate it will fail
// catastrophically. The buffer must be aligned on a FrozenAlignment boundary
// (see AlignedBuffer) and its length must be exactly the length that was
// written. A reference to the buffer is retained for the lifetime of the view.
func ReadFrozenView(b []byte) (*Bitmap, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	if !isAligned(b) {
		return nil, ErrMisaligned
	}
	p := C.roaring_bitmap_frozen_view((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)))
	if p == nil {
		return nil, ErrDeserialize
	}
	rb := wrap(p)
	rb.pinned = &b[0]
	return rb, nil
}

// ReadPortableFrozenView reads a bitmap written in the portable format (see
// Write) without copying the container data: the result is a read-only view
// over b. A reference to the buffer is retained for the lifetime of the view.
func ReadPortableFrozenView(b []byte) (*Bitmap, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	p := C.roaring_bitmap_portable_deserialize_frozen((*C.char)(unsafe.Pointer(&b[0])))
	if p == nil {
		return nil, ErrDeserialize
	}
	rb := wrap(p)
	rb.pinned = &b[0]
	return rb, nil
}

////////////////////////////////////////////////////////////////////////////////
// Conversion
////////////////////////////////////////////////////////////////////////////////

// ToArray creates a new slice containing all of the integers stored in the
// Bitmap in sorted order.
func (rb *Bitmap) ToArray() []uint32 {
	card := rb.Cardinality()
	array := make([]uint32, card)
	if card > 0 {
		C.roaring_bitmap_to_uint32_array(rb.cpointer, (*C.uint32_t)(unsafe.Pointer(&array[0])))
		runtime.KeepAlive(array)
	}
	runtime.KeepAlive(rb)
	return array
}

// RangeToArray returns at most limit integers from the bitmap, in sorted
// order, starting at the given offset (a rank, not a value).
func (rb *Bitmap) RangeToArray(offset, limit uint64) []uint32 {
	card := rb.Cardinality()
	if offset >= card || limit == 0 {
		return []uint32{}
	}
	if limit > card-offset {
		limit = card - offset
	}
	array := make([]uint32, limit)
	C.roaring_bitmap_range_uint32_array(rb.cpointer, C.size_t(offset), C.size_t(limit),
		(*C.uint32_t)(unsafe.Pointer(&array[0])))
	runtime.KeepAlive(array)
	runtime.KeepAlive(rb)
	return array
}

// ToDenseBitset converts the bitmap to an uncompressed bitset, returned as a
// slice of 64-bit words in little-endian bit order: value x is present when
// bit x%64 of word x/64 is set. Beware that the result can be large: a bitmap
// containing a single large value produces a slice proportional to that value.
func (rb *Bitmap) ToDenseBitset() ([]uint64, error) {
	bs := C.bitset_create()
	if bs == nil {
		return nil, errors.New("failed to allocate a bitset")
	}
	defer C.bitset_free(bs)
	ok := bool(C.roaring_bitmap_to_bitset(rb.cpointer, bs))
	runtime.KeepAlive(rb)
	if !ok {
		return nil, errors.New("failed to convert the bitmap to a bitset")
	}
	words := int(C.bitset_size_in_words(bs))
	answer := make([]uint64, words)
	if words > 0 {
		copy(answer, unsafe.Slice((*uint64)(unsafe.Pointer(bs.array)), words))
	}
	return answer, nil
}

////////////////////////////////////////////////////////////////////////////////
// Statistics
////////////////////////////////////////////////////////////////////////////////

// Stats returns some statistics about the roaring bitmap.
func (rb *Bitmap) Stats() map[string]uint64 {
	var stat C.roaring_statistics_t
	C.roaring_bitmap_statistics(rb.cpointer, &stat)
	runtime.KeepAlive(rb)
	return map[string]uint64{
		"cardinality":         uint64(stat.cardinality),
		"n_containers":        uint64(stat.n_containers),
		"n_array_containers":  uint64(stat.n_array_containers),
		"n_run_containers":    uint64(stat.n_run_containers),
		"n_bitset_containers": uint64(stat.n_bitset_containers),

		"n_bytes_array_containers":  uint64(stat.n_bytes_array_containers),
		"n_bytes_run_containers":    uint64(stat.n_bytes_run_containers),
		"n_bytes_bitset_containers": uint64(stat.n_bytes_bitset_containers),

		"n_values_array_containers":  uint64(stat.n_values_array_containers),
		"n_values_run_containers":    uint64(stat.n_values_run_containers),
		"n_values_bitset_containers": uint64(stat.n_values_bitset_containers),

		"min_value": uint64(stat.min_value),
		"max_value": uint64(stat.max_value),
	}
}

// Statistics describes the internal structure of a bitmap.
type Statistics struct {
	Cardinality uint64
	Containers  uint64

	ArrayContainers      uint64
	ArrayContainerBytes  uint64
	ArrayContainerValues uint64

	BitmapContainers      uint64
	BitmapContainerBytes  uint64
	BitmapContainerValues uint64

	RunContainers      uint64
	RunContainerBytes  uint64
	RunContainerValues uint64

	// MinValue and MaxValue are undefined when Cardinality is zero.
	MinValue uint64
	MaxValue uint64
}

// StatsStruct is the same as Stats but returns a typed struct.
// See https://github.com/RoaringBitmap/roaring/pull/73 for the rationale.
func (rb *Bitmap) StatsStruct() Statistics {
	var stat C.roaring_statistics_t
	C.roaring_bitmap_statistics(rb.cpointer, &stat)
	runtime.KeepAlive(rb)
	return Statistics{
		Cardinality: uint64(stat.cardinality),
		Containers:  uint64(stat.n_containers),

		ArrayContainers:      uint64(stat.n_array_containers),
		ArrayContainerBytes:  uint64(stat.n_bytes_array_containers),
		ArrayContainerValues: uint64(stat.n_values_array_containers),

		BitmapContainers:      uint64(stat.n_bitset_containers),
		BitmapContainerBytes:  uint64(stat.n_bytes_bitset_containers),
		BitmapContainerValues: uint64(stat.n_values_bitset_containers),

		RunContainers:      uint64(stat.n_run_containers),
		RunContainerBytes:  uint64(stat.n_bytes_run_containers),
		RunContainerValues: uint64(stat.n_values_run_containers),

		MinValue: uint64(stat.min_value),
		MaxValue: uint64(stat.max_value),
	}
}
