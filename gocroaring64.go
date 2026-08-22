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
#cgo noescape gocroaring64_deserialize_validate
#cgo noescape roaring64_bitmap_add
#cgo noescape roaring64_bitmap_add_bulk
#cgo noescape roaring64_bitmap_add_checked
#cgo noescape roaring64_bitmap_add_many
#cgo noescape roaring64_bitmap_add_offset_signed
#cgo noescape roaring64_bitmap_add_range
#cgo noescape roaring64_bitmap_add_range_closed
#cgo noescape roaring64_bitmap_and
#cgo noescape roaring64_bitmap_and_cardinality
#cgo noescape roaring64_bitmap_and_inplace
#cgo noescape roaring64_bitmap_andnot
#cgo noescape roaring64_bitmap_andnot_cardinality
#cgo noescape roaring64_bitmap_andnot_inplace
#cgo noescape roaring64_bitmap_clear
#cgo noescape roaring64_bitmap_contains
#cgo noescape roaring64_bitmap_contains_bulk
#cgo noescape roaring64_bitmap_contains_range
#cgo noescape roaring64_bitmap_contains_range_closed
#cgo noescape roaring64_bitmap_copy
#cgo noescape roaring64_bitmap_create
#cgo noescape roaring64_bitmap_equals
#cgo noescape roaring64_bitmap_flip
#cgo noescape roaring64_bitmap_flip_closed
#cgo noescape roaring64_bitmap_flip_closed_inplace
#cgo noescape roaring64_bitmap_flip_inplace
#cgo noescape roaring64_bitmap_free
#cgo noescape roaring64_bitmap_from_range
#cgo noescape roaring64_bitmap_frozen_serialize
#cgo noescape roaring64_bitmap_frozen_size_in_bytes
#cgo noescape roaring64_bitmap_get_cardinality
#cgo noescape roaring64_bitmap_get_index
#cgo noescape roaring64_bitmap_internal_validate
#cgo noescape roaring64_bitmap_intersect
#cgo noescape roaring64_bitmap_intersect_with_range
#cgo noescape roaring64_bitmap_is_empty
#cgo noescape roaring64_bitmap_is_strict_subset
#cgo noescape roaring64_bitmap_is_subset
#cgo noescape roaring64_bitmap_jaccard_index
#cgo noescape roaring64_bitmap_maximum
#cgo noescape roaring64_bitmap_minimum
#cgo noescape roaring64_bitmap_move_from_roaring32
#cgo noescape roaring64_bitmap_of_ptr
#cgo noescape roaring64_bitmap_or
#cgo noescape roaring64_bitmap_or_cardinality
#cgo noescape roaring64_bitmap_or_inplace
#cgo noescape roaring64_bitmap_overwrite
#cgo noescape roaring64_bitmap_portable_deserialize_safe
#cgo noescape roaring64_bitmap_portable_deserialize_size
#cgo noescape roaring64_bitmap_portable_serialize
#cgo noescape roaring64_bitmap_portable_size_in_bytes
#cgo noescape roaring64_bitmap_range_cardinality
#cgo noescape roaring64_bitmap_range_closed_cardinality
#cgo noescape roaring64_bitmap_rank
#cgo noescape roaring64_bitmap_remove
#cgo noescape roaring64_bitmap_remove_bulk
#cgo noescape roaring64_bitmap_remove_checked
#cgo noescape roaring64_bitmap_remove_many
#cgo noescape roaring64_bitmap_remove_range
#cgo noescape roaring64_bitmap_remove_range_closed
#cgo noescape roaring64_bitmap_remove_run_compression
#cgo noescape roaring64_bitmap_run_optimize
#cgo noescape roaring64_bitmap_select
#cgo noescape roaring64_bitmap_shrink_to_fit
#cgo noescape roaring64_bitmap_statistics
#cgo noescape roaring64_bitmap_to_uint64_array
#cgo noescape roaring64_bitmap_xor
#cgo noescape roaring64_bitmap_xor_cardinality
#cgo noescape roaring64_bitmap_xor_inplace

#cgo nocallback gocroaring64_deserialize_validate
#cgo nocallback roaring64_bitmap_add
#cgo nocallback roaring64_bitmap_add_bulk
#cgo nocallback roaring64_bitmap_add_checked
#cgo nocallback roaring64_bitmap_add_many
#cgo nocallback roaring64_bitmap_add_offset_signed
#cgo nocallback roaring64_bitmap_add_range
#cgo nocallback roaring64_bitmap_add_range_closed
#cgo nocallback roaring64_bitmap_and
#cgo nocallback roaring64_bitmap_and_cardinality
#cgo nocallback roaring64_bitmap_and_inplace
#cgo nocallback roaring64_bitmap_andnot
#cgo nocallback roaring64_bitmap_andnot_cardinality
#cgo nocallback roaring64_bitmap_andnot_inplace
#cgo nocallback roaring64_bitmap_clear
#cgo nocallback roaring64_bitmap_contains
#cgo nocallback roaring64_bitmap_contains_bulk
#cgo nocallback roaring64_bitmap_contains_range
#cgo nocallback roaring64_bitmap_contains_range_closed
#cgo nocallback roaring64_bitmap_copy
#cgo nocallback roaring64_bitmap_create
#cgo nocallback roaring64_bitmap_equals
#cgo nocallback roaring64_bitmap_flip
#cgo nocallback roaring64_bitmap_flip_closed
#cgo nocallback roaring64_bitmap_flip_closed_inplace
#cgo nocallback roaring64_bitmap_flip_inplace
#cgo nocallback roaring64_bitmap_free
#cgo nocallback roaring64_bitmap_from_range
#cgo nocallback roaring64_bitmap_frozen_serialize
#cgo nocallback roaring64_bitmap_frozen_size_in_bytes
#cgo nocallback roaring64_bitmap_frozen_view
#cgo nocallback roaring64_bitmap_get_cardinality
#cgo nocallback roaring64_bitmap_get_index
#cgo nocallback roaring64_bitmap_internal_validate
#cgo nocallback roaring64_bitmap_intersect
#cgo nocallback roaring64_bitmap_intersect_with_range
#cgo nocallback roaring64_bitmap_is_empty
#cgo nocallback roaring64_bitmap_is_strict_subset
#cgo nocallback roaring64_bitmap_is_subset
#cgo nocallback roaring64_bitmap_jaccard_index
#cgo nocallback roaring64_bitmap_maximum
#cgo nocallback roaring64_bitmap_minimum
#cgo nocallback roaring64_bitmap_move_from_roaring32
#cgo nocallback roaring64_bitmap_of_ptr
#cgo nocallback roaring64_bitmap_or
#cgo nocallback roaring64_bitmap_or_cardinality
#cgo nocallback roaring64_bitmap_or_inplace
#cgo nocallback roaring64_bitmap_overwrite
#cgo nocallback roaring64_bitmap_portable_deserialize_frozen
#cgo nocallback roaring64_bitmap_portable_deserialize_safe
#cgo nocallback roaring64_bitmap_portable_deserialize_size
#cgo nocallback roaring64_bitmap_portable_serialize
#cgo nocallback roaring64_bitmap_portable_size_in_bytes
#cgo nocallback roaring64_bitmap_range_cardinality
#cgo nocallback roaring64_bitmap_range_closed_cardinality
#cgo nocallback roaring64_bitmap_rank
#cgo nocallback roaring64_bitmap_remove
#cgo nocallback roaring64_bitmap_remove_bulk
#cgo nocallback roaring64_bitmap_remove_checked
#cgo nocallback roaring64_bitmap_remove_many
#cgo nocallback roaring64_bitmap_remove_range
#cgo nocallback roaring64_bitmap_remove_range_closed
#cgo nocallback roaring64_bitmap_remove_run_compression
#cgo nocallback roaring64_bitmap_run_optimize
#cgo nocallback roaring64_bitmap_select
#cgo nocallback roaring64_bitmap_shrink_to_fit
#cgo nocallback roaring64_bitmap_statistics
#cgo nocallback roaring64_bitmap_to_uint64_array
#cgo nocallback roaring64_bitmap_xor
#cgo nocallback roaring64_bitmap_xor_cardinality
#cgo nocallback roaring64_bitmap_xor_inplace
#include "roaring.h"

// Deserialize and validate in a single cgo crossing.
static inline roaring64_bitmap_t *gocroaring64_deserialize_validate(
    const char *buf, size_t maxbytes, const char **reason) {
    roaring64_bitmap_t *r = roaring64_bitmap_portable_deserialize_safe(buf, maxbytes);
    if (r == NULL) {
        *reason = "deserialization failed";
        return NULL;
    }
    if (!roaring64_bitmap_internal_validate(r, reason)) {
        roaring64_bitmap_free(r);
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

// Frozen64Alignment is the alignment that a buffer must have before it can
// back a 64-bit frozen view. See AlignedBuffer64 and ReadFrozenView64.
const Frozen64Alignment = 64

// ErrMisaligned64 is returned by ReadFrozenView64 when the buffer is not
// aligned on a Frozen64Alignment boundary.
var ErrMisaligned64 = fmt.Errorf("buffer is not aligned on a %d-byte boundary", Frozen64Alignment)

// ErrNotShrunken is returned by Bitmap64.WriteFrozen when ShrinkToFit has not
// been called since the last modification of the bitmap.
var ErrNotShrunken = errors.New("the bitmap must be shrunk to fit before it can be frozen")

// Bitmap64 is a compressed bitmap of 64-bit integers.
//
// A Bitmap64 is not safe for concurrent modification.
type Bitmap64 struct {
	cpointer *C.roaring64_bitmap_t
	cleanup  runtime.Cleanup
	// pinned keeps the buffer backing a frozen view alive for as long as the
	// bitmap is alive. It is nil for ordinary bitmaps.
	pinned *byte
}

// wrap64 takes ownership of a C bitmap and arranges for it to be freed once
// the returned Bitmap64 becomes unreachable. It panics if p is nil.
func wrap64(p *C.roaring64_bitmap_t) *Bitmap64 {
	if p == nil {
		panic("C code returned a null pointer.")
	}
	rb := &Bitmap64{cpointer: p}
	// See the comment in wrap: runtime.AddCleanup is cheaper than
	// runtime.SetFinalizer, and the closure must not capture rb.
	rb.cleanup = runtime.AddCleanup(rb, func(p *C.roaring64_bitmap_t) {
		C.roaring64_bitmap_free(p)
	}, p)
	return rb
}

// Free releases the memory held by the bitmap. Using the bitmap afterwards is
// a mistake. Calling Free more than once is harmless.
func (rb *Bitmap64) Free() {
	if rb.cpointer == nil {
		return
	}
	rb.cleanup.Stop()
	C.roaring64_bitmap_free(rb.cpointer)
	rb.cpointer = nil
	rb.pinned = nil
}

// New64 creates a new Bitmap64 with any number of initial values.
// This function may panic if the allocation failed.
func New64(x ...uint64) *Bitmap64 {
	if len(x) == 0 {
		return wrap64(C.roaring64_bitmap_create())
	}
	rb := wrap64(C.roaring64_bitmap_of_ptr(C.size_t(len(x)), (*C.uint64_t)(unsafe.Pointer(&x[0]))))
	runtime.KeepAlive(x)
	return rb
}

// FromRange64 creates a bitmap containing min, min+step, min+2*step... up to
// but not including max. An empty range yields an empty bitmap. The step must
// be strictly positive.
// This function may panic if the allocation failed.
func FromRange64(min, max, step uint64) *Bitmap64 {
	if step == 0 {
		panic("gocroaring: FromRange64 requires a strictly positive step")
	}
	// C returns a null pointer for an empty range, which is not an error.
	if max <= min {
		return New64()
	}
	return wrap64(C.roaring64_bitmap_from_range(C.uint64_t(min), C.uint64_t(max), C.uint64_t(step)))
}

// MoveFrom32 builds a 64-bit bitmap by moving the containers out of a 32-bit
// bitmap. This avoids copying the container data, but it leaves the source
// bitmap empty.
//
// The containers are taken without regard for copy-on-write sharing, so do not
// use it on a bitmap whose containers are shared with another one (that is, a
// bitmap that has copy-on-write enabled and has been cloned). Clone first if
// you need the source intact.
// This function may panic if the allocation failed.
func MoveFrom32(from *Bitmap) *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_move_from_roaring32(from.cpointer))
	runtime.KeepAlive(from)
	return b
}

// Clone creates a copy of the Bitmap64.
// This function may panic if the allocation failed.
func (rb *Bitmap64) Clone() *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_copy(rb.cpointer))
	runtime.KeepAlive(rb)
	return b
}

// Assign copies x2 over rb.
func (rb *Bitmap64) Assign(x2 *Bitmap64) {
	if rb.aliases(x2) {
		return // already a copy of itself
	}
	C.roaring64_bitmap_overwrite(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// String creates a string representation of the Bitmap64.
func (rb *Bitmap64) String() string {
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
		buffer.WriteString(strconv.FormatUint(i.Next(), 10))
		counter++
	}
	buffer.WriteString("}")
	return buffer.String()
}

////////////////////////////////////////////////////////////////////////////////
// Adding and removing values
////////////////////////////////////////////////////////////////////////////////

// Add the integer(s) x to the bitmap.
func (rb *Bitmap64) Add(x ...uint64) {
	switch len(x) {
	case 0:
		return
	case 1:
		C.roaring64_bitmap_add(rb.cpointer, C.uint64_t(x[0]))
	default:
		C.roaring64_bitmap_add_many(rb.cpointer, C.size_t(len(x)), (*C.uint64_t)(unsafe.Pointer(&x[0])))
		runtime.KeepAlive(x)
	}
	runtime.KeepAlive(rb)
}

// AddChecked adds the integer x to the bitmap and reports whether a new value
// was actually added.
func (rb *Bitmap64) AddChecked(x uint64) bool {
	answer := bool(C.roaring64_bitmap_add_checked(rb.cpointer, C.uint64_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// AddRange adds all values in the range [min, max).
func (rb *Bitmap64) AddRange(min, max uint64) {
	C.roaring64_bitmap_add_range(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// AddRangeClosed adds all values in the range [min, max].
func (rb *Bitmap64) AddRangeClosed(min, max uint64) {
	C.roaring64_bitmap_add_range_closed(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// Remove the integer x from the bitmap.
func (rb *Bitmap64) Remove(x uint64) {
	C.roaring64_bitmap_remove(rb.cpointer, C.uint64_t(x))
	runtime.KeepAlive(rb)
}

// RemoveChecked removes the integer x from the bitmap and reports whether a
// value was actually removed.
func (rb *Bitmap64) RemoveChecked(x uint64) bool {
	answer := bool(C.roaring64_bitmap_remove_checked(rb.cpointer, C.uint64_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// RemoveMany removes the integer(s) x from the bitmap.
func (rb *Bitmap64) RemoveMany(x ...uint64) {
	if len(x) == 0 {
		return
	}
	C.roaring64_bitmap_remove_many(rb.cpointer, C.size_t(len(x)), (*C.uint64_t)(unsafe.Pointer(&x[0])))
	runtime.KeepAlive(x)
	runtime.KeepAlive(rb)
}

// RemoveRange removes all values in the range [min, max).
func (rb *Bitmap64) RemoveRange(min, max uint64) {
	C.roaring64_bitmap_remove_range(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// RemoveRangeClosed removes all values in the range [min, max].
func (rb *Bitmap64) RemoveRangeClosed(min, max uint64) {
	C.roaring64_bitmap_remove_range_closed(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// Clear removes all elements from the bitmap.
func (rb *Bitmap64) Clear() {
	C.roaring64_bitmap_clear(rb.cpointer)
	runtime.KeepAlive(rb)
}

// BulkContext64 accelerates repeated accesses to a Bitmap64 when the values
// are provided in ascending order. A context is tied to the bitmap it was last
// used with: it must not be reused across bitmaps, and it must be discarded
// whenever the bitmap is modified by anything other than AddBulk or
// RemoveBulk.
type BulkContext64 struct {
	ctx C.roaring64_bulk_context_t
}

// NewBulkContext64 returns a fresh context for use with AddBulk, RemoveBulk
// and ContainsBulk.
func NewBulkContext64() *BulkContext64 {
	return &BulkContext64{}
}

// AddBulk adds the integer x to the bitmap, using ctx to remember the last
// container visited. Values should be provided in ascending order.
func (rb *Bitmap64) AddBulk(ctx *BulkContext64, x uint64) {
	C.roaring64_bitmap_add_bulk(rb.cpointer, &ctx.ctx, C.uint64_t(x))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(ctx)
}

// RemoveBulk removes the integer x from the bitmap, using ctx to remember the
// last container visited. Values should be provided in ascending order.
func (rb *Bitmap64) RemoveBulk(ctx *BulkContext64, x uint64) {
	C.roaring64_bitmap_remove_bulk(rb.cpointer, &ctx.ctx, C.uint64_t(x))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(ctx)
}

// ContainsBulk reports whether the integer x is in the bitmap, using ctx to
// remember the last container visited. Values should be provided in ascending
// order.
func (rb *Bitmap64) ContainsBulk(ctx *BulkContext64, x uint64) bool {
	answer := bool(C.roaring64_bitmap_contains_bulk(rb.cpointer, &ctx.ctx, C.uint64_t(x)))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(ctx)
	return answer
}

////////////////////////////////////////////////////////////////////////////////
// Queries
////////////////////////////////////////////////////////////////////////////////

// Contains returns true if the integer is contained in the bitmap.
func (rb *Bitmap64) Contains(x uint64) bool {
	answer := bool(C.roaring64_bitmap_contains(rb.cpointer, C.uint64_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// ContainsRange returns true if all the integers in the range [x, y) are
// contained in the bitmap.
func (rb *Bitmap64) ContainsRange(x, y uint64) bool {
	answer := bool(C.roaring64_bitmap_contains_range(rb.cpointer, C.uint64_t(x), C.uint64_t(y)))
	runtime.KeepAlive(rb)
	return answer
}

// ContainsRangeClosed returns true if all the integers in the range [x, y] are
// contained in the bitmap.
func (rb *Bitmap64) ContainsRangeClosed(x, y uint64) bool {
	answer := bool(C.roaring64_bitmap_contains_range_closed(rb.cpointer, C.uint64_t(x), C.uint64_t(y)))
	runtime.KeepAlive(rb)
	return answer
}

// Cardinality returns the number of integers contained in the bitmap.
func (rb *Bitmap64) Cardinality() uint64 {
	answer := uint64(C.roaring64_bitmap_get_cardinality(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// GetCardinality returns the number of integers contained in the bitmap.
func (rb *Bitmap64) GetCardinality() uint64 {
	return rb.Cardinality()
}

// RangeCardinality returns the number of integers in the bitmap that fall in
// the range [min, max).
func (rb *Bitmap64) RangeCardinality(min, max uint64) uint64 {
	answer := uint64(C.roaring64_bitmap_range_cardinality(rb.cpointer, C.uint64_t(min), C.uint64_t(max)))
	runtime.KeepAlive(rb)
	return answer
}

// RangeCardinalityClosed returns the number of integers in the bitmap that
// fall in the range [min, max].
func (rb *Bitmap64) RangeCardinalityClosed(min, max uint64) uint64 {
	answer := uint64(C.roaring64_bitmap_range_closed_cardinality(rb.cpointer, C.uint64_t(min), C.uint64_t(max)))
	runtime.KeepAlive(rb)
	return answer
}

// IsEmpty returns true if the Bitmap64 is empty (it is faster than doing
// Cardinality() == 0).
func (rb *Bitmap64) IsEmpty() bool {
	answer := bool(C.roaring64_bitmap_is_empty(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Maximum returns the largest of the integers contained in the bitmap,
// or 0 if the bitmap is empty.
func (rb *Bitmap64) Maximum() uint64 {
	answer := uint64(C.roaring64_bitmap_maximum(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Minimum returns the smallest of the integers contained in the bitmap,
// or math.MaxUint64 if the bitmap is empty.
func (rb *Bitmap64) Minimum() uint64 {
	answer := uint64(C.roaring64_bitmap_minimum(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Rank returns the number of values smaller or equal to x.
func (rb *Bitmap64) Rank(x uint64) uint64 {
	answer := uint64(C.roaring64_bitmap_rank(rb.cpointer, C.uint64_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// GetIndex returns the index of x in the bitmap, or -1 if x is not present.
// Unlike Rank, it distinguishes a missing value from a value of rank zero.
func (rb *Bitmap64) GetIndex(x uint64) int64 {
	var index C.uint64_t
	found := bool(C.roaring64_bitmap_get_index(rb.cpointer, C.uint64_t(x), &index))
	runtime.KeepAlive(rb)
	if !found {
		return -1
	}
	return int64(index)
}

// Select returns the element having the designated rank, if it exists.
func (rb *Bitmap64) Select(rank uint64) (uint64, error) {
	var element C.uint64_t
	exists := bool(C.roaring64_bitmap_select(rb.cpointer, C.uint64_t(rank), &element))
	runtime.KeepAlive(rb)
	if !exists {
		return 0, ErrNoSuchElement
	}
	return uint64(element), nil
}

// Equals returns true if the two bitmaps contain the same integers.
func (rb *Bitmap64) Equals(o interface{}) bool {
	srb, ok := o.(*Bitmap64)
	if !ok {
		return false
	}
	answer := bool(C.roaring64_bitmap_equals(rb.cpointer, srb.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(srb)
	return answer
}

// IsSubset returns true if every integer of rb is also in x2.
func (rb *Bitmap64) IsSubset(x2 *Bitmap64) bool {
	answer := bool(C.roaring64_bitmap_is_subset(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// IsStrictSubset returns true if every integer of rb is also in x2 and the two
// bitmaps differ.
func (rb *Bitmap64) IsStrictSubset(x2 *Bitmap64) bool {
	answer := bool(C.roaring64_bitmap_is_strict_subset(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// Intersect checks whether the two bitmaps intersect.
func (rb *Bitmap64) Intersect(x2 *Bitmap64) bool {
	answer := bool(C.roaring64_bitmap_intersect(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// IntersectWithRange checks whether the bitmap intersects the range [x, y).
func (rb *Bitmap64) IntersectWithRange(x, y uint64) bool {
	answer := bool(C.roaring64_bitmap_intersect_with_range(rb.cpointer, C.uint64_t(x), C.uint64_t(y)))
	runtime.KeepAlive(rb)
	return answer
}

// JaccardIndex computes the Jaccard index between two bitmaps.
func (rb *Bitmap64) JaccardIndex(x2 *Bitmap64) float64 {
	answer := float64(C.roaring64_bitmap_jaccard_index(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// AndCardinality computes the size of the intersection between two bitmaps.
func (rb *Bitmap64) AndCardinality(x2 *Bitmap64) uint64 {
	answer := uint64(C.roaring64_bitmap_and_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// OrCardinality computes the size of the union between two bitmaps.
func (rb *Bitmap64) OrCardinality(x2 *Bitmap64) uint64 {
	answer := uint64(C.roaring64_bitmap_or_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// XorCardinality computes the size of the symmetric difference between two
// bitmaps.
func (rb *Bitmap64) XorCardinality(x2 *Bitmap64) uint64 {
	answer := uint64(C.roaring64_bitmap_xor_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// AndNotCardinality computes the size of the difference between two bitmaps.
func (rb *Bitmap64) AndNotCardinality(x2 *Bitmap64) uint64 {
	answer := uint64(C.roaring64_bitmap_andnot_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// InternalValidate performs internal consistency checks. It is useful after
// deserializing bitmaps from untrusted sources. It returns nil if the bitmap
// is consistent, and an error describing the problem otherwise.
func (rb *Bitmap64) InternalValidate() error {
	var reason *C.char
	ok := bool(C.roaring64_bitmap_internal_validate(rb.cpointer, &reason))
	runtime.KeepAlive(rb)
	if ok {
		return nil
	}
	return errors.New(C.GoString(reason))
}

////////////////////////////////////////////////////////////////////////////////
// In-place set operations
////////////////////////////////////////////////////////////////////////////////

// aliases reports whether the two wrappers refer to the same C bitmap. Several
// of the in-place C routines assert that their operands are distinct, so we
// answer the aliased cases ourselves rather than let the C library abort.
func (rb *Bitmap64) aliases(x2 *Bitmap64) bool {
	return rb == x2 || rb.cpointer == x2.cpointer
}

// And computes the intersection between two bitmaps and stores the result in
// the current bitmap.
func (rb *Bitmap64) And(x2 *Bitmap64) {
	if rb.aliases(x2) {
		return // x AND x is x
	}
	C.roaring64_bitmap_and_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Xor computes the symmetric difference between two bitmaps and stores the
// result in the current bitmap.
func (rb *Bitmap64) Xor(x2 *Bitmap64) {
	if rb.aliases(x2) {
		rb.Clear() // x XOR x is empty
		return
	}
	C.roaring64_bitmap_xor_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Or computes the union between two bitmaps and stores the result in the
// current bitmap.
func (rb *Bitmap64) Or(x2 *Bitmap64) {
	if rb.aliases(x2) {
		return // x OR x is x
	}
	C.roaring64_bitmap_or_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// AndNot computes the difference between two bitmaps and stores the result in
// the current bitmap.
func (rb *Bitmap64) AndNot(x2 *Bitmap64) {
	if rb.aliases(x2) {
		rb.Clear() // x ANDNOT x is empty
		return
	}
	C.roaring64_bitmap_andnot_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Flip negates the bits in the given range (i.e., [rangeStart, rangeEnd)).
func (rb *Bitmap64) Flip(rangeStart, rangeEnd uint64) {
	C.roaring64_bitmap_flip_inplace(rb.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd))
	runtime.KeepAlive(rb)
}

// FlipClosed negates the bits in the given range (i.e., [rangeStart, rangeEnd]).
func (rb *Bitmap64) FlipClosed(rangeStart, rangeEnd uint64) {
	C.roaring64_bitmap_flip_closed_inplace(rb.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd))
	runtime.KeepAlive(rb)
}

// RunOptimize improves the compression of the bitmap (call this after
// populating a new bitmap); it returns true if the bitmap was modified.
func (rb *Bitmap64) RunOptimize() bool {
	answer := bool(C.roaring64_bitmap_run_optimize(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// RemoveRunCompression removes run-length encoding even when it is more space
// efficient; it returns whether a change was applied.
func (rb *Bitmap64) RemoveRunCompression() bool {
	answer := bool(C.roaring64_bitmap_remove_run_compression(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// ShrinkToFit releases unused memory and returns how many bytes were freed.
func (rb *Bitmap64) ShrinkToFit() int {
	answer := int(C.roaring64_bitmap_shrink_to_fit(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

////////////////////////////////////////////////////////////////////////////////
// Set operations returning a new bitmap
////////////////////////////////////////////////////////////////////////////////

// Or64 computes the union between two bitmaps and returns the result.
// This function may panic if the allocation failed.
func Or64(x1, x2 *Bitmap64) *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_or(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// And64 computes the intersection between two bitmaps and returns the result.
// This function may panic if the allocation failed.
func And64(x1, x2 *Bitmap64) *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_and(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// Xor64 computes the symmetric difference between two bitmaps and returns the
// result.
// This function may panic if the allocation failed.
func Xor64(x1, x2 *Bitmap64) *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_xor(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// AndNot64 computes the difference between two bitmaps and returns the result.
// This function may panic if the allocation failed.
func AndNot64(x1, x2 *Bitmap64) *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_andnot(x1.cpointer, x2.cpointer))
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	return b
}

// Flip64 negates the bits in the given range (i.e., [rangeStart, rangeEnd))
// and returns the result.
// This function may panic if the allocation failed.
func Flip64(bm *Bitmap64, rangeStart, rangeEnd uint64) *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_flip(bm.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd)))
	runtime.KeepAlive(bm)
	return b
}

// FlipClosed64 negates the bits in the given range (i.e., [rangeStart,
// rangeEnd]) and returns the result.
// This function may panic if the allocation failed.
func FlipClosed64(bm *Bitmap64, rangeStart, rangeEnd uint64) *Bitmap64 {
	b := wrap64(C.roaring64_bitmap_flip_closed(bm.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd)))
	runtime.KeepAlive(bm)
	return b
}

// AddOffset returns a copy of the bitmap with the given (possibly negative)
// offset added to every value. Values that fall outside the 64-bit range are
// dropped.
// This function may panic if the allocation failed.
func (rb *Bitmap64) AddOffset(offset int64) *Bitmap64 {
	positive := offset >= 0
	var magnitude uint64
	if positive {
		magnitude = uint64(offset)
	} else {
		// Negating math.MinInt64 overflows, so go through uint64.
		magnitude = -uint64(offset)
	}
	b := wrap64(C.roaring64_bitmap_add_offset_signed(rb.cpointer, C.bool(positive), C.uint64_t(magnitude)))
	runtime.KeepAlive(rb)
	return b
}

////////////////////////////////////////////////////////////////////////////////
// Serialization
////////////////////////////////////////////////////////////////////////////////

// SerializedSizeInBytes computes the serialized size in bytes of the Bitmap64,
// using the portable format.
func (rb *Bitmap64) SerializedSizeInBytes() int {
	answer := int(C.roaring64_bitmap_portable_size_in_bytes(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Write writes a serialized version of this bitmap to b, using the portable
// format. The buffer must be at least SerializedSizeInBytes long.
func (rb *Bitmap64) Write(b []byte) error {
	if len(b) < rb.SerializedSizeInBytes() {
		return ErrNotEnoughSpace
	}
	if len(b) == 0 {
		return ErrNotEnoughSpace
	}
	C.roaring64_bitmap_portable_serialize(rb.cpointer, (*C.char)(unsafe.Pointer(&b[0])))
	runtime.KeepAlive(b)
	runtime.KeepAlive(rb)
	return nil
}

// ToBytes returns a serialized version of this bitmap, using the portable
// format.
func (rb *Bitmap64) ToBytes() []byte {
	b := make([]byte, rb.SerializedSizeInBytes())
	if len(b) > 0 {
		C.roaring64_bitmap_portable_serialize(rb.cpointer, (*C.char)(unsafe.Pointer(&b[0])))
		runtime.KeepAlive(b)
	}
	runtime.KeepAlive(rb)
	return b
}

// Read64 reads a serialized version of the bitmap, in the portable format. The
// buffer is not retained. If the data comes from an untrusted source, prefer
// ReadValidated64.
func Read64(b []byte) (*Bitmap64, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	p := C.roaring64_bitmap_portable_deserialize_safe((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)))
	runtime.KeepAlive(b)
	if p == nil {
		return nil, ErrDeserialize
	}
	return wrap64(p), nil
}

// ReadValidated64 reads a serialized version of the bitmap, in the portable
// format, and checks its internal consistency. Use it when the data comes from
// an untrusted source. The deserialization and the validation are performed in
// a single crossing of the Go/C boundary.
func ReadValidated64(b []byte) (*Bitmap64, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	var reason *C.char
	p := C.gocroaring64_deserialize_validate((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)), &reason)
	runtime.KeepAlive(b)
	if p == nil {
		return nil, fmt.Errorf("%w: %s", ErrDeserialize, C.GoString(reason))
	}
	return wrap64(p), nil
}

// PortableDeserializeSize64 returns how many bytes would be read from b by
// Read64, or zero if b does not start with a valid bitmap.
func PortableDeserializeSize64(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	answer := int(C.roaring64_bitmap_portable_deserialize_size((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b))))
	runtime.KeepAlive(b)
	return answer
}

// FrozenSizeInBytes computes the frozen serialized size in bytes. ShrinkToFit
// must have been called since the last modification of the bitmap; otherwise
// this returns zero.
func (rb *Bitmap64) FrozenSizeInBytes() int {
	answer := int(C.roaring64_bitmap_frozen_size_in_bytes(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// WriteFrozen writes a serialized version of the bitmap to b in the frozen
// format. ShrinkToFit must have been called since the last modification of the
// bitmap. The buffer must be at least FrozenSizeInBytes long. The frozen
// format is endian-sensitive and version-specific; it is meant for fast
// reloading of data you produced yourself, not for interchange.
func (rb *Bitmap64) WriteFrozen(b []byte) error {
	size := rb.FrozenSizeInBytes()
	if size == 0 {
		return ErrNotShrunken
	}
	if len(b) < size {
		return ErrNotEnoughSpace
	}
	C.roaring64_bitmap_frozen_serialize(rb.cpointer, (*C.char)(unsafe.Pointer(&b[0])))
	runtime.KeepAlive(b)
	runtime.KeepAlive(rb)
	return nil
}

// isAligned64 reports whether the first byte of b sits on a
// Frozen64Alignment boundary.
func isAligned64(b []byte) bool {
	return uintptr(unsafe.Pointer(&b[0]))%Frozen64Alignment == 0
}

// AlignedBuffer64 returns a byte slice of the requested size whose first byte
// is aligned on a Frozen64Alignment boundary, as required by ReadFrozenView64.
func AlignedBuffer64(size int) []byte {
	if size == 0 {
		return nil
	}
	b := make([]byte, size+Frozen64Alignment)
	offset := int(uintptr(unsafe.Pointer(&b[0])) % Frozen64Alignment)
	if offset != 0 {
		offset = Frozen64Alignment - offset
	}
	return b[offset : offset+size : offset+size]
}

// ReadFrozenView64 reads a frozen serialized version of the bitmap, as written
// by Bitmap64.WriteFrozen. The result is immutable: attempting to mutate it
// will fail catastrophically. The buffer must be aligned on a
// Frozen64Alignment boundary (see AlignedBuffer64) and its length must be
// exactly the length that was written. A reference to the buffer is retained
// for the lifetime of the view.
func ReadFrozenView64(b []byte) (*Bitmap64, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	if !isAligned64(b) {
		return nil, ErrMisaligned64
	}
	p := C.roaring64_bitmap_frozen_view((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)))
	if p == nil {
		return nil, ErrDeserialize
	}
	rb := wrap64(p)
	rb.pinned = &b[0]
	return rb, nil
}

// ReadPortableFrozenView64 reads a bitmap written in the portable format (see
// Bitmap64.Write) without copying the container data: the result is a
// read-only view over b. A reference to the buffer is retained for the
// lifetime of the view. It fails on big-endian systems, where the portable
// format cannot be viewed in place.
func ReadPortableFrozenView64(b []byte) (*Bitmap64, error) {
	if len(b) == 0 {
		return nil, ErrEmptyBuffer
	}
	p := C.roaring64_bitmap_portable_deserialize_frozen((*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b)))
	if p == nil {
		return nil, ErrDeserialize
	}
	rb := wrap64(p)
	rb.pinned = &b[0]
	return rb, nil
}

////////////////////////////////////////////////////////////////////////////////
// Conversion
////////////////////////////////////////////////////////////////////////////////

// ToArray creates a new slice containing all of the integers stored in the
// Bitmap64 in sorted order.
func (rb *Bitmap64) ToArray() []uint64 {
	card := rb.Cardinality()
	array := make([]uint64, card)
	if card > 0 {
		C.roaring64_bitmap_to_uint64_array(rb.cpointer, (*C.uint64_t)(unsafe.Pointer(&array[0])))
		runtime.KeepAlive(array)
	}
	runtime.KeepAlive(rb)
	return array
}

////////////////////////////////////////////////////////////////////////////////
// Statistics
////////////////////////////////////////////////////////////////////////////////

// StatsStruct returns statistics describing the internal structure of the
// bitmap.
func (rb *Bitmap64) StatsStruct() Statistics {
	var stat C.roaring64_statistics_t
	C.roaring64_bitmap_statistics(rb.cpointer, &stat)
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

// Stats returns some statistics about the roaring bitmap.
func (rb *Bitmap64) Stats() map[string]uint64 {
	s := rb.StatsStruct()
	return map[string]uint64{
		"cardinality":  s.Cardinality,
		"n_containers": s.Containers,

		"n_array_containers":  s.ArrayContainers,
		"n_run_containers":    s.RunContainers,
		"n_bitset_containers": s.BitmapContainers,

		"n_bytes_array_containers":  s.ArrayContainerBytes,
		"n_bytes_run_containers":    s.RunContainerBytes,
		"n_bytes_bitset_containers": s.BitmapContainerBytes,

		"n_values_array_containers":  s.ArrayContainerValues,
		"n_values_run_containers":    s.RunContainerValues,
		"n_values_bitset_containers": s.BitmapContainerValues,

		"min_value": s.MinValue,
		"max_value": s.MaxValue,
	}
}
