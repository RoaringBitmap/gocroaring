// Package gocroaring is an wrapper for CRoaring in go
// It provides a fast compressed bitmap data structure.
// See http://roaringbitmap.org for details.
package gocroaring

/*
#cgo CFLAGS: -march=native -O3  -std=c99
#include "roaring.h"

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

const CRoaringMajor = C.ROARING_VERSION_MAJOR
const CRoaringMinor = C.ROARING_VERSION_MINOR
const CRoaringRevision = C.ROARING_VERSION_REVISION

func free(a *Bitmap) {
	C.roaring_bitmap_free(a.cpointer)
}

// Bitmap is the roaring bitmap
type Bitmap struct {
	cpointer *C.struct_roaring_bitmap_s
}

type frozenBitmap struct {
	Bitmap
	buffer *byte
}

// New creates a new Bitmap with any number of initial values.
// This function may panic if the allocation failed.
func New(x ...uint32) *Bitmap {
	var answer *Bitmap
	if len(x) > 0 {
		ptr := unsafe.Pointer(&x[0])
		answer = &Bitmap{C.roaring_bitmap_of_ptr(C.size_t(len(x)), (*C.uint32_t)(ptr))}
		runtime.KeepAlive(x)
	} else {
		answer = &Bitmap{C.roaring_bitmap_create()}
	}
	if answer.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(answer, free)
	return answer
}

func (rb *Bitmap) Free() {
	// Clear the finalizer to avoid double frees
	runtime.SetFinalizer(rb, nil)
	free(rb)
}

// Printf writes a description of the bitmap to stdout
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

// Add the integer(s) x to the bitmap
func (rb *Bitmap) Add(x ...uint32) {
	if len(x) == 1 {
		C.roaring_bitmap_add(rb.cpointer, C.uint32_t(x[0]))
	} else {
		ptr := unsafe.Pointer(&x[0])
		C.roaring_bitmap_add_many(rb.cpointer, C.size_t(len(x)), (*C.uint32_t)(ptr))
		runtime.KeepAlive(x)
	}
	runtime.KeepAlive(rb)
}

// AddRange - add all values in range [min, max)
func (rb *Bitmap) AddRange(min, max uint64) {
	C.roaring_bitmap_add_range(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// RemoveRange - remove all values in range [min, max)
func (rb *Bitmap) RemoveRange(min, max uint64) {
	C.roaring_bitmap_remove_range(rb.cpointer, C.uint64_t(min), C.uint64_t(max))
	runtime.KeepAlive(rb)
}

// RunOptimize the compression of the bitmap (call this after populating a new bitmap), return true if the bitmap was modified
func (rb *Bitmap) RunOptimize() bool {
	answer := bool(C.roaring_bitmap_run_optimize(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// RemoveRunCompression  Remove run-length encoding even when it is more space efficient return whether a change was applied
func (rb *Bitmap) RemoveRunCompression() bool {
	answer := bool(C.roaring_bitmap_remove_run_compression(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// FastOr computes the union between many bitmaps quickly, as opposed to having to call Or repeatedly.
// It might also be faster than calling Or repeatedly.
// This function may panic if the allocation failed.
func FastOr(bitmaps ...*Bitmap) *Bitmap {
	number := len(bitmaps)
	po := make([]*C.struct_roaring_bitmap_s, number)
	for i, v := range bitmaps {
		po[i] = v.cpointer
	}
	b := &Bitmap{C.roaring_bitmap_or_many(C.size_t(number), (**C.struct_roaring_bitmap_s)(unsafe.Pointer(&po[0])))}
	runtime.KeepAlive(bitmaps)
	if b.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(b, free)
	runtime.KeepAlive(po)
	return b
}

// Contains returns true if the integer is contained in the bitmap
func (rb *Bitmap) Contains(x uint32) bool {
	answer := bool(C.roaring_bitmap_contains(rb.cpointer, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// ContainsRange returns true if the integers in the range [x, y) are contained in the bitmap
func (rb *Bitmap) ContainsRange(x, y uint64) bool {
	answer := bool(C.roaring_bitmap_contains_range(rb.cpointer, C.uint64_t(x), C.uint64_t(y)))
	runtime.KeepAlive(rb)
	return answer
}

// Clear removes all elements from the bitmap
func (rb *Bitmap) Clear() {
	C.roaring_bitmap_clear(rb.cpointer)
	runtime.KeepAlive(rb)
}

// Remove the integer x from the bitmap
func (rb *Bitmap) Remove(x uint32) {
	C.roaring_bitmap_remove(rb.cpointer, C.uint32_t(x))
	runtime.KeepAlive(rb)
}

// Cardinality returns the number of integers contained in the bitmap
func (rb *Bitmap) Cardinality() uint64 {
	answer := uint64(C.roaring_bitmap_get_cardinality(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Cardinality returns the number of integers contained in the bitmap
func (rb *Bitmap) GetCardinality() uint64 {
	answer := uint64(C.roaring_bitmap_get_cardinality(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Maximum returns the largest of the integers contained in the bitmap assuming that it is not empty
func (rb *Bitmap) Maximum() uint32 {
	answer := uint32(C.roaring_bitmap_maximum(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Minimum returns the smallest of the integers contained in the bitmap assuming that it is not empty
func (rb *Bitmap) Minimum() uint32 {
	answer := uint32(C.roaring_bitmap_minimum(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Rank returns the number of values smaller or equal to x
func (rb *Bitmap) Rank(x uint32) uint64 {
	answer := uint64(C.roaring_bitmap_rank(rb.cpointer, C.uint32_t(x)))
	runtime.KeepAlive(rb)
	return answer
}

// Select returns the element having the designated rank, if it exists
func (rb *Bitmap) Select(rank uint32) (uint32, error) {
	var element uint32 = 0
	exists := bool(C.roaring_bitmap_select(rb.cpointer, C.uint32_t(rank), (*C.uint32_t)(unsafe.Pointer(&element))))
	runtime.KeepAlive(rb)
	if exists {
		return element, nil
	} else {
		return element, errors.New("no such element")
	}
}

// IsEmpty returns true if the Bitmap is empty (it is faster than doing (Cardinality() == 0))
func (rb *Bitmap) IsEmpty() bool {
	answer := bool(C.roaring_bitmap_is_empty(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// Equals returns true if the two bitmaps contain the same integers
func (rb *Bitmap) Equals(o interface{}) bool {
	srb, ok := o.(*Bitmap)
	if ok {
		answer := bool(C.roaring_bitmap_equals(rb.cpointer, srb.cpointer))
		runtime.KeepAlive(rb)
		runtime.KeepAlive(srb)
		return answer
	}
	return false
}

// Clone creates a copy of the Bitmap
// This function may panic if the allocation failed.
func (rb *Bitmap) Clone() *Bitmap {
	b := &Bitmap{C.roaring_bitmap_copy(rb.cpointer)}
	runtime.KeepAlive(rb)
	if b.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(b, free)
	return b
}

// Assign let rb = x2
func (rb *Bitmap) Assign(x2 *Bitmap) bool {
	answer := bool(C.roaring_bitmap_overwrite(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// And computes the intersection between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) And(x2 *Bitmap) {
	C.roaring_bitmap_and_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) Xor(x2 *Bitmap) {
	C.roaring_bitmap_xor_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Or computes the union between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) Or(x2 *Bitmap) {
	C.roaring_bitmap_or_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// AndNot computes the difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) AndNot(x2 *Bitmap) {
	C.roaring_bitmap_andnot_inplace(rb.cpointer, x2.cpointer)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
}

// Intersect checks whether the two bitmaps intersect
func (rb *Bitmap) Intersect(x2 *Bitmap) bool {
	answer := bool(C.roaring_bitmap_intersect(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// JaccardIndex computes the Jaccard index between two bitmaps
func (rb *Bitmap) JaccardIndex(x2 *Bitmap) float64 {
	answer := float64(C.roaring_bitmap_jaccard_index(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// AndCardinality computes the size of the intersection between two bitmaps
func (rb *Bitmap) AndCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_and_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// XorCardinality computes the size of the symmetric difference between two bitmaps
func (rb *Bitmap) XorCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_xor_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// OrCardinality computes the size of the union between two bitmaps
func (rb *Bitmap) OrCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_or_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// AndNotCardinality computes the size of the difference between two bitmaps
func (rb *Bitmap) AndNotCardinality(x2 *Bitmap) uint64 {
	answer := uint64(C.roaring_bitmap_andnot_cardinality(rb.cpointer, x2.cpointer))
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}

// Or computes the union between two bitmaps and returns the result
// This function may panic if the allocation failed.
func Or(x1, x2 *Bitmap) *Bitmap {
	b := &Bitmap{C.roaring_bitmap_or(x1.cpointer, x2.cpointer)}
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	if b.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(b, free)
	return b
}

// And computes the intersection between two bitmaps and returns the result
// This function may panic if the allocation failed.
func And(x1, x2 *Bitmap) *Bitmap {
	b := &Bitmap{C.roaring_bitmap_and(x1.cpointer, x2.cpointer)}
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	if b.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(b, free)
	return b
}

// Xor computes the symmetric difference between two bitmaps and returns the result
// This function may panic if the allocation failed.
func Xor(x1, x2 *Bitmap) *Bitmap {
	b := &Bitmap{C.roaring_bitmap_xor(x1.cpointer, x2.cpointer)}
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	if b.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(b, free)
	return b
}

// AndNot computes the difference between two bitmaps and returns the result
// This function may panic if the allocation failed.
func AndNot(x1, x2 *Bitmap) *Bitmap {
	b := &Bitmap{C.roaring_bitmap_andnot(x1.cpointer, x2.cpointer)}
	runtime.KeepAlive(x1)
	runtime.KeepAlive(x2)
	if b.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(b, free)
	return b
}

// Flip negates the bits in the given range (i.e., [rangeStart,rangeEnd)), any integer present in this range and in the bitmap is removed.
func (rb *Bitmap) Flip(rangeStart, rangeEnd uint64) {
	C.roaring_bitmap_flip_inplace(rb.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd))
	runtime.KeepAlive(rb)
}

// Flip negates the bits in the given range  (i.e., [rangeStart,rangeEnd)), any integer present in this range and in the bitmap is removed.
// This function may panic if the allocation failed.
func Flip(bm *Bitmap, rangeStart, rangeEnd uint64) *Bitmap {
	b := &Bitmap{C.roaring_bitmap_flip(bm.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd))}
	if b.cpointer == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(b, free)
	runtime.KeepAlive(bm)
	return b
}

// SerializedSizeInBytes computes the serialized size in bytes  the Bitmap.
func (rb *Bitmap) SerializedSizeInBytes() int {
	answer := int(C.roaring_bitmap_portable_size_in_bytes(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer
}

// FrozenSizeInBytes computes the frozen serialized size in bytes
func (rb *Bitmap) FrozenSizeInBytes() int {
	answer := int(C.roaring_bitmap_frozen_size_in_bytes(rb.cpointer))
	runtime.KeepAlive(rb)
	return answer

}

// IntIterable allows you to iterate over the values in a Bitmap
type IntIterable interface {
	HasNext() bool
	Next() uint32
}

type intIterator struct {
	pointertonext *C.roaring_uint32_iterator_t
	current       uint32
	has_next      bool
}

// Iterator creates a new IntIterable to iterate over the integers contained in the bitmap, in sorted order
func (rb *Bitmap) Iterator() IntIterable {
	return newIntIterator(rb)
}

// HasNext returns true if there are more integers to iterate over
func (ii *intIterator) HasNext() bool {
	return ii.has_next
}

// Next returns the next integer
func (ii *intIterator) Next() uint32 {
	answer := ii.current
	ii.has_next = bool(ii.pointertonext.has_value)
	ii.current = uint32(ii.pointertonext.current_value)
	C.roaring_advance_uint32_iterator(ii.pointertonext)
	runtime.KeepAlive(ii)
	return answer
}

func freeIntIterator(a *intIterator) {
	C.roaring_free_uint32_iterator(a.pointertonext)
	runtime.KeepAlive(a)
}

// This function may panic if the allocation failed.
func newIntIterator(a *Bitmap) *intIterator {
	p := new(intIterator)
	p.pointertonext = C.roaring_create_iterator(a.cpointer)
	p.has_next = bool(p.pointertonext.has_value)
	p.current = uint32(p.pointertonext.current_value)
	if p.has_next {
		C.roaring_advance_uint32_iterator(p.pointertonext)
	}
	runtime.KeepAlive(a)
	if p.pointertonext == nil {
		panic("C code returned a null pointer.")
	}
	runtime.SetFinalizer(p, freeIntIterator)
	return p
}

// Write writes a serialized version of this bitmap to stream (you should have enough space)
func (rb *Bitmap) Write(b []byte) error {
	if len(b) < rb.SerializedSizeInBytes() {
		return errors.New("not enough space")
	}
	bchar := (*C.char)(unsafe.Pointer(&b[0]))
	C.roaring_bitmap_portable_serialize(rb.cpointer, bchar)
	runtime.KeepAlive(b)
	runtime.KeepAlive(rb)
	return nil
}

// WriteFrozen writes a serialized version of bitmap to the stream in the Frozen format
func (rb *Bitmap) WriteFrozen(b []byte) error {
	if len(b) < rb.FrozenSizeInBytes() {
		return errors.New("not enough space")
	}
	bchar := (*C.char)(unsafe.Pointer(&b[0]))
	C.roaring_bitmap_frozen_serialize(rb.cpointer, bchar)
	runtime.KeepAlive(b)
	runtime.KeepAlive(rb)
	return nil
}

// ToArray creates a new slice containing all of the integers stored in the Bitmap in sorted order
func (rb *Bitmap) ToArray() []uint32 {
	card := rb.Cardinality()
	array := make([]uint32, card)
	if card > 0 {
		C.roaring_bitmap_to_uint32_array(rb.cpointer, (*C.uint32_t)(unsafe.Pointer(&array[0])))
	}
	runtime.KeepAlive(rb)
	return array
}

// String creates a string representation of the Bitmap
func (rb *Bitmap) String() string {
	arr := rb.ToArray() // todo: replace with an iterator
	var buffer bytes.Buffer
	start := []byte("{")
	buffer.Write(start)
	l := len(arr)
	for counter, i := range arr {
		// to avoid exhausting the memory
		if counter > 0x40000 {
			buffer.WriteString("...")
			break
		}
		buffer.WriteString(strconv.FormatInt(int64(i), 10))
		if counter+1 < l { // there is more
			buffer.WriteString(",")
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}

// Read reads a serialized version of the bitmap (you need to call Free on it once you are done)
func Read(b []byte) (*Bitmap, error) {
	bchar := (*C.char)(unsafe.Pointer(&b[0]))
	answer := &Bitmap{C.roaring_bitmap_portable_deserialize_safe(bchar, C.size_t(len(b)))}
	runtime.KeepAlive(b)
	if answer.cpointer == nil {
		return nil, errors.New("failed to read roaring array")
	}
	runtime.SetFinalizer(answer, free)
	return answer, nil
}

// ReadFrozenView reads a frozen serialized version of the bitmap
// this is immutable and attempting to mutate it will fail catastrophically
// It keeps a reference to the buffer internally to make sure it's alive for
// the complete lifetime of the view
//
func ReadFrozenView(b []byte) (*Bitmap, error) {
	bchar := (*C.char)(unsafe.Pointer(&b[0]))
	answer := &frozenBitmap{
		Bitmap{C.roaring_bitmap_frozen_view(bchar, C.size_t(len(b)))},
		&b[0],
	}
	if answer.cpointer == nil {
		return nil, errors.New("failed to read roaring array")
	}
	runtime.SetFinalizer(&answer.Bitmap, free)
	return &answer.Bitmap, nil
}

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
	}
}

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
}

// StatsStruct - same as Stats but returns typed struct. See https://github.com/RoaringBitmap/roaring/pull/73 for rationale
func (rb *Bitmap) StatsStruct() Statistics {
	var stat C.roaring_statistics_t
	C.roaring_bitmap_statistics(rb.cpointer, &stat)
	stats := Statistics{
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
	}

	return stats
}
