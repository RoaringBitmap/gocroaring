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
#cgo noescape roaring_iterator_create
#cgo noescape roaring_iterator_init
#cgo noescape roaring_iterator_init_last
#cgo noescape roaring_uint32_iterator_advance
#cgo noescape roaring_uint32_iterator_copy
#cgo noescape roaring_uint32_iterator_free
#cgo noescape roaring_uint32_iterator_move_equalorlarger
#cgo noescape roaring_uint32_iterator_previous
#cgo noescape roaring_uint32_iterator_read
#cgo noescape roaring_uint32_iterator_read_backward
#cgo noescape roaring_uint32_iterator_read_prev_ranges
#cgo noescape roaring_uint32_iterator_read_ranges
#cgo noescape roaring_uint32_iterator_skip
#cgo noescape roaring_uint32_iterator_skip_backward

#cgo nocallback roaring_iterator_create
#cgo nocallback roaring_iterator_init
#cgo nocallback roaring_iterator_init_last
#cgo nocallback roaring_uint32_iterator_advance
#cgo nocallback roaring_uint32_iterator_copy
#cgo nocallback roaring_uint32_iterator_free
#cgo nocallback roaring_uint32_iterator_move_equalorlarger
#cgo nocallback roaring_uint32_iterator_previous
#cgo nocallback roaring_uint32_iterator_read
#cgo nocallback roaring_uint32_iterator_read_backward
#cgo nocallback roaring_uint32_iterator_read_prev_ranges
#cgo nocallback roaring_uint32_iterator_read_ranges
#cgo nocallback roaring_uint32_iterator_skip
#cgo nocallback roaring_uint32_iterator_skip_backward
#include "roaring.h"
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// iterBufferSize is how many values a buffered iterator pulls across the
// Go/C boundary at a time. Crossing that boundary is what makes iteration
// expensive, so we amortize it over a whole block of values.
const iterBufferSize = 512

// IntIterable allows you to iterate over the values in a Bitmap.
type IntIterable interface {
	HasNext() bool
	Next() uint32
}

// intIterator is a forward-only iterator that reads values in blocks.
type intIterator struct {
	it      *C.roaring_uint32_iterator_t
	cleanup runtime.Cleanup
	parent  *Bitmap
	buf     [iterBufferSize]uint32
	pos     int
	n       int
}

// Iterator creates a new IntIterable to iterate over the integers contained in
// the bitmap, in sorted order.
// This function may panic if the allocation failed.
func (rb *Bitmap) Iterator() IntIterable {
	return newIntIterator(rb)
}

func newIntIterator(rb *Bitmap) *intIterator {
	p := C.roaring_iterator_create(rb.cpointer)
	runtime.KeepAlive(rb)
	if p == nil {
		panic("C code returned a null pointer.")
	}
	ii := &intIterator{it: p, parent: rb}
	ii.cleanup = runtime.AddCleanup(ii, func(p *C.roaring_uint32_iterator_t) {
		C.roaring_uint32_iterator_free(p)
	}, p)
	ii.fill()
	return ii
}

func (ii *intIterator) fill() {
	ii.n = int(C.roaring_uint32_iterator_read(ii.it,
		(*C.uint32_t)(unsafe.Pointer(&ii.buf[0])), C.uint32_t(len(ii.buf))))
	ii.pos = 0
	runtime.KeepAlive(ii)
}

// HasNext returns true if there are more integers to iterate over.
func (ii *intIterator) HasNext() bool {
	return ii.pos < ii.n
}

// Next returns the next integer. It must not be called when HasNext is false.
func (ii *intIterator) Next() uint32 {
	answer := ii.buf[ii.pos]
	ii.pos++
	if ii.pos == ii.n {
		ii.fill()
	}
	return answer
}

// Iterate calls cb with every integer in the bitmap, in sorted order, stopping
// early if cb returns false.
func (rb *Bitmap) Iterate(cb func(x uint32) bool) {
	it := newIntIterator(rb)
	for it.HasNext() {
		if !cb(it.Next()) {
			return
		}
	}
}

// Iterator is a full-featured iterator over the values of a Bitmap. Unlike the
// iterator returned by Bitmap.Iterator, it can move backwards and can seek.
//
// An Iterator points at a value or is exhausted; use HasValue to tell the two
// apart. It is invalidated by any modification of the underlying bitmap.
type Iterator struct {
	it      *C.roaring_uint32_iterator_t
	cleanup runtime.Cleanup
	parent  *Bitmap
}

func newIterator(rb *Bitmap, p *C.roaring_uint32_iterator_t) *Iterator {
	if p == nil {
		panic("C code returned a null pointer.")
	}
	i := &Iterator{it: p, parent: rb}
	i.cleanup = runtime.AddCleanup(i, func(p *C.roaring_uint32_iterator_t) {
		C.roaring_uint32_iterator_free(p)
	}, p)
	return i
}

// NewIterator returns an iterator positioned on the smallest value of the
// bitmap.
// This function may panic if the allocation failed.
func (rb *Bitmap) NewIterator() *Iterator {
	p := C.roaring_iterator_create(rb.cpointer)
	runtime.KeepAlive(rb)
	return newIterator(rb, p)
}

// ReverseIterator returns an iterator positioned on the largest value of the
// bitmap, meant to be walked with Previous.
// This function may panic if the allocation failed.
func (rb *Bitmap) ReverseIterator() *Iterator {
	p := C.roaring_iterator_create(rb.cpointer)
	runtime.KeepAlive(rb)
	i := newIterator(rb, p)
	C.roaring_iterator_init_last(rb.cpointer, p)
	runtime.KeepAlive(rb)
	runtime.KeepAlive(i)
	return i
}

// Free releases the memory held by the iterator. Using the iterator afterwards
// is a mistake. Calling Free more than once is harmless.
func (i *Iterator) Free() {
	if i.it == nil {
		return
	}
	i.cleanup.Stop()
	C.roaring_uint32_iterator_free(i.it)
	i.it = nil
	i.parent = nil
}

// HasValue reports whether the iterator points at a value.
func (i *Iterator) HasValue() bool {
	answer := bool(i.it.has_value)
	runtime.KeepAlive(i)
	return answer
}

// Value returns the value the iterator points at. It is meaningless when
// HasValue is false.
func (i *Iterator) Value() uint32 {
	answer := uint32(i.it.current_value)
	runtime.KeepAlive(i)
	return answer
}

// Next moves the iterator to the next value and reports whether it points at
// one.
func (i *Iterator) Next() bool {
	answer := bool(C.roaring_uint32_iterator_advance(i.it))
	runtime.KeepAlive(i)
	return answer
}

// Previous moves the iterator to the previous value and reports whether it
// points at one.
func (i *Iterator) Previous() bool {
	answer := bool(C.roaring_uint32_iterator_previous(i.it))
	runtime.KeepAlive(i)
	return answer
}

// AdvanceIfNeeded moves the iterator to the smallest value that is greater
// than or equal to x, and reports whether it points at a value. The iterator
// does not move if it already points at such a value.
func (i *Iterator) AdvanceIfNeeded(x uint32) bool {
	answer := bool(C.roaring_uint32_iterator_move_equalorlarger(i.it, C.uint32_t(x)))
	runtime.KeepAlive(i)
	return answer
}

// Skip advances the iterator by count values and returns how many values were
// actually skipped.
func (i *Iterator) Skip(count uint32) uint32 {
	answer := uint32(C.roaring_uint32_iterator_skip(i.it, C.uint32_t(count)))
	runtime.KeepAlive(i)
	return answer
}

// SkipBackward moves the iterator back by count values and returns how many
// values were actually skipped.
func (i *Iterator) SkipBackward(count uint32) uint32 {
	answer := uint32(C.roaring_uint32_iterator_skip_backward(i.it, C.uint32_t(count)))
	runtime.KeepAlive(i)
	return answer
}

// Read fills buf with up to len(buf) values, in ascending order, and returns
// how many were written. A return value smaller than len(buf) means the
// iterator is exhausted.
func (i *Iterator) Read(buf []uint32) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring_uint32_iterator_read(i.it,
		(*C.uint32_t)(unsafe.Pointer(&buf[0])), C.uint32_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// ReadBackward fills buf with up to len(buf) values, in descending order, and
// returns how many were written.
func (i *Iterator) ReadBackward(buf []uint32) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring_uint32_iterator_read_backward(i.it,
		(*C.uint32_t)(unsafe.Pointer(&buf[0])), C.uint32_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// Range is a closed interval [Min, Max] of 32-bit values.
type Range struct {
	Min uint32
	Max uint32
}

// ReadRanges fills buf with up to len(buf) runs of consecutive values, in
// ascending order, and returns how many were written.
func (i *Iterator) ReadRanges(buf []Range) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring_uint32_iterator_read_ranges(i.it,
		(*C.roaring_uint32_range_closed_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// ReadPreviousRanges fills buf with up to len(buf) runs of consecutive values,
// in descending order, and returns how many were written.
func (i *Iterator) ReadPreviousRanges(buf []Range) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring_uint32_iterator_read_prev_ranges(i.it,
		(*C.roaring_uint32_range_closed_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// Clone returns a copy of the iterator, positioned on the same value.
// This function may panic if the allocation failed.
func (i *Iterator) Clone() *Iterator {
	p := C.roaring_uint32_iterator_copy(i.it)
	runtime.KeepAlive(i)
	return newIterator(i.parent, p)
}

// Reset repositions the iterator on the smallest value of the bitmap it was
// created from.
func (i *Iterator) Reset() {
	C.roaring_iterator_init(i.parent.cpointer, i.it)
	runtime.KeepAlive(i)
}

// ResetToLast repositions the iterator on the largest value of the bitmap it
// was created from.
func (i *Iterator) ResetToLast() {
	C.roaring_iterator_init_last(i.parent.cpointer, i.it)
	runtime.KeepAlive(i)
}
