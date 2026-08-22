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
#cgo noescape roaring64_iterator_advance
#cgo noescape roaring64_iterator_copy
#cgo noescape roaring64_iterator_create
#cgo noescape roaring64_iterator_create_last
#cgo noescape roaring64_iterator_free
#cgo noescape roaring64_iterator_has_value
#cgo noescape roaring64_iterator_move_equalorlarger
#cgo noescape roaring64_iterator_previous
#cgo noescape roaring64_iterator_read
#cgo noescape roaring64_iterator_read_backward
#cgo noescape roaring64_iterator_read_prev_ranges
#cgo noescape roaring64_iterator_read_ranges
#cgo noescape roaring64_iterator_reinit
#cgo noescape roaring64_iterator_reinit_last
#cgo noescape roaring64_iterator_value

#cgo nocallback roaring64_iterator_advance
#cgo nocallback roaring64_iterator_copy
#cgo nocallback roaring64_iterator_create
#cgo nocallback roaring64_iterator_create_last
#cgo nocallback roaring64_iterator_free
#cgo nocallback roaring64_iterator_has_value
#cgo nocallback roaring64_iterator_move_equalorlarger
#cgo nocallback roaring64_iterator_previous
#cgo nocallback roaring64_iterator_read
#cgo nocallback roaring64_iterator_read_backward
#cgo nocallback roaring64_iterator_read_prev_ranges
#cgo nocallback roaring64_iterator_read_ranges
#cgo nocallback roaring64_iterator_reinit
#cgo nocallback roaring64_iterator_reinit_last
#cgo nocallback roaring64_iterator_value
#include "roaring.h"
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// IntIterable64 allows you to iterate over the values in a Bitmap64.
type IntIterable64 interface {
	HasNext() bool
	Next() uint64
}

// intIterator64 is a forward-only iterator that reads values in blocks, so
// that the cost of crossing the Go/C boundary is amortized over a whole block.
type intIterator64 struct {
	it      *C.roaring64_iterator_t
	cleanup runtime.Cleanup
	parent  *Bitmap64
	buf     [iterBufferSize]uint64
	pos     int
	n       int
}

// Iterator creates a new IntIterable64 to iterate over the integers contained
// in the bitmap, in sorted order.
// This function may panic if the allocation failed.
func (rb *Bitmap64) Iterator() IntIterable64 {
	return newIntIterator64(rb)
}

func newIntIterator64(rb *Bitmap64) *intIterator64 {
	p := C.roaring64_iterator_create(rb.cpointer)
	runtime.KeepAlive(rb)
	if p == nil {
		panic("C code returned a null pointer.")
	}
	ii := &intIterator64{it: p, parent: rb}
	ii.cleanup = runtime.AddCleanup(ii, func(p *C.roaring64_iterator_t) {
		C.roaring64_iterator_free(p)
	}, p)
	ii.fill()
	return ii
}

func (ii *intIterator64) fill() {
	ii.n = int(C.roaring64_iterator_read(ii.it,
		(*C.uint64_t)(unsafe.Pointer(&ii.buf[0])), C.uint64_t(len(ii.buf))))
	ii.pos = 0
	runtime.KeepAlive(ii)
}

// HasNext returns true if there are more integers to iterate over.
func (ii *intIterator64) HasNext() bool {
	return ii.pos < ii.n
}

// Next returns the next integer. It must not be called when HasNext is false.
func (ii *intIterator64) Next() uint64 {
	answer := ii.buf[ii.pos]
	ii.pos++
	if ii.pos == ii.n {
		ii.fill()
	}
	return answer
}

// Iterate calls cb with every integer in the bitmap, in sorted order, stopping
// early if cb returns false.
func (rb *Bitmap64) Iterate(cb func(x uint64) bool) {
	it := newIntIterator64(rb)
	for it.HasNext() {
		if !cb(it.Next()) {
			return
		}
	}
}

// Iterator64 is a full-featured iterator over the values of a Bitmap64. Unlike
// the iterator returned by Bitmap64.Iterator, it can move backwards and can
// seek.
//
// An Iterator64 points at a value or is exhausted; use HasValue to tell the
// two apart. It is invalidated by any modification of the underlying bitmap.
type Iterator64 struct {
	it      *C.roaring64_iterator_t
	cleanup runtime.Cleanup
	parent  *Bitmap64
}

func newIterator64(rb *Bitmap64, p *C.roaring64_iterator_t) *Iterator64 {
	if p == nil {
		panic("C code returned a null pointer.")
	}
	i := &Iterator64{it: p, parent: rb}
	i.cleanup = runtime.AddCleanup(i, func(p *C.roaring64_iterator_t) {
		C.roaring64_iterator_free(p)
	}, p)
	return i
}

// NewIterator returns an iterator positioned on the smallest value of the
// bitmap.
// This function may panic if the allocation failed.
func (rb *Bitmap64) NewIterator() *Iterator64 {
	p := C.roaring64_iterator_create(rb.cpointer)
	runtime.KeepAlive(rb)
	return newIterator64(rb, p)
}

// ReverseIterator returns an iterator positioned on the largest value of the
// bitmap, meant to be walked with Previous.
// This function may panic if the allocation failed.
func (rb *Bitmap64) ReverseIterator() *Iterator64 {
	p := C.roaring64_iterator_create_last(rb.cpointer)
	runtime.KeepAlive(rb)
	return newIterator64(rb, p)
}

// Free releases the memory held by the iterator. Using the iterator afterwards
// is a mistake. Calling Free more than once is harmless.
func (i *Iterator64) Free() {
	if i.it == nil {
		return
	}
	i.cleanup.Stop()
	C.roaring64_iterator_free(i.it)
	i.it = nil
	i.parent = nil
}

// HasValue reports whether the iterator points at a value.
func (i *Iterator64) HasValue() bool {
	answer := bool(C.roaring64_iterator_has_value(i.it))
	runtime.KeepAlive(i)
	return answer
}

// Value returns the value the iterator points at. It is meaningless when
// HasValue is false.
func (i *Iterator64) Value() uint64 {
	answer := uint64(C.roaring64_iterator_value(i.it))
	runtime.KeepAlive(i)
	return answer
}

// Next moves the iterator to the next value and reports whether it points at
// one.
func (i *Iterator64) Next() bool {
	answer := bool(C.roaring64_iterator_advance(i.it))
	runtime.KeepAlive(i)
	return answer
}

// Previous moves the iterator to the previous value and reports whether it
// points at one.
func (i *Iterator64) Previous() bool {
	answer := bool(C.roaring64_iterator_previous(i.it))
	runtime.KeepAlive(i)
	return answer
}

// AdvanceIfNeeded moves the iterator to the smallest value that is greater
// than or equal to x, and reports whether it points at a value. The iterator
// does not move if it already points at such a value.
func (i *Iterator64) AdvanceIfNeeded(x uint64) bool {
	answer := bool(C.roaring64_iterator_move_equalorlarger(i.it, C.uint64_t(x)))
	runtime.KeepAlive(i)
	return answer
}

// Read fills buf with up to len(buf) values, in ascending order, and returns
// how many were written. A return value smaller than len(buf) means the
// iterator is exhausted.
func (i *Iterator64) Read(buf []uint64) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring64_iterator_read(i.it,
		(*C.uint64_t)(unsafe.Pointer(&buf[0])), C.uint64_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// ReadBackward fills buf with up to len(buf) values, in descending order, and
// returns how many were written.
func (i *Iterator64) ReadBackward(buf []uint64) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring64_iterator_read_backward(i.it,
		(*C.uint64_t)(unsafe.Pointer(&buf[0])), C.uint64_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// Range64 is a closed interval [Min, Max] of 64-bit values.
type Range64 struct {
	Min uint64
	Max uint64
}

// ReadRanges fills buf with up to len(buf) runs of consecutive values, in
// ascending order, and returns how many were written.
func (i *Iterator64) ReadRanges(buf []Range64) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring64_iterator_read_ranges(i.it,
		(*C.roaring64_range_closed_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// ReadPreviousRanges fills buf with up to len(buf) runs of consecutive values,
// in descending order, and returns how many were written.
func (i *Iterator64) ReadPreviousRanges(buf []Range64) int {
	if len(buf) == 0 {
		return 0
	}
	n := int(C.roaring64_iterator_read_prev_ranges(i.it,
		(*C.roaring64_range_closed_t)(unsafe.Pointer(&buf[0])), C.size_t(len(buf))))
	runtime.KeepAlive(buf)
	runtime.KeepAlive(i)
	return n
}

// Clone returns a copy of the iterator, positioned on the same value.
// This function may panic if the allocation failed.
func (i *Iterator64) Clone() *Iterator64 {
	p := C.roaring64_iterator_copy(i.it)
	runtime.KeepAlive(i)
	return newIterator64(i.parent, p)
}

// Reset repositions the iterator on the smallest value of the bitmap it was
// created from.
func (i *Iterator64) Reset() {
	C.roaring64_iterator_reinit(i.parent.cpointer, i.it)
	runtime.KeepAlive(i)
}

// ResetToLast repositions the iterator on the largest value of the bitmap it
// was created from.
func (i *Iterator64) ResetToLast() {
	C.roaring64_iterator_reinit_last(i.parent.cpointer, i.it)
	runtime.KeepAlive(i)
}
