// Package gocroaring is an wrapper for CRoaring in go
// It provides a fast compressed bitmap data structure.
// See http://roaringbitmap.org for details.

package gocroaring

/*
#cgo CFLAGS: -march=native -O3  -Wa,-q
#include "roaring.h"
*/
import "C"
import "os"
import "bytes"
import "io"
import "runtime"
import "unsafe"
import "errors"

func free(a *Bitmap) {
	C.roaring_bitmap_free(a.cpointer)
}

type Bitmap struct {
	cpointer *C.struct_roaring_bitmap_s
}

// NewBitmap creates a new empty Bitmap
func NewBitmap() *Bitmap {
	answer := &Bitmap{C.roaring_bitmap_create()}
	runtime.SetFinalizer(answer, free)
	return answer
}

// Print a description of the bitmap to stdout
func (rb *Bitmap) Printf() {
	C.roaring_bitmap_printf(rb.cpointer)
	C.fflush(C.stdout)
}

// Add the integer x to the bitmap
func (rb *Bitmap) Add(x uint32) {
	C.roaring_bitmap_add(rb.cpointer, C.uint32_t(x))

}

// RunOptimize the compression of the bitmap (call this after populating a new bitmap), return true if the bitmap was modified
func (rb *Bitmap) RunOptimize() bool {
	return bool(C.roaring_bitmap_run_optimize(rb.cpointer))
}

// RemoveRunCompression  Remove run-length encoding even when it is more space efficient return whether a change was applied
func (rb *Bitmap) RemoveRunCompression() bool {
	return bool(C.roaring_bitmap_remove_run_compression(rb.cpointer))
}

// FastOr computes the union between many bitmaps quickly, as opposed to having to call Or repeatedly.
// It might also be faster than calling Or repeatedly.
func FastOr(bitmaps ...*Bitmap) *Bitmap {
	number := len(bitmaps)
	po := make([]*C.struct_roaring_bitmap_s, number)
	for i, v := range bitmaps {
		po[i] = v.cpointer
	}
	return &Bitmap{C.roaring_bitmap_or_many(C.size_t(number), (**C.struct_roaring_bitmap_s)(unsafe.Pointer(&po[0])))}
}

// Contains returns true if the integer is contained in the bitmap
func (rb *Bitmap) Contains(x uint32) bool {
	return bool(C.roaring_bitmap_contains(rb.cpointer, C.uint32_t(x)))
}

// Remove the integer x from the bitmap
func (rb *Bitmap) Remove(x uint32) {
	C.roaring_bitmap_remove(rb.cpointer, C.uint32_t(x))
}

// GetCardinality returns the number of integers contained in the bitmap
func (rb *Bitmap) GetCardinality() uint64 {
	return uint64(C.roaring_bitmap_get_cardinality(rb.cpointer))
}

// IsEmpty returns true if the Bitmap is empty (it is faster than doing (GetCardinality() == 0))
func (rb *Bitmap) IsEmpty() bool {
	return bool(C.roaring_bitmap_is_empty(rb.cpointer))
}

// Equals returns true if the two bitmaps contain the same integers
func (rb *Bitmap) Equals(o interface{}) bool {
	srb, ok := o.(*Bitmap)
	if ok {
		return bool(C.roaring_bitmap_equals(rb.cpointer, srb.cpointer))
	}
	return false
}

// Clone creates a copy of the Bitmap
func (rb *Bitmap) Clone() *Bitmap {
	return &Bitmap{C.roaring_bitmap_copy(rb.cpointer)}
}

// And computes the intersection between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) And(x2 *Bitmap) {
	C.roaring_bitmap_and_inplace(rb.cpointer, x2.cpointer)
}

// Xor computes the symmetric difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) Xor(x2 *Bitmap) {
	C.roaring_bitmap_xor_inplace(rb.cpointer, x2.cpointer)
}

// Or computes the union between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) Or(x2 *Bitmap) {
	C.roaring_bitmap_or_inplace(rb.cpointer, x2.cpointer)
}

// AndNot computes the difference between two bitmaps and stores the result in the current bitmap
func (rb *Bitmap) AndNot(x2 *Bitmap) {
	C.roaring_bitmap_andnot_inplace(rb.cpointer, x2.cpointer)
}

// Or computes the union between two bitmaps and returns the result
func Or(x1, x2 *Bitmap) *Bitmap {
	return &Bitmap{C.roaring_bitmap_or(x1.cpointer, x2.cpointer)}
}

// And computes the intersection between two bitmaps and returns the result
func And(x1, x2 *Bitmap) *Bitmap {
	return &Bitmap{C.roaring_bitmap_and(x1.cpointer, x2.cpointer)}
}

// Xor computes the symmetric difference between two bitmaps and returns the result
func Xor(x1, x2 *Bitmap) *Bitmap {
	return &Bitmap{C.roaring_bitmap_xor(x1.cpointer, x2.cpointer)}
}

// AndNot computes the difference between two bitmaps and returns the result
func AndNot(x1, x2 *Bitmap) *Bitmap {
	return &Bitmap{C.roaring_bitmap_andnot(x1.cpointer, x2.cpointer)}
}

// Flip negates the bits in the given range (i.e., [rangeStart,rangeEnd)), any integer present in this range and in the bitmap is removed,
func (rb *Bitmap) Flip(rangeStart, rangeEnd uint64) {
	C.roaring_bitmap_flip_inplace(rb.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd))
}

// Flip negates the bits in the given range  (i.e., [rangeStart,rangeEnd)), any integer present in this range and in the bitmap is removed,
func Flip(bm *Bitmap, rangeStart, rangeEnd uint64) *Bitmap {
	return &Bitmap{C.roaring_bitmap_flip(bm.cpointer, C.uint64_t(rangeStart), C.uint64_t(rangeEnd))}
}

// GetSerializedSizeInBytes computes the serialized size in bytes  the Bitmap.
func (rb *Bitmap) GetSerializedSizeInBytes() int {
	return int(C.roaring_bitmap_portable_size_in_bytes(rb.cpointer))
}

// Write writes a serialized version of this bitmap to stream (you should have enough space)
func (rb *Bitmap) Write(b []byte) error {
	if len(b) < rb.GetSerializedSizeInBytes() {
		return errors.New("not enough space")
	}
	bchar := (*C.char)(unsafe.Pointer(&b[0]))
	C.roaring_bitmap_portable_serialize(rb.cpointer, bchar)
	return nil
}

// ToArray creates a new slice containing all of the integers stored in the Bitmap in sorted order
func (rb *Bitmap) ToArray() []uint32 {
	array := make([]uint32, rb.GetCardinality())
	C.roaring_bitmap_to_uint32_array(rb.cpointer, (*C.uint32_t)(unsafe.Pointer(&array[0])))
	return array
}

// String creates a string representation of the Bitmap
func (rb *Bitmap) String() string {
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	C.roaring_bitmap_printf(rb.cpointer)
	C.fflush(C.stdout)
	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC
	return out
}

// Read reads a serialized version of the bitmap (you need to call Free on it once you are done)
func Read(b []byte) (*Bitmap, error) {
	bchar := (*C.char)(unsafe.Pointer(&b[0]))
	answer := &Bitmap{C.roaring_bitmap_portable_deserialize(bchar)}
	if answer.cpointer == nil {
		return nil, errors.New("failed to read roaring array")
	}
	runtime.SetFinalizer(answer, free)
	return answer, nil
}
