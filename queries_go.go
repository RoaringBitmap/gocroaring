//go:build (amd64 || arm64) && !gocroaring_cqueries

package gocroaring

/*
#include "roaring.h"
*/
import "C"

import (
	"math"
	"math/bits"
	"runtime"
	"unsafe"
)

// The read-only queries below (Contains, Rank, Minimum, Maximum, Select and
// Intersect) read the CRoaring data structures directly from Go instead of
// calling into C. They are short, so the cost of a cgo call (about 40 ns)
// would otherwise dominate them. The struct layouts come from roaring.h
// through cgo, so they always match the bundled CRoaring: a renamed field
// breaks the build rather than the results.
//
// Portable frozen views point into the caller's buffer, so their arrays need
// not be aligned. Unaligned loads are fine on amd64 and arm64; elsewhere,
// queries_c.go routes these queries through CRoaring.
//
// A container type this code does not know makes the query fall back to C.

const (
	goBitsetType = C.BITSET_CONTAINER_TYPE
	goArrayType  = C.ARRAY_CONTAINER_TYPE
	goRunType    = C.RUN_CONTAINER_TYPE
	goSharedType = C.SHARED_CONTAINER_TYPE

	bitsetWords = C.BITSET_CONTAINER_SIZE_IN_WORDS
)

func u16At(p unsafe.Pointer, i int) uint16 {
	return *(*uint16)(unsafe.Add(p, uintptr(i)*2))
}

func u64At(p unsafe.Pointer, i int) uint64 {
	return *(*uint64)(unsafe.Add(p, uintptr(i)*8))
}

// A run is a pair of uint16 values: its start and its length minus one.
func runAt(p unsafe.Pointer, i int) (start, length uint32) {
	r := unsafe.Add(p, uintptr(i)*4)
	return uint32(*(*uint16)(r)), uint32(*(*uint16)(unsafe.Add(r, 2)))
}

// lowerBound16 returns the index of the last element at or below "target" in
// the "n" sorted uint16 values found every "stride" bytes from "p", or -1 if
// they are all larger. The loop has no data-dependent branch, so that random
// lookups do not pay a misprediction per step: d >> 63 is all ones when the
// probed value exceeds the target, and zero otherwise. (The Go compiler does
// not turn the equivalent if statement into a conditional move.)
func lowerBound16(p unsafe.Pointer, n int, stride uintptr, target uint16) int {
	if n == 0 || *(*uint16)(p) > target {
		return -1
	}
	base := 0
	for n > 1 {
		half := n >> 1
		d := int(target) - int(*(*uint16)(unsafe.Add(p, uintptr(base+half)*stride)))
		base += half &^ (d >> 63)
		n -= half
	}
	return base
}

// gallop16 returns the smallest index i >= pos such that the i-th of the "n"
// sorted uint16 values found every "stride" bytes from "p" is at least "min",
// or n if there is none. It probes pos, pos+1, pos+3, pos+7, ... and then
// searches the last interval, so it is quick when the answer is near pos.
func gallop16(p unsafe.Pointer, stride uintptr, n, pos int, min uint16) int {
	at := func(i int) uint16 { return *(*uint16)(unsafe.Add(p, uintptr(i)*stride)) }
	if pos >= n || at(pos) >= min {
		return pos
	}
	// at(lo) < min; hi == n or at(hi) >= min.
	lo, step := pos, 1
	hi := lo + step
	for hi < n && at(hi) < min {
		lo = hi
		step <<= 1
		hi = lo + step
	}
	if hi > n {
		hi = n
	}
	// The answer is past the last value of [lo, hi) below min. Since
	// at(lo) < min, min > 0 and lowerBound16 returns at least 0.
	return lo + 1 + lowerBound16(unsafe.Add(p, uintptr(lo)*stride), hi-lo, stride, min-1)
}

// raView gives access to the top-level arrays of a bitmap.
type raView struct {
	keys       unsafe.Pointer
	containers unsafe.Pointer
	typecodes  unsafe.Pointer
	n          int
}

func viewOf(rb *Bitmap) raView {
	ra := &rb.cpointer.high_low_container
	return raView{
		keys:       unsafe.Pointer(ra.keys),
		containers: unsafe.Pointer(ra.containers),
		typecodes:  unsafe.Pointer(ra.typecodes),
		n:          int(ra.size),
	}
}

func (v *raView) key(i int) uint16 { return u16At(v.keys, i) }

// container returns the i-th container and its type. A shared (copy-on-write)
// container is replaced by the container it wraps.
func (v *raView) container(i int) (unsafe.Pointer, uint8) {
	c := *(*unsafe.Pointer)(unsafe.Add(v.containers, uintptr(i)*unsafe.Sizeof(uintptr(0))))
	t := *(*uint8)(unsafe.Add(v.typecodes, i))
	if t == goSharedType {
		sc := (*C.struct_shared_container_s)(c)
		c = unsafe.Pointer(sc.container)
		t = uint8(sc.typecode)
	}
	return c, t
}

func arrayOf(c unsafe.Pointer) (unsafe.Pointer, int) {
	ac := (*C.struct_array_container_s)(c)
	return unsafe.Pointer(ac.array), int(ac.cardinality)
}

func bitsetOf(c unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer((*C.struct_bitset_container_s)(c).words)
}

func runsOf(c unsafe.Pointer) (unsafe.Pointer, int) {
	rc := (*C.struct_run_container_s)(c)
	return unsafe.Pointer(rc.runs), int(rc.n_runs)
}

// cardinality returns the number of values in a container, or -1 if the
// container type is unknown.
func cardinality(c unsafe.Pointer, t uint8) int {
	switch t {
	case goBitsetType:
		if card := int((*C.struct_bitset_container_s)(c).cardinality); card >= 0 {
			return card
		}
		// The cardinality is unknown only in the middle of lazy
		// operations, but counting is cheap enough to stay safe.
		words, card := bitsetOf(c), 0
		for k := 0; k < bitsetWords; k++ {
			card += bits.OnesCount64(u64At(words, k))
		}
		return card
	case goArrayType:
		_, card := arrayOf(c)
		return card
	case goRunType:
		runs, n := runsOf(c)
		card := n
		for i := 0; i < n; i++ {
			_, length := runAt(runs, i)
			card += int(length)
		}
		return card
	}
	return -1
}

func (rb *Bitmap) contains(x uint32) bool {
	v := viewOf(rb)
	hb := uint16(x >> 16)
	idx := lowerBound16(v.keys, v.n, 2, hb)
	if idx < 0 || v.key(idx) != hb {
		runtime.KeepAlive(rb)
		return false
	}
	c, t := v.container(idx)
	low := uint16(x)
	var answer bool
	switch t {
	case goBitsetType:
		answer = (u64At(bitsetOf(c), int(low>>6))>>(low&63))&1 != 0
	case goArrayType:
		arr, card := arrayOf(c)
		i := lowerBound16(arr, card, 2, low)
		answer = i >= 0 && u16At(arr, i) == low
	case goRunType:
		runs, n := runsOf(c)
		// The run that starts last at or before low, if any.
		if i := lowerBound16(runs, n, 4, low); i >= 0 {
			start, length := runAt(runs, i)
			answer = uint32(low) <= start+length
		}
	default:
		answer = rb.containsC(x)
	}
	runtime.KeepAlive(rb)
	return answer
}

func (rb *Bitmap) minimum() uint32 {
	v := viewOf(rb)
	if v.n == 0 {
		return math.MaxUint32
	}
	c, t := v.container(0)
	var low uint32
	switch t {
	case goBitsetType:
		words := bitsetOf(c)
		for k := 0; k < bitsetWords; k++ {
			if w := u64At(words, k); w != 0 {
				low = uint32(k*64 + bits.TrailingZeros64(w))
				break
			}
		}
	case goArrayType:
		arr, _ := arrayOf(c)
		low = uint32(u16At(arr, 0))
	case goRunType:
		runs, _ := runsOf(c)
		low, _ = runAt(runs, 0)
	default:
		return rb.minimumC()
	}
	answer := uint32(v.key(0))<<16 | low
	runtime.KeepAlive(rb)
	return answer
}

func (rb *Bitmap) maximum() uint32 {
	v := viewOf(rb)
	if v.n == 0 {
		return 0
	}
	c, t := v.container(v.n - 1)
	var low uint32
	switch t {
	case goBitsetType:
		words := bitsetOf(c)
		for k := bitsetWords - 1; k >= 0; k-- {
			if w := u64At(words, k); w != 0 {
				low = uint32(k*64 + 63 - bits.LeadingZeros64(w))
				break
			}
		}
	case goArrayType:
		arr, card := arrayOf(c)
		low = uint32(u16At(arr, card-1))
	case goRunType:
		runs, n := runsOf(c)
		start, length := runAt(runs, n-1)
		low = start + length
	default:
		return rb.maximumC()
	}
	answer := uint32(v.key(v.n-1))<<16 | low
	runtime.KeepAlive(rb)
	return answer
}

// containerRank returns the number of values at or below low in a container,
// or -1 if the container type is unknown.
func containerRank(c unsafe.Pointer, t uint8, low uint16) int {
	switch t {
	case goBitsetType:
		words, end := bitsetOf(c), int(low>>6)
		r := 0
		for k := 0; k < end; k++ {
			r += bits.OnesCount64(u64At(words, k))
		}
		// 2 << 63 is 0 for a uint64, so the mask is all ones for bit 63.
		return r + bits.OnesCount64(u64At(words, end)&((uint64(2)<<(low&63))-1))
	case goArrayType:
		arr, card := arrayOf(c)
		return lowerBound16(arr, card, 2, low) + 1
	case goRunType:
		runs, n := runsOf(c)
		r := 0
		for i := 0; i < n; i++ {
			start, length := runAt(runs, i)
			if uint32(low) < start {
				break
			}
			if uint32(low) <= start+length {
				return r + int(uint32(low)-start) + 1
			}
			r += int(length) + 1
		}
		return r
	}
	return -1
}

// The Go code wins on short queries because it avoids the cost of a cgo call,
// but C is faster per step on long ones: Rank and Select add up the
// cardinalities of many containers, and Intersect may scan long arrays before
// it can conclude. So Rank and Select visit at most goContainerBudget
// containers in Go, and Intersect takes at most goIntersectBudget steps (a
// step is a key skipped or a pair of values compared); beyond that, they
// start over in C. The wasted work is bounded and small next to the cost of
// the queries that exhaust the budget. (They are variables so that the tests
// can lift them, to check the Go code on long queries too.)
var (
	goContainerBudget = 16
	goIntersectBudget = 24
)

func (rb *Bitmap) rank(x uint32) uint64 {
	v := viewOf(rb)
	hb := uint16(x >> 16)
	// The containers to visit are those up to the last one whose key is at
	// most hb.
	last := lowerBound16(v.keys, v.n, 2, hb)
	if last >= goContainerBudget {
		return rb.rankC(x)
	}
	var size uint64
	for i := 0; i <= last; i++ {
		c, t := v.container(i)
		var r int
		if v.key(i) < hb {
			r = cardinality(c, t)
		} else {
			r = containerRank(c, t, uint16(x))
		}
		if r < 0 {
			return rb.rankC(x)
		}
		size += uint64(r)
	}
	runtime.KeepAlive(rb)
	return size
}

// selectInWord returns the position of the (r+1)-th set bit of w; w must have
// more than r bits set.
func selectInWord(w uint64, r int) int {
	for ; r > 0; r-- {
		w &= w - 1
	}
	return bits.TrailingZeros64(w)
}

// containerSelect returns the value of rank r (counting from zero) in a
// container of a known type that holds more than r values.
func containerSelect(c unsafe.Pointer, t uint8, r int) uint32 {
	switch t {
	case goBitsetType:
		words := bitsetOf(c)
		for k := 0; k < bitsetWords; k++ {
			w := u64At(words, k)
			if cnt := bits.OnesCount64(w); r >= cnt {
				r -= cnt
			} else {
				return uint32(k*64 + selectInWord(w, r))
			}
		}
	case goArrayType:
		arr, _ := arrayOf(c)
		return uint32(u16At(arr, r))
	case goRunType:
		runs, n := runsOf(c)
		for i := 0; i < n; i++ {
			start, length := runAt(runs, i)
			if r <= int(length) {
				return start + uint32(r)
			}
			r -= int(length) + 1
		}
	}
	return 0 // unreachable when the preconditions hold
}

func (rb *Bitmap) selectValue(rank uint32) (uint32, bool) {
	v := viewOf(rb)
	r := int(rank)
	budget := goContainerBudget
	for i := 0; i < v.n; i++ {
		c, t := v.container(i)
		// Summing the runs of a run container costs about as much as
		// visiting a container per 8 runs.
		if budget--; t == goRunType {
			_, n := runsOf(c)
			budget -= n / 8
		}
		if budget < 0 {
			return rb.selectC(rank)
		}
		card := cardinality(c, t)
		if card < 0 {
			return rb.selectC(rank)
		}
		if r < card {
			answer := uint32(v.key(i))<<16 | containerSelect(c, t, r)
			runtime.KeepAlive(rb)
			return answer, true
		}
		r -= card
	}
	runtime.KeepAlive(rb)
	return 0, false
}

// Most intersection helpers below take the remaining work budget and return
// it, decremented by the steps they took. A negative budget means that they
// gave up.

// bitsetRangeAny reports whether a bitset has a bit set in [start, end].
func bitsetRangeAny(words unsafe.Pointer, start, end uint32, budget int) (bool, int) {
	first, last := int(start>>6), int(end>>6)
	lowMask := ^uint64(0) << (start & 63)
	highMask := ^uint64(0) >> (63 - end&63)
	if first == last {
		return u64At(words, first)&lowMask&highMask != 0, budget
	}
	if u64At(words, first)&lowMask != 0 {
		return true, budget
	}
	for k := first + 1; k < last; k++ {
		if budget--; budget < 0 {
			return false, budget
		}
		if u64At(words, k) != 0 {
			return true, budget
		}
	}
	return u64At(words, last)&highMask != 0, budget
}

func arraysIntersect(a unsafe.Pointer, na int, b unsafe.Pointer, nb int, budget int) (bool, int) {
	if na > nb {
		a, na, b, nb = b, nb, a, na
	}
	if na == 0 {
		return false, budget
	}
	if nb >= 8*na {
		// Very different sizes: look the values of the small array up in
		// the large one, moving forward through it.
		pos := 0
		for i := 0; i < na; i++ {
			if budget -= 2; budget < 0 {
				return false, budget
			}
			x := u16At(a, i)
			pos = gallop16(b, 2, nb, pos, x)
			if pos == nb {
				return false, budget
			}
			if u16At(b, pos) == x {
				return true, budget
			}
		}
		return false, budget
	}
	// A merge without data-dependent branches but for the (rare) match:
	// lt is 1 when x < y and 0 otherwise.
	i, j := 0, 0
	for i < na && j < nb {
		if budget--; budget < 0 {
			return false, budget
		}
		x, y := u16At(a, i), u16At(b, j)
		if x == y {
			return true, budget
		}
		lt := int(uint(int(x)-int(y)) >> 63)
		i += lt
		j += 1 - lt
	}
	return false, budget
}

func arrayRunIntersect(arr unsafe.Pointer, card int, runs unsafe.Pointer, n int, budget int) (bool, int) {
	i, j := 0, 0
	for i < card && j < n {
		if budget--; budget < 0 {
			return false, budget
		}
		x := uint32(u16At(arr, i))
		start, length := runAt(runs, j)
		switch {
		case x > start+length:
			j++
		case x >= start:
			return true, budget
		default:
			i++
		}
	}
	return false, budget
}

func runsIntersect(a unsafe.Pointer, na int, b unsafe.Pointer, nb int) bool {
	i, j := 0, 0
	for i < na && j < nb {
		s1, l1 := runAt(a, i)
		s2, l2 := runAt(b, j)
		switch {
		case s1+l1 < s2:
			i++
		case s2+l2 < s1:
			j++
		default:
			return true
		}
	}
	return false
}

// containersIntersect reports whether two containers share a value. A
// negative budget on return means that it gave up, or that a container type
// is unknown.
func containersIntersect(c1 unsafe.Pointer, t1 uint8, c2 unsafe.Pointer, t2 uint8, budget int) (bool, int) {
	if t1 > t2 { // so that the cases below need only list t1 <= t2
		c1, t1, c2, t2 = c2, t2, c1, t1
	}
	switch {
	case t1 == goBitsetType && t2 == goBitsetType:
		// For this pair and the next one, as well as for two run
		// containers, Go is as fast as C per step: no budget is needed.
		w1, w2 := bitsetOf(c1), bitsetOf(c2)
		for k := 0; k < bitsetWords; k++ {
			if u64At(w1, k)&u64At(w2, k) != 0 {
				return true, budget
			}
		}
		return false, budget
	case t1 == goBitsetType && t2 == goArrayType:
		words := bitsetOf(c1)
		arr, card := arrayOf(c2)
		for i := 0; i < card; i++ {
			x := u16At(arr, i)
			if (u64At(words, int(x>>6))>>(x&63))&1 != 0 {
				return true, budget
			}
		}
		return false, budget
	case t1 == goBitsetType && t2 == goRunType:
		words := bitsetOf(c1)
		runs, n := runsOf(c2)
		for i := 0; i < n; i++ {
			if budget--; budget < 0 {
				return false, budget
			}
			start, length := runAt(runs, i)
			var found bool
			if found, budget = bitsetRangeAny(words, start, start+length, budget); found || budget < 0 {
				return found, budget
			}
		}
		return false, budget
	case t1 == goArrayType && t2 == goArrayType:
		a, na := arrayOf(c1)
		b, nb := arrayOf(c2)
		return arraysIntersect(a, na, b, nb, budget)
	case t1 == goArrayType && t2 == goRunType:
		arr, card := arrayOf(c1)
		runs, n := runsOf(c2)
		return arrayRunIntersect(arr, card, runs, n, budget)
	case t1 == goRunType && t2 == goRunType:
		a, na := runsOf(c1)
		b, nb := runsOf(c2)
		return runsIntersect(a, na, b, nb), budget
	}
	return false, -1
}

func (rb *Bitmap) intersect(x2 *Bitmap) bool {
	a, b := viewOf(rb), viewOf(x2)
	i, j := 0, 0
	answer := false
	budget := goIntersectBudget
walk:
	for i < a.n && j < b.n {
		if budget--; budget < 0 {
			return rb.intersectC(x2)
		}
		ka, kb := a.key(i), b.key(j)
		switch {
		case ka == kb:
			c1, t1 := a.container(i)
			c2, t2 := b.container(j)
			var found bool
			found, budget = containersIntersect(c1, t1, c2, t2, budget)
			if budget < 0 {
				return rb.intersectC(x2)
			}
			if found {
				answer = true
				break walk
			}
			i++
			j++
		case ka < kb:
			i = gallop16(a.keys, 2, a.n, i+1, kb)
		default:
			j = gallop16(b.keys, 2, b.n, j+1, ka)
		}
	}
	runtime.KeepAlive(rb)
	runtime.KeepAlive(x2)
	return answer
}
