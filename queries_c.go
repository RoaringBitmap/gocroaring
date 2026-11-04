//go:build !(amd64 || arm64) || gocroaring_cqueries

package gocroaring

// On architectures where unaligned loads may fault or be slow, the read-only
// queries go through CRoaring. Portable frozen views point into the caller's
// buffer, so their arrays are not necessarily aligned. The gocroaring_cqueries
// build tag selects these C versions on every architecture.

func (rb *Bitmap) contains(x uint32) bool                 { return rb.containsC(x) }
func (rb *Bitmap) rank(x uint32) uint64                   { return rb.rankC(x) }
func (rb *Bitmap) minimum() uint32                        { return rb.minimumC() }
func (rb *Bitmap) maximum() uint32                        { return rb.maximumC() }
func (rb *Bitmap) selectValue(rank uint32) (uint32, bool) { return rb.selectC(rank) }
func (rb *Bitmap) intersect(x2 *Bitmap) bool              { return rb.intersectC(x2) }
