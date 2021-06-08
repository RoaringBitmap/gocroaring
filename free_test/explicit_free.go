package main

import (
	"fmt"
	"os"
	"runtime"
	"syscall"

	"github.com/RoaringBitmap/gocroaring"
)

// Exits with code 0 on success, 1 on test failure
// or 2 on program failure (e.g. OOM or syscall error).

func main() {
	const (
		N = 10000
		M = 10000
	)

	var rusage syscall.Rusage
	var maxrss_0, maxrss_free, maxrss_gc int64

	fmt.Printf("Starting execution... ")
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(2)
	}
	maxrss_0 = rusage.Maxrss
	fmt.Printf("max RSS: %d bytes\n", maxrss_0)

	bitmap := gocroaring.New()
	for i := uint32(0); i < M; i++ {
		bitmap.Add(i*10)
	}
	fmt.Printf("Created reference bitmap... ")
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(2)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	bitmaps := make([]*gocroaring.Bitmap, N)
	fmt.Printf("Allocated slice for %d bitmap pointers... ", N)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(2)
	}
	maxrss_free = rusage.Maxrss
	fmt.Printf("max RSS: %d bytes\n", maxrss_free)

	for i := range bitmaps {
		bitmaps[i] = bitmap.Clone()
		bitmaps[i].Free()
	}
	fmt.Printf("Copied then explicitly freed %d bitmaps... ", N)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(2)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	bitmaps = nil
	runtime.GC()
	fmt.Printf("GC'd old bitmaps... ")
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(2)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	bitmaps = make([]*gocroaring.Bitmap, N)
	fmt.Printf("Allocated slice for %d bitmap pointers... ", N)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(2)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	for i := range bitmaps {
		bitmaps[i] = bitmap.Clone()
	}
	bitmaps = nil
	runtime.GC()
	fmt.Printf("Copied then GC'd %d bitmaps... ", N)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(2)
	}
	maxrss_gc = rusage.Maxrss
	fmt.Printf("max RSS: %d bytes\n", maxrss_gc)

	ballpark_free := maxrss_free - maxrss_0
	ballpark_gc := maxrss_gc - maxrss_0
	fmt.Printf("Used ~%d bytes with explicit free\n", ballpark_free)
	fmt.Printf("Used ~%d bytes without explicit free\n", ballpark_gc)

	if ballpark_gc < 100*ballpark_free {
		fmt.Printf("Expected a much greater difference!\n")
		os.Exit(1)
	}
}
