package main

import (
	"fmt"
	"os"
	"runtime"
	"syscall"

	"github.com/RoaringBitmap/gocroaring"
)

func main() {
	var rusage syscall.Rusage

	fmt.Printf("Starting execution... ")
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	bitmap := gocroaring.New()
	for i := uint32(0); i < 100000; i++ {
		bitmap.Add(i*10)
	}
	fmt.Printf("Created reference bitmap... ")
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	const N = 100000
	bitmaps := make([]*gocroaring.Bitmap, N)
	fmt.Printf("Allocated slice for %d bitmap pointers... ", N)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	for i := range bitmaps {
		bitmaps[i] = bitmap.Clone()
		bitmaps[i].Free()
	}
	fmt.Println("Copied then explicitly freed %d bitmaps... ")
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	bitmaps = nil
	runtime.GC()
	fmt.Printf("GC'd old bitmaps... ")
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)

	bitmaps = make([]*gocroaring.Bitmap, N)
	fmt.Printf("Allocated slice for %d bitmap pointers... ", N)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		fmt.Printf("Getrusage failed with error: %s\n", err)
		os.Exit(1)
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
		os.Exit(1)
	}
	fmt.Printf("max RSS: %d bytes\n", rusage.Maxrss)
}
