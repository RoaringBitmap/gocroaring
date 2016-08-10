package gocroaring

import (
	"fmt"
	"testing"
)

func TestSimpleCard(t *testing.T) {
	bitmap := NewBitmap()
	for i := 100; i < 1000; i++ {
		bitmap.Add(uint32(i))
	}
	c := bitmap.GetCardinality()
	fmt.Println("cardinality: ", c)
	if c != 900 {
		t.Error("Expected ", 900, ", got ", c)
	}
}

func TestFancier(t *testing.T) {
	rb1 := NewBitmap()
	rb1.Add(1)
	rb1.Add(2)
	rb1.Add(3)
	rb1.Add(4)
	rb1.Add(5)
	rb1.Add(100)
	rb1.Add(1000)
	rb2 := NewBitmap()
	rb2.Add(3)
	rb2.Add(4)
	rb2.Add(1000)
	rb3 := NewBitmap()
	fmt.Println("Cardinality: ", rb1.GetCardinality())
	if rb1.GetCardinality() != 7 {
		t.Error("Bad card")
  }
  if ! rb1.Contains(3) {
		t.Error("should contain it")
  }
	rb1.And(rb2)
  fmt.Println(rb1.ToArray())
	rb3.Add(1)
	rb3.Add(5)
	rb3.Or(rb1)
	// next we include an example of serialization
	buf := make([]byte, rb1.GetSerializedSizeInBytes())
	rb1.Write(buf) // we omit error handling
	newrb,_ := Read(buf)
	if rb1.Equals(newrb) {
		fmt.Println("I wrote the content to a byte stream and read it back.")
	} else {
		t.Error("Bad read")
  }
}
