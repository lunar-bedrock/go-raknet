package raknet

import (
	"bytes"
	"testing"
)

func Test_uint24(t *testing.T) {
	b := bytes.NewBuffer(nil)
	writeUint24(b, 123456)
	val, err := readUint24(b)
	if err != nil {
		t.Fatalf("error reading uint24: %v", err)
	}
	if val != 123456 {
		t.Fatal("read uint24 was not equal to 123456")
	}
}

func TestUint24IncrementWraps(t *testing.T) {
	u := uint24(0xffffff)
	if old := u.Inc(); old != 0xffffff || u != 0 {
		t.Fatalf("increment: old=%#x current=%#x", old, u)
	}
}
