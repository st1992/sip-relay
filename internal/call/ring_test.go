package call

import (
	"bytes"
	"testing"
)

func TestByteRingWriteReadRoundTrip(t *testing.T) {
	r := newByteRing(8)
	if n := r.Write([]byte{1, 2, 3}); n != 3 {
		t.Fatalf("Write = %d, want 3", n)
	}
	if r.Len() != 3 {
		t.Fatalf("Len = %d, want 3", r.Len())
	}

	dst := make([]byte, 3)
	if n := r.Read(dst); n != 3 {
		t.Fatalf("Read = %d, want 3", n)
	}
	if !bytes.Equal(dst, []byte{1, 2, 3}) {
		t.Fatalf("Read gave %v, want [1 2 3]", dst)
	}
	if r.Len() != 0 {
		t.Fatalf("Len after full read = %d, want 0", r.Len())
	}
}

// Chunk boundaries must be invisible: bytes written across several calls read
// back as one contiguous stream, which is what lets the pacer cut fixed-size
// frames wherever it likes.
func TestByteRingIsContiguousAcrossWrites(t *testing.T) {
	r := newByteRing(16)
	r.Write([]byte{1, 2, 3})
	r.Write([]byte{4, 5})
	r.Write([]byte{6, 7, 8, 9})

	dst := make([]byte, 9)
	if n := r.Read(dst); n != 9 {
		t.Fatalf("Read = %d, want 9", n)
	}
	if !bytes.Equal(dst, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("Read gave %v, want a contiguous 1..9", dst)
	}
}

func TestByteRingWrapsAroundCapacity(t *testing.T) {
	r := newByteRing(5)
	r.Write([]byte{1, 2, 3, 4})

	dst := make([]byte, 3)
	r.Read(dst) // head now at 3

	// This write wraps past the end of the backing array.
	if n := r.Write([]byte{5, 6, 7}); n != 3 {
		t.Fatalf("Write across the wrap = %d, want 3", n)
	}
	got := make([]byte, 4)
	if n := r.Read(got); n != 4 {
		t.Fatalf("Read = %d, want 4", n)
	}
	if !bytes.Equal(got, []byte{4, 5, 6, 7}) {
		t.Fatalf("Read across the wrap gave %v, want [4 5 6 7]", got)
	}
}

func TestByteRingShortWriteReportsOverflow(t *testing.T) {
	r := newByteRing(4)
	if n := r.Write([]byte{1, 2, 3, 4, 5, 6}); n != 4 {
		t.Fatalf("Write = %d, want 4 accepted", n)
	}
	if r.Len() != 4 {
		t.Fatalf("Len = %d, want 4", r.Len())
	}
	// Oldest audio is closest to playing, so it must survive; the newest
	// bytes are the ones dropped.
	got := make([]byte, 4)
	r.Read(got)
	if !bytes.Equal(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("overflow evicted buffered audio: got %v, want [1 2 3 4]", got)
	}
}

func TestByteRingShortReadOnUnderrun(t *testing.T) {
	r := newByteRing(8)
	r.Write([]byte{1, 2})

	dst := make([]byte, 5)
	if n := r.Read(dst); n != 2 {
		t.Fatalf("Read = %d, want 2 (a short read, not an error)", n)
	}
	if n := r.Read(dst); n != 0 {
		t.Fatalf("Read on an empty ring = %d, want 0", n)
	}
}

func TestByteRingReset(t *testing.T) {
	r := newByteRing(8)
	r.Write([]byte{1, 2, 3})
	r.Reset()
	if r.Len() != 0 {
		t.Fatalf("Len after Reset = %d, want 0", r.Len())
	}
	r.Write([]byte{9})
	dst := make([]byte, 1)
	r.Read(dst)
	if dst[0] != 9 {
		t.Fatalf("ring unusable after Reset: got %v", dst)
	}
}
