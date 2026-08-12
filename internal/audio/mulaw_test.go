package audio

import "testing"

func TestMulawSilenceBytesDecodeToZero(t *testing.T) {
	// This codebase uses 0xFF as its PCMU silence byte (see internal/rtp
	// SilenceByte); mu-law has two representations of zero (positive and
	// negative), 0x7F and 0xFF, and both must decode to exactly 0.
	for _, b := range []byte{0x7F, 0xFF} {
		if got := MulawToLinear(b); got != 0 {
			t.Errorf("MulawToLinear(%#x) = %d, want 0", b, got)
		}
	}
}

func TestMulawEncodeOfAlreadyQuantizedValueIsExact(t *testing.T) {
	// mulawEncodeTable is built as the exact nearest-match inverse of
	// mulawDecodeTable, so re-encoding an already-quantized value must land
	// on a byte that decodes to precisely the same value (not just "close") --
	// no other byte can be strictly closer than an exact match.
	for b := 0; b < 256; b++ {
		want := MulawToLinear(byte(b))
		got := MulawToLinear(LinearToMulaw(want))
		if got != want {
			t.Fatalf("byte %#x: decode(encode(decode(b)))=%d, want %d", b, got, want)
		}
	}
}

func TestMulawRoundTripErrorIsBounded(t *testing.T) {
	// Derive the bound from the table under test itself (the largest gap
	// between adjacent decoded values) rather than a hardcoded magic
	// constant, so this stays meaningful regardless of exact table shape.
	sorted := make([]int, 256)
	for i := range sorted {
		sorted[i] = int(MulawToLinear(byte(i)))
	}
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	maxGap := 0
	for i := 1; i < len(sorted); i++ {
		if gap := sorted[i] - sorted[i-1]; gap > maxGap {
			maxGap = gap
		}
	}

	samples := []int16{0, 1, -1, 100, -100, 1000, -1000, 10000, -10000, 32000, -32000, 32767, -32768}
	for _, want := range samples {
		got := MulawToLinear(LinearToMulaw(want))
		diff := int(got) - int(want)
		if diff < 0 {
			diff = -diff
		}
		if diff > maxGap {
			t.Errorf("sample %d: round trip = %d, error %d exceeds max quantization gap %d", want, got, diff, maxGap)
		}
	}
}

func TestDecodeEncodeMulawSliceLengths(t *testing.T) {
	src := []byte{0xFF, 0x7F, 0x00, 0x80, 0x55, 0xAA}
	decoded := DecodeMulaw(nil, src)
	if len(decoded) != len(src) {
		t.Fatalf("DecodeMulaw length = %d, want %d", len(decoded), len(src))
	}
	encoded := EncodeMulaw(nil, decoded)
	if len(encoded) != len(decoded) {
		t.Fatalf("EncodeMulaw length = %d, want %d", len(encoded), len(decoded))
	}
}

func TestDecodeMulawAppendsToExistingSlice(t *testing.T) {
	dst := []int16{1, 2, 3}
	got := DecodeMulaw(dst, []byte{0xFF})
	if len(got) != 4 || got[3] != 0 {
		t.Fatalf("DecodeMulaw append result = %v, want [1 2 3 0]", got)
	}
}
