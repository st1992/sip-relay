package audio

import (
	"reflect"
	"testing"
)

func TestSamplesToBytesLERoundTrip(t *testing.T) {
	samples := []int16{0, 1, -1, 32767, -32768, 12345, -12345}

	bytes := SamplesToBytesLE(nil, samples)
	if len(bytes) != 2*len(samples) {
		t.Fatalf("len(bytes) = %d, want %d", len(bytes), 2*len(samples))
	}

	var d PCM16LEDecoder
	got := d.Decode(nil, bytes)
	if !reflect.DeepEqual(got, samples) {
		t.Fatalf("round trip = %v, want %v", got, samples)
	}
	if d.hasPending {
		t.Fatal("decoder should have no pending byte after an even-length input")
	}
}

func TestPCM16LEDecoderCarriesOddByteAcrossCalls(t *testing.T) {
	samples := []int16{100, -200, 300, -400, 500}
	whole := SamplesToBytesLE(nil, samples)

	// Split at an odd boundary so a sample straddles two Decode calls.
	splitAt := 3
	first, second := whole[:splitAt], whole[splitAt:]

	var d PCM16LEDecoder
	got := d.Decode(nil, first)
	if !d.hasPending {
		t.Fatal("expected a pending byte after an odd-length chunk")
	}
	got = d.Decode(got, second)
	if !reflect.DeepEqual(got, samples) {
		t.Fatalf("split decode = %v, want %v", got, samples)
	}
}

func TestPCM16LEDecoderHandlesManyOddSplits(t *testing.T) {
	samples := make([]int16, 50)
	for i := range samples {
		samples[i] = int16(i*137 - 3000)
	}
	whole := SamplesToBytesLE(nil, samples)

	var d PCM16LEDecoder
	var got []int16
	for i := 0; i < len(whole); i += 3 { // 3-byte chunks guarantee repeated odd splits
		end := i + 3
		if end > len(whole) {
			end = len(whole)
		}
		got = d.Decode(got, whole[i:end])
	}
	if !reflect.DeepEqual(got, samples) {
		t.Fatalf("chunked decode = %v, want %v", got, samples)
	}
}

func TestPCM16LEDecoderEmptyInputIsNoop(t *testing.T) {
	var d PCM16LEDecoder
	got := d.Decode([]int16{7}, nil)
	if !reflect.DeepEqual(got, []int16{7}) {
		t.Fatalf("Decode with empty input = %v, want [7]", got)
	}
	if d.hasPending {
		t.Fatal("empty input should not set a pending byte")
	}
}

func TestPCM16LEDecoderReset(t *testing.T) {
	var d PCM16LEDecoder
	d.Decode(nil, []byte{0x01})
	if !d.hasPending {
		t.Fatal("expected pending byte before Reset")
	}
	d.Reset()
	if d.hasPending {
		t.Fatal("Reset should clear the pending byte")
	}
}
