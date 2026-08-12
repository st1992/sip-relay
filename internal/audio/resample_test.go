package audio

import (
	"math"
	"reflect"
	"testing"
)

// sineWave generates n samples of a sinHz tone sampled at sampleHz.
func sineWave(n int, sampleHz, sinHz float64, amplitude float64) []int16 {
	out := make([]int16, n)
	for i := range out {
		out[i] = int16(amplitude * math.Sin(2*math.Pi*sinHz*float64(i)/sampleHz))
	}
	return out
}

func TestResamplerUpsampleOutputLength(t *testing.T) {
	real := sineWave(2000, 8000, 300, 10000)
	pad := make([]int16, 4*InterpolationOrder) // flush the trailing held-back context
	total := len(real) + len(pad)

	r := NewResampler(8000, 16000)
	out := r.Process(nil, real)
	out = r.Process(out, pad)

	want := total * 16000 / 8000
	if diff := abs(len(out) - want); diff > 2*InterpolationOrder {
		t.Fatalf("output length = %d, want ~%d (diff %d)", len(out), want, diff)
	}
}

func TestResamplerDownsampleOutputLength(t *testing.T) {
	real := sineWave(3000, 24000, 300, 10000)
	pad := make([]int16, 4*InterpolationOrder)
	total := len(real) + len(pad)

	r := NewResampler(24000, 8000)
	out := r.Process(nil, real)
	out = r.Process(out, pad)

	want := total * 8000 / 24000
	if diff := abs(len(out) - want); diff > 2*InterpolationOrder {
		t.Fatalf("output length = %d, want ~%d (diff %d)", len(out), want, diff)
	}
}

func TestResamplerIdentitySampleRateReproducesInputExactly(t *testing.T) {
	real := sineWave(500, 8000, 300, 10000)
	pad := make([]int16, InterpolationOrder+1)

	r := NewResampler(8000, 8000)
	out := r.Process(nil, real)
	out = r.Process(out, pad)

	if len(out) < len(real) {
		t.Fatalf("output length = %d, want at least %d", len(out), len(real))
	}
	if !reflect.DeepEqual(out[:len(real)], real) {
		t.Fatalf("identity resample did not reproduce input exactly")
	}
}

func TestResamplerContinuityAcrossChunkBoundaries(t *testing.T) {
	real := sineWave(4000, 8000, 300, 10000)
	pad := make([]int16, 4*InterpolationOrder)
	full := append(append([]int16{}, real...), pad...)

	whole := NewResampler(8000, 16000)
	wantOut := whole.Process(nil, full)

	chunked := NewResampler(8000, 16000)
	var gotOut []int16
	chunkSizes := []int{17, 23, 31, 5, 40}
	pos := 0
	ci := 0
	for pos < len(full) {
		size := chunkSizes[ci%len(chunkSizes)]
		ci++
		end := pos + size
		if end > len(full) {
			end = len(full)
		}
		gotOut = chunked.Process(gotOut, full[pos:end])
		pos = end
	}

	if !reflect.DeepEqual(gotOut, wantOut) {
		t.Fatalf("chunked resampling diverged from single-call resampling: got %d samples, want %d samples", len(gotOut), len(wantOut))
	}
}

func TestResamplerZeroLengthChunkIsNoop(t *testing.T) {
	real := sineWave(200, 8000, 300, 10000)

	baseline := NewResampler(8000, 16000)
	want := baseline.Process(nil, real)

	withNoop := NewResampler(8000, 16000)
	got := withNoop.Process(nil, nil)
	if len(got) != 0 {
		t.Fatalf("Process(nil, nil) produced %d samples, want 0", len(got))
	}
	got = withNoop.Process(got, real)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("a leading zero-length Process call altered subsequent output")
	}
}

func TestResamplerResetMatchesFreshInstance(t *testing.T) {
	warmup := sineWave(300, 8000, 300, 10000)
	real := sineWave(300, 8000, 440, 8000)

	r := NewResampler(8000, 16000)
	_ = r.Process(nil, warmup)
	r.Reset()
	got := r.Process(nil, real)

	fresh := NewResampler(8000, 16000)
	want := fresh.Process(nil, real)

	if !reflect.DeepEqual(got, want) {
		t.Fatal("Reset() did not fully restore fresh-instance behavior")
	}
}

func TestClampToInt16(t *testing.T) {
	cases := []struct {
		in   float64
		want int16
	}{
		{0, 0},
		{100.4, 100},
		{100.6, 101},
		{32767, 32767},
		{32767.9, 32767},
		{40000, 32767},
		{-32768, -32768},
		{-40000, -32768},
	}
	for _, c := range cases {
		if got := clampToInt16(c.in); got != c.want {
			t.Errorf("clampToInt16(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
