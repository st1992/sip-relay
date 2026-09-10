package audio

import (
	"math"
	"reflect"
	"testing"
)

// toneAmplitude measures the peak amplitude of a single frequency in samples,
// in the same units as the input, without needing a full FFT.
//
// It correlates against a complex exponential through a Hann window. The
// window matters: correlating directly against an arbitrary frequency leaks
// energy across bins whenever the tone is not an exact integer number of
// cycles in the sample window, costing up to 3.9dB purely as a measurement
// artifact and making a flat passband look like a ragged one.
func toneAmplitude(samples []int16, sampleHz, targetHz float64) float64 {
	n := len(samples)
	if n == 0 {
		return 0
	}
	var re, im, gain float64
	for t, x := range samples {
		w := 0.5 - 0.5*math.Cos(2*math.Pi*float64(t)/float64(n-1))
		phase := 2 * math.Pi * targetHz * float64(t) / sampleHz
		re += w * float64(x) * math.Cos(phase)
		im += w * float64(x) * math.Sin(phase)
		gain += w
	}
	return 2 * math.Hypot(re, im) / gain
}

func dB(ratio float64) float64 {
	if ratio <= 0 {
		return math.Inf(-1)
	}
	return 20 * math.Log10(ratio)
}

// resampleTone pushes a pure tone through a full rate conversion and returns
// the output, with the filter's start-up transient trimmed so spectral
// measurements see only steady state.
func resampleTone(srcHz, dstHz int, toneHz, amplitude float64, opts ...ResamplerOption) []int16 {
	const srcSamples = 24000 // one second at the default input rate
	in := sineWave(srcSamples, float64(srcHz), toneHz, amplitude)

	r := NewResampler(srcHz, dstHz, opts...)
	out := r.Process(nil, in)
	out = r.Process(out, make([]int16, 4*InterpolationOrder)) // flush held-back context

	// Drop the leading group-delay ramp of the FIR.
	skip := dstHz / 40 // 25ms
	if skip >= len(out) {
		return out
	}
	return out[skip:]
}

// The core of the fix. At 24kHz a 6kHz tone sits above the 4kHz Nyquist of an
// 8kHz output, so decimating without filtering folds it down onto 2kHz --
// indistinguishable from real speech content once it lands there.
func TestDownsampleRejectsAliasingTone(t *testing.T) {
	const (
		srcHz     = 24000
		dstHz     = 8000
		toneHz    = 6000 // folds to |6000 - 8000| = 2000
		aliasHz   = 2000
		amplitude = 10000
	)

	// Reference: a genuine 2kHz tone through the same path, so the comparison
	// is against real passband content rather than an abstract scale.
	ref := resampleTone(srcHz, dstHz, aliasHz, amplitude)
	refAmp := toneAmplitude(ref, dstHz, aliasHz)
	if refAmp <= 0 {
		t.Fatal("reference passband tone measured as silence")
	}

	filtered := resampleTone(srcHz, dstHz, toneHz, amplitude)
	aliasAmp := toneAmplitude(filtered, dstHz, aliasHz)
	rejection := -dB(aliasAmp / refAmp)
	t.Logf("alias rejection with filter: %.1f dB (alias amp %.1f, reference %.1f)", rejection, aliasAmp, refAmp)

	const wantRejection float64 = 40
	if rejection < wantRejection {
		t.Errorf("alias rejection = %.1f dB, want >= %.0f dB", rejection, wantRejection)
	}

	// Guard against the test passing for the wrong reason: the unfiltered path
	// must visibly fail the same bar, or this proves nothing.
	unfiltered := resampleTone(srcHz, dstHz, toneHz, amplitude, WithoutAntiAlias())
	unfilteredAlias := toneAmplitude(unfiltered, dstHz, aliasHz)
	unfilteredRejection := -dB(unfilteredAlias / refAmp)
	t.Logf("alias rejection without filter: %.1f dB", unfilteredRejection)
	if unfilteredRejection >= wantRejection {
		t.Errorf("unfiltered path rejected the alias by %.1f dB; the test signal is not exercising aliasing",
			unfilteredRejection)
	}
	if rejection <= unfilteredRejection {
		t.Errorf("filter did not improve alias rejection: %.1f dB filtered vs %.1f dB unfiltered",
			rejection, unfilteredRejection)
	}
}

// Rejecting the stopband is only useful if the passband survives intact.
func TestDownsamplePreservesPassband(t *testing.T) {
	const amplitude = 10000
	for _, toneHz := range []float64{300, 1000, 2000, 3000} {
		out := resampleTone(24000, 8000, toneHz, amplitude)
		got := toneAmplitude(out, 8000, toneHz)
		loss := -dB(got / amplitude)
		t.Logf("%.0f Hz: %.2f dB loss", toneHz, loss)
		if loss > 3 {
			t.Errorf("%.0f Hz attenuated by %.2f dB, want <= 3 dB", toneHz, loss)
		}
	}
}

func TestDownsampleAttenuatesTransitionBand(t *testing.T) {
	const amplitude = 10000
	// Just below the output Nyquist: must be well down by the time it gets
	// there, or it aliases onto itself.
	out := resampleTone(24000, 8000, 3950, amplitude)
	got := toneAmplitude(out, 8000, 3950)
	attenuation := -dB(got / amplitude)
	t.Logf("3950 Hz attenuated by %.1f dB", attenuation)
	if attenuation < 20 {
		t.Errorf("3950 Hz attenuated by only %.1f dB, want >= 20 dB", attenuation)
	}
}

// The filter carries a delay line across calls, so output must not depend on
// how the caller happened to chunk the input -- the same property the
// resampler and transcoders already guarantee.
func TestLowpassChunkInvariance(t *testing.T) {
	in := sineWave(4000, 24000, 900, 12000)

	whole := NewResampler(24000, 8000).Process(nil, in)

	chunked := NewResampler(24000, 8000)
	var got []int16
	sizes := []int{7, 13, 21, 3, 50}
	for i, si := 0, 0; i < len(in); si++ {
		size := sizes[si%len(sizes)]
		if i+size > len(in) {
			size = len(in) - i
		}
		got = chunked.Process(got, in[i:i+size])
		i += size
	}

	if !reflect.DeepEqual(got, whole) {
		t.Fatalf("chunked output differs from whole-stream output: %d vs %d samples", len(got), len(whole))
	}
}

func TestLowpassResetMatchesFreshInstance(t *testing.T) {
	warmup := sineWave(2000, 24000, 5000, 12000)
	real := sineWave(2000, 24000, 900, 12000)

	r := NewResampler(24000, 8000)
	r.Process(nil, warmup)
	r.Reset()
	got := r.Process(nil, real)

	want := NewResampler(24000, 8000).Process(nil, real)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("Reset did not clear filter state: output differs from a fresh instance")
	}
}

// Pins the scope: only downsampling aliases, so only downsampling is filtered.
// The upsample path feeding the backend's ASR stays bit-identical.
func TestOnlyDownsamplingIsFiltered(t *testing.T) {
	if r := NewResampler(8000, 16000); r.lp != nil {
		t.Error("upsample 8k->16k should not build an anti-aliasing filter")
	}
	if r := NewResampler(8000, 8000); r.lp != nil {
		t.Error("identity 8k->8k should not build an anti-aliasing filter")
	}
	if r := NewResampler(24000, 8000); r.lp == nil {
		t.Fatal("downsample 24k->8k must build an anti-aliasing filter")
	}
	if r := NewResampler(24000, 8000, WithoutAntiAlias()); r.lp != nil {
		t.Error("WithoutAntiAlias should disable the filter")
	}
}

func TestLowpassTapCountIsBoundedAndOdd(t *testing.T) {
	cases := []struct{ from, to int }{
		{24000, 8000}, {16000, 8000}, {48000, 8000}, {8000, 7999}, {1 << 20, 8000},
	}
	for _, c := range cases {
		f := newLowpass(c.from, c.to)
		if f == nil {
			t.Fatalf("%d->%d: expected a filter", c.from, c.to)
		}
		n := len(f.taps)
		if n < minLowpassTaps || n > maxLowpassTaps {
			t.Errorf("%d->%d: %d taps, want within [%d, %d]", c.from, c.to, n, minLowpassTaps, maxLowpassTaps)
		}
		if n%2 == 0 {
			t.Errorf("%d->%d: %d taps is even; linear phase needs an odd count", c.from, c.to, n)
		}
		if len(f.delay) != n-1 {
			t.Errorf("%d->%d: delay line = %d, want %d", c.from, c.to, len(f.delay), n-1)
		}
	}
}

// Unity DC gain: the filter must not change the signal's level.
func TestLowpassTapsAreNormalized(t *testing.T) {
	f := newLowpass(24000, 8000)
	sum := 0.0
	for _, tap := range f.taps {
		sum += tap
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("tap sum = %.12f, want 1 (unity gain at DC)", sum)
	}
}

func BenchmarkOutputTranscode24kTo8k(b *testing.B) {
	// One 20ms chunk at 24kHz, PCM16LE -- the real per-message workload.
	chunk := make([]byte, 480*2)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	t := NewOutputTranscoder(24000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		t.Transcode(chunk)
	}
}
