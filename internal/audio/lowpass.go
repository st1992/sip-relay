package audio

import "math"

// lowpass is a streaming linear-phase FIR low-pass filter, used as the
// anti-aliasing stage ahead of decimation.
//
// Interpolation alone cannot resample correctly when the rate goes down.
// Everything in the input above the output's Nyquist frequency has nowhere to
// go: instead of disappearing it folds back into the audible band (resampling
// 24kHz to 8kHz maps 6kHz onto 2kHz, 10kHz onto 2kHz, and so on). Speech
// carries real energy up there -- sibilants and fricatives especially -- so the
// fold-down is broadband, speech-correlated, and audible as a gritty, metallic
// edge. Removing that energy before the rate changes is the only fix; nothing
// downstream can separate it from the wanted signal afterwards.
//
// The filter emits exactly one sample per input sample, so it changes the
// content of the stream handed to the interpolator but never its length. Its
// delay line carries across calls, so output does not depend on how the input
// was chunked.
//
// Not safe for concurrent use; it belongs to one Resampler.
type lowpass struct {
	taps  []float64
	delay []int16 // the len(taps)-1 most recent input samples from prior calls
	// scratch and ext are reused across calls: process runs once per backend
	// audio chunk, dozens of times a second for the life of every call.
	scratch []int16
	ext     []int16
}

// Filter geometry, as fractions of the output sample rate.
const (
	// lowpassCutoffRatio puts the cutoff at 0.425 of the output rate --
	// 3400Hz for an 8kHz output, which is the band a real telephony codec
	// passes anyway.
	lowpassCutoffRatio = 0.425
	// lowpassStopRatio is where the stopband must be reached: the output
	// Nyquist. Nothing may arrive there with meaningful energy.
	lowpassStopRatio = 0.5
	// hammingTransitionWidth is the approximate normalized transition width
	// of a Hamming-windowed sinc, in tap-count terms: taps ~= this / width.
	// A Hamming window reaches about -53dB of stopband attenuation, well
	// below the noise floor of any speech signal.
	hammingTransitionWidth = 3.3

	minLowpassTaps = 31
	// maxLowpassTaps bounds the filter for pathological configuration:
	// transcode sample_rate is validated only as "positive", so an absurd
	// input rate would otherwise ask for an enormous filter.
	maxLowpassTaps = 255
)

// newLowpass builds the anti-aliasing filter for a fromHz->toHz conversion,
// or returns nil when no filtering is needed. Only downsampling can alias;
// at or above the input rate there is nothing to fold, so the conversion is
// left exactly as it was.
func newLowpass(fromHz, toHz int) *lowpass {
	if fromHz <= toHz || fromHz <= 0 || toHz <= 0 {
		return nil
	}

	// Transition band, normalized to the input rate the filter runs at.
	width := (lowpassStopRatio - lowpassCutoffRatio) * float64(toHz) / float64(fromHz)
	n := minLowpassTaps
	if width > 0 {
		n = int(math.Ceil(hammingTransitionWidth / width))
	}
	if n < minLowpassTaps {
		n = minLowpassTaps
	}
	if n > maxLowpassTaps {
		n = maxLowpassTaps
	}
	if n%2 == 0 {
		n++ // odd tap count keeps the filter linear-phase about a whole sample
	}

	cutoff := lowpassCutoffRatio * float64(toHz) / float64(fromHz)
	return &lowpass{
		taps:  hammingSincTaps(n, cutoff),
		delay: make([]int16, n-1),
	}
}

// hammingSincTaps returns n Hamming-windowed sinc coefficients for the given
// cutoff, expressed in cycles per sample. The result is normalized to unity
// gain at DC so the filter neither boosts nor attenuates the passband.
func hammingSincTaps(n int, cutoff float64) []float64 {
	taps := make([]float64, n)
	mid := float64(n-1) / 2
	sum := 0.0
	for i := range taps {
		x := float64(i) - mid
		// Ideal brick-wall impulse response, sampled.
		var sinc float64
		if x == 0 {
			sinc = 2 * cutoff
		} else {
			sinc = math.Sin(2*math.Pi*cutoff*x) / (math.Pi * x)
		}
		// Truncating that infinite response ripples badly; the window tapers
		// the ends to keep the stopband flat.
		window := 0.54 - 0.46*math.Cos(2*math.Pi*float64(i)/float64(n-1))
		taps[i] = sinc * window
		sum += taps[i]
	}
	for i := range taps {
		taps[i] /= sum
	}
	return taps
}

// process filters in and returns exactly len(in) samples. The returned slice
// is reused on the next call, so callers must consume it before calling
// again -- the Resampler does, immediately.
func (f *lowpass) process(in []int16) []int16 {
	if f == nil || len(in) == 0 {
		return in
	}
	n := len(f.taps)

	// The filter needs the n-1 samples preceding each output, which for the
	// front of this chunk live in the previous one.
	need := len(f.delay) + len(in)
	if cap(f.ext) < need {
		f.ext = make([]int16, need)
	}
	ext := f.ext[:0]
	ext = append(ext, f.delay...)
	ext = append(ext, in...)

	if cap(f.scratch) < len(in) {
		f.scratch = make([]int16, len(in))
	}
	out := f.scratch[:len(in)]

	// The taps are symmetric (that is what makes the filter linear-phase), so
	// each pair h[k] == h[n-1-k] shares one multiply across the two samples it
	// weights, halving the work.
	half := (n - 1) / 2
	for i := range out {
		acc := 0.0
		for k := 0; k < half; k++ {
			// ext[i+n-1] is the newest sample in this output's window.
			acc += f.taps[k] * (float64(ext[i+n-1-k]) + float64(ext[i+k]))
		}
		acc += f.taps[half] * float64(ext[i+half])
		out[i] = clampToInt16(acc)
	}

	copy(f.delay, ext[len(ext)-len(f.delay):])
	return out
}

// reset clears the delay line, as if newly constructed.
func (f *lowpass) reset() {
	if f == nil {
		return
	}
	for i := range f.delay {
		f.delay[i] = 0
	}
}
