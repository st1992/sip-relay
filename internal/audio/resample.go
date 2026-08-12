package audio

import "math"

// InterpolationOrder is the number of input samples used on each side of the
// interpolation point (an 8-sample Lagrange window for the default order 4),
// matching the pure-Go resampler LiveKit's SIP media pipeline falls back to
// when not built with cgo.
const InterpolationOrder = 4

// Resampler performs streaming mono 16-bit PCM sample-rate conversion via
// Lagrange polynomial interpolation. It keeps a small history of trailing
// input samples and a fractional phase position across calls, so feeding
// audio in arbitrarily sized chunks (even one sample at a time) produces the
// same output as a single large call would -- no discontinuities/clicks at
// chunk boundaries. Supports arbitrary (non-integer-ratio) sample rates.
//
// Because interpolation needs InterpolationOrder samples of future context,
// output for the trailing InterpolationOrder-ish input samples of a call is
// held back until more input arrives; those samples are only ever flushed by
// a subsequent Process call. At true end-of-stream this leaves a fixed,
// sub-millisecond amount of audio never emitted -- an accepted, standard
// resampler tradeoff, not a bug.
//
// Not safe for concurrent use; intended to live for exactly one call's one
// audio direction and never be reset mid-stream.
type Resampler struct {
	ratio   float64 // input samples consumed per output sample (fromHz/toHz)
	history []int16 // trailing samples retained from the previous Process call
	frac    float64 // fractional position, within (history ++ next input), of the next output sample
}

// NewResampler constructs a Resampler converting mono PCM16 from fromHz to
// toHz. Both must be positive. fromHz == toHz is legal and reproduces the
// input exactly (subject to the trailing-context latency described above).
func NewResampler(fromHz, toHz int) *Resampler {
	return &Resampler{ratio: float64(fromHz) / float64(toHz)}
}

// Reset clears history and phase state as if newly constructed. For
// instance reuse across unrelated streams -- never call mid-stream.
func (r *Resampler) Reset() {
	r.history = nil
	r.frac = 0
}

// Process appends the resampled output for in to dst and returns the
// extended slice. May be called repeatedly for the same logical stream,
// including with a nil/empty in; state carries across calls. Output samples
// are clamped to [-32768, 32767] since Lagrange interpolation can overshoot
// near sharp transitions.
func (r *Resampler) Process(dst []int16, in []int16) []int16 {
	if len(in) == 0 {
		return dst
	}

	const n = InterpolationOrder
	histLen := len(r.history)
	window := make([]int16, histLen+len(in))
	copy(window, r.history)
	copy(window[histLen:], in)

	lastValidBase := len(window) - 1 - n
	for int(math.Floor(r.frac)) <= lastValidBase {
		base := math.Floor(r.frac)
		bi := int(base)
		t := r.frac - base

		var acc float64
		for k := -n + 1; k <= n; k++ {
			idx := bi + k
			var xv int16
			if idx >= 0 && idx < len(window) {
				xv = window[idx]
			}
			acc += lagrangeWeight(k, t, n) * float64(xv)
		}
		dst = append(dst, clampToInt16(acc))
		r.frac += r.ratio
	}

	keep := 2*n - 1
	if keep > len(window) {
		keep = len(window)
	}
	r.frac -= float64(len(window) - keep)
	newHistory := make([]int16, keep)
	copy(newHistory, window[len(window)-keep:])
	r.history = newHistory

	return dst
}

// lagrangeWeight evaluates the Lagrange basis polynomial for node k, among
// the 2*n nodes {-n+1, ..., n}, at fractional offset t (0 <= t < 1) from node 0.
func lagrangeWeight(k int, t float64, n int) float64 {
	weight := 1.0
	for m := -n + 1; m <= n; m++ {
		if m == k {
			continue
		}
		weight *= (t - float64(m)) / float64(k-m)
	}
	return weight
}

func clampToInt16(v float64) int16 {
	switch {
	case v > 32767:
		return 32767
	case v < -32768:
		return -32768
	default:
		return int16(math.Round(v))
	}
}
