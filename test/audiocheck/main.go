// Command audiocheck renders the backend->caller downsampling path to
// playable WAV files, with and without the anti-aliasing filter, so the
// difference can be heard rather than only asserted in dB.
//
// The default probe is a logarithmic sweep from 100Hz to 11kHz at 24kHz. It
// makes aliasing unmistakable: once the sweep climbs past the 4kHz Nyquist of
// the 8kHz output, unfiltered decimation folds it back down, so before.wav
// contains a descending ghost tone crossing the ascending sweep. after.wav
// should simply go quiet above 3.4kHz, the way a telephone does.
//
// Pass -in to run real audio through instead (raw mono PCM16LE at the input
// rate), which is the better test for the gritty, metallic edge on sibilants
// that motivated the filter.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	"sip-relay/internal/audio"
)

func main() {
	var (
		in      = flag.String("in", "", "raw mono PCM16LE input file at -src-rate (default: synthesized sweep)")
		outDir  = flag.String("out-dir", ".", "directory to write before.wav / after.wav / source.wav into")
		seconds = flag.Float64("seconds", 6, "length of the synthesized sweep")
		srcRate = flag.Int("src-rate", 24000, "input sample rate")
		dstRate = flag.Int("dst-rate", 8000, "output (telephony) sample rate")
		sweepLo = flag.Float64("sweep-lo", 100, "sweep start frequency")
		sweepHi = flag.Float64("sweep-hi", 11000, "sweep end frequency")
	)
	flag.Parse()

	var src []int16
	var err error
	if *in != "" {
		src, err = readPCM16LE(*in)
		if err != nil {
			log.Fatalf("read %s: %v", *in, err)
		}
		fmt.Printf("input: %s (%d samples, %.2fs at %dHz)\n", *in, len(src), float64(len(src))/float64(*srcRate), *srcRate)
	} else {
		src = logSweep(*seconds, float64(*srcRate), *sweepLo, *sweepHi, 12000)
		fmt.Printf("input: synthesized %.0fHz->%.0fHz sweep (%.1fs at %dHz)\n", *sweepLo, *sweepHi, *seconds, *srcRate)
	}
	if len(src) == 0 {
		log.Fatal("no input samples")
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("create %s: %v", *outDir, err)
	}

	// Both paths run the full production conversion -- resample, mu-law
	// encode -- then decode back so the result is playable. The only
	// difference between them is the anti-aliasing filter.
	before := telephonyRoundTrip(src, *srcRate, *dstRate, audio.WithoutAntiAlias())
	after := telephonyRoundTrip(src, *srcRate, *dstRate)

	files := []struct {
		name    string
		samples []int16
		rate    int
		desc    string
	}{
		{"source.wav", src, *srcRate, "original input"},
		{"before.wav", before, *dstRate, "no anti-aliasing (what shipped before)"},
		{"after.wav", after, *dstRate, "anti-aliased (current)"},
	}
	for _, f := range files {
		path := filepath.Join(*outDir, f.name)
		if err := writeWAV(path, f.samples, f.rate); err != nil {
			log.Fatalf("write %s: %v", path, err)
		}
		fmt.Printf("wrote %-12s %6d samples @ %5dHz  %s\n", path, len(f.samples), f.rate, f.desc)
	}

	fmt.Println("\nListen to before.wav and after.wav.")
	if *in == "" {
		fmt.Println("On the sweep: before.wav has a descending ghost tone crossing the rising sweep")
		fmt.Println("(content above 4kHz folding back down). after.wav should just fade out above ~3.4kHz.")
	}
}

// telephonyRoundTrip runs the real backend->caller conversion and decodes the
// result back to linear PCM so it can be played.
func telephonyRoundTrip(src []int16, srcRate, dstRate int, opts ...audio.ResamplerOption) []int16 {
	r := audio.NewResampler(srcRate, dstRate, opts...)
	resampled := r.Process(nil, src)
	// Flush the interpolator's held-back trailing context.
	resampled = r.Process(resampled, make([]int16, 4*audio.InterpolationOrder))
	mulaw := audio.EncodeMulaw(nil, resampled)
	return audio.DecodeMulaw(nil, mulaw)
}

// logSweep generates a logarithmic frequency sweep, with short fades so the
// ends do not click.
func logSweep(seconds, sampleHz, fromHz, toHz, amplitude float64) []int16 {
	n := int(seconds * sampleHz)
	if n <= 0 {
		return nil
	}
	out := make([]int16, n)
	ratio := toHz / fromHz
	// Instantaneous frequency f(t) = fromHz * ratio^(t/T); its integral gives
	// the phase.
	k := seconds / math.Log(ratio)
	fade := int(0.01 * sampleHz) // 10ms
	for i := range out {
		t := float64(i) / sampleHz
		phase := 2 * math.Pi * fromHz * k * (math.Pow(ratio, t/seconds) - 1)
		a := amplitude
		if i < fade {
			a *= float64(i) / float64(fade)
		}
		if rem := n - 1 - i; rem < fade {
			a *= float64(rem) / float64(fade)
		}
		out[i] = int16(a * math.Sin(phase))
	}
	return out
}

func readPCM16LE(path string) ([]int16, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := make([]int16, len(raw)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(raw[2*i:]))
	}
	return out, nil
}

// writeWAV emits a 16-bit mono PCM RIFF/WAVE file. The standard library has
// no WAV support, and the header is only 44 bytes.
func writeWAV(path string, samples []int16, sampleRate int) error {
	const (
		headerSize    = 44
		bitsPerSample = 16
		numChannels   = 1
	)
	dataSize := len(samples) * 2
	buf := make([]byte, 0, headerSize+dataSize)

	byteRate := sampleRate * numChannels * bitsPerSample / 8
	blockAlign := numChannels * bitsPerSample / 8

	buf = append(buf, "RIFF"...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(36+dataSize))
	buf = append(buf, "WAVE"...)

	buf = append(buf, "fmt "...)
	buf = binary.LittleEndian.AppendUint32(buf, 16) // PCM fmt chunk size
	buf = binary.LittleEndian.AppendUint16(buf, 1)  // format: PCM
	buf = binary.LittleEndian.AppendUint16(buf, numChannels)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(sampleRate))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(byteRate))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(blockAlign))
	buf = binary.LittleEndian.AppendUint16(buf, bitsPerSample)

	buf = append(buf, "data"...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(dataSize))
	for _, s := range samples {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(s))
	}

	return os.WriteFile(path, buf, 0o644)
}
