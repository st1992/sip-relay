package audio

// telephonyRate is the fixed PCMU/RTP sample rate this relay's SIP side
// always uses (PCMU/8000 is the only codec the SDP negotiation accepts).
const telephonyRate = 8000

// InputTranscoder converts PCMU 8kHz mu-law bytes (as delivered once per RTP
// frame, always gapless -- internal/call pads caller silence with PCMU too)
// into little-endian PCM16 bytes at outputHz, for sending to a WebSocket
// backend that expects linear PCM. Construct exactly once per call's send
// direction and reuse for every chunk. Not safe for concurrent use.
type InputTranscoder struct {
	resampler *Resampler
}

// NewInputTranscoder builds an InputTranscoder resampling from the fixed
// telephony rate (8000Hz) to outputHz. outputHz must be positive.
func NewInputTranscoder(outputHz int) *InputTranscoder {
	return &InputTranscoder{resampler: NewResampler(telephonyRate, outputHz)}
}

// Transcode decodes mulawPCMU, resamples to the configured rate, and
// returns freshly allocated little-endian PCM16 bytes. May return an empty
// (non-nil) slice if not enough audio has accumulated yet to produce a
// resampled sample.
func (t *InputTranscoder) Transcode(mulawPCMU []byte) []byte {
	linear := DecodeMulaw(nil, mulawPCMU)
	resampled := t.resampler.Process(nil, linear)
	return SamplesToBytesLE(make([]byte, 0, 2*len(resampled)), resampled)
}

// OutputTranscoder converts little-endian PCM16 bytes at inputHz (as
// received in arbitrarily sized WebSocket binary messages, with gaps
// between bot utterances) back into PCMU 8kHz mu-law bytes for RTP playout
// and recording. Construct exactly once per call's receive direction and
// reuse for every message. Not safe for concurrent use.
type OutputTranscoder struct {
	decoder   PCM16LEDecoder
	resampler *Resampler
}

// NewOutputTranscoder builds an OutputTranscoder resampling from inputHz
// down to the fixed telephony rate (8000Hz). inputHz must be positive.
func NewOutputTranscoder(inputHz int) *OutputTranscoder {
	return &OutputTranscoder{resampler: NewResampler(inputHz, telephonyRate)}
}

// Transcode buffers any trailing odd byte left over from a previous call,
// decodes the resulting PCM16LE bytes, resamples to 8000Hz, mu-law encodes,
// and returns freshly allocated PCMU 8kHz bytes. May return an empty
// (non-nil) slice, e.g. when pcm16LE contributes only a leftover odd byte or
// not enough samples to produce resampled output yet -- callers should treat
// that as "nothing to emit this call", not an error.
func (t *OutputTranscoder) Transcode(pcm16LE []byte) []byte {
	samples := t.decoder.Decode(nil, pcm16LE)
	resampled := t.resampler.Process(nil, samples)
	return EncodeMulaw(make([]byte, 0, len(resampled)), resampled)
}
