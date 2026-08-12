// Package audio implements the PCM transcoding primitives used to bridge
// PCMU/G.711 mu-law (the SIP/RTP side's only supported codec) with the raw
// 16-bit linear PCM some WebSocket telephony backends require, at whatever
// sample rate that backend expects.
package audio

const (
	// mulawBias and mulawClip are the standard ITU-T G.711 mu-law companding
	// constants operating directly on the full 16-bit linear domain (no
	// pre-scaling), so decoded samples span close to the full int16 range.
	mulawBias = 0x84
	mulawClip = 32635
)

var mulawDecodeTable [256]int16

// mulawEncodeTable maps every possible int16 sample (indexed by its bit
// pattern via uint16(sample)) to the mu-law byte whose decoded value is
// closest to it. Building it as the exact inverse of mulawDecodeTable
// guarantees encode/decode are consistent by construction, rather than
// relying on a second, independently hand-derived formula.
var mulawEncodeTable [65536]byte

func init() {
	for i := range mulawDecodeTable {
		mulawDecodeTable[i] = decodeMulawByte(byte(i))
	}
	for sample := -32768; sample <= 32767; sample++ {
		var best byte
		bestDiff := int32(1 << 30)
		for b := 0; b < 256; b++ {
			diff := int32(mulawDecodeTable[b]) - int32(sample)
			if diff < 0 {
				diff = -diff
			}
			if diff < bestDiff {
				bestDiff = diff
				best = byte(b)
			}
		}
		mulawEncodeTable[uint16(int16(sample))] = best
	}
}

func decodeMulawByte(mu byte) int16 {
	mu = ^mu
	sign := mu & 0x80
	exponent := (mu >> 4) & 0x07
	mantissa := mu & 0x0F
	magnitude := (int32(mantissa)<<3+mulawBias)<<exponent - mulawBias
	if magnitude > mulawClip {
		magnitude = mulawClip
	}
	if sign != 0 {
		return int16(-magnitude)
	}
	return int16(magnitude)
}

// MulawToLinear decodes one G.711 mu-law byte to a 16-bit signed linear PCM sample.
func MulawToLinear(mu byte) int16 {
	return mulawDecodeTable[mu]
}

// LinearToMulaw encodes one 16-bit signed linear PCM sample to a G.711 mu-law byte.
func LinearToMulaw(sample int16) byte {
	return mulawEncodeTable[uint16(sample)]
}

// DecodeMulaw appends the linear PCM decode of src to dst and returns the extended slice.
func DecodeMulaw(dst []int16, src []byte) []int16 {
	for _, b := range src {
		dst = append(dst, mulawDecodeTable[b])
	}
	return dst
}

// EncodeMulaw appends the mu-law encode of src to dst and returns the extended slice.
func EncodeMulaw(dst []byte, src []int16) []byte {
	for _, s := range src {
		dst = append(dst, mulawEncodeTable[uint16(s)])
	}
	return dst
}
