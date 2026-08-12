package audio

import "encoding/binary"

// SamplesToBytesLE appends the little-endian byte encoding of src to dst and
// returns the extended slice. Always exactly 2*len(src) bytes.
func SamplesToBytesLE(dst []byte, src []int16) []byte {
	for _, s := range src {
		dst = binary.LittleEndian.AppendUint16(dst, uint16(s))
	}
	return dst
}

// PCM16LEDecoder converts a stream of little-endian PCM16 bytes into int16
// samples, buffering a trailing odd byte across calls so a 16-bit sample is
// never dropped or misaligned when it straddles two calls (e.g. two separate
// WebSocket binary messages). Not safe for concurrent use; intended to be
// used by a single audio direction of a single call.
type PCM16LEDecoder struct {
	pending    byte
	hasPending bool
}

// Decode appends the samples decoded from any byte buffered from a previous
// call plus src to dst, and returns the extended slice. If the combined byte
// count is odd, the final byte is retained internally and consumed on the
// next call.
func (d *PCM16LEDecoder) Decode(dst []int16, src []byte) []int16 {
	i := 0
	if d.hasPending {
		if len(src) == 0 {
			return dst
		}
		dst = append(dst, int16(binary.LittleEndian.Uint16([]byte{d.pending, src[0]})))
		d.hasPending = false
		i = 1
	}
	for ; i+1 < len(src); i += 2 {
		dst = append(dst, int16(binary.LittleEndian.Uint16(src[i:i+2])))
	}
	if i < len(src) {
		d.pending = src[i]
		d.hasPending = true
	}
	return dst
}

// Reset clears any buffered odd byte, as if newly constructed.
func (d *PCM16LEDecoder) Reset() {
	d.hasPending = false
}
