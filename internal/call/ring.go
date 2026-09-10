package call

// byteRing is a fixed-capacity circular buffer of PCMU bytes: the outbound
// playout buffer sitting between the backend (which delivers audio in
// arbitrarily sized, bursty chunks) and the RTP pacer (which must emit
// exactly one fixed-size frame every frame interval).
//
// Holding bytes rather than whole chunks is the point. Backend chunk
// boundaries fall wherever the backend's encoder or the resampler happens to
// put them, essentially never on a frame boundary; buffering at byte
// granularity lets the pacer cut frames on its own schedule so those
// boundaries never reach the wire as short RTP packets.
//
// Not safe for concurrent use: it is owned exclusively by the
// outboundRTPWriter run loop.
type byteRing struct {
	buf  []byte
	head int // index of the oldest buffered byte
	size int // number of buffered bytes
}

func newByteRing(capacity int) *byteRing {
	if capacity <= 0 {
		capacity = 1
	}
	return &byteRing{buf: make([]byte, capacity)}
}

// Len reports how many bytes are currently buffered.
func (r *byteRing) Len() int { return r.size }

// Cap reports the buffer's fixed capacity.
func (r *byteRing) Cap() int { return len(r.buf) }

// Write copies as much of p as fits and returns the number of bytes
// accepted. A short return means the buffer overflowed and the remainder of
// p was dropped; callers decide how to account for that. Nothing already
// buffered is ever evicted -- audio that arrived earlier is closer to being
// played, so dropping the newest bytes loses the least.
func (r *byteRing) Write(p []byte) int {
	n := len(p)
	if free := len(r.buf) - r.size; n > free {
		n = free
	}
	for i := 0; i < n; i++ {
		r.buf[(r.head+r.size+i)%len(r.buf)] = p[i]
	}
	r.size += n
	return n
}

// Read fills dst with up to len(dst) buffered bytes and returns how many it
// copied, consuming them. A return of less than len(dst) (including zero) is
// normal underrun, not an error: the caller pads the rest of the frame.
func (r *byteRing) Read(dst []byte) int {
	n := len(dst)
	if n > r.size {
		n = r.size
	}
	for i := 0; i < n; i++ {
		dst[i] = r.buf[(r.head+i)%len(r.buf)]
	}
	r.head = (r.head + n) % len(r.buf)
	r.size -= n
	return n
}

// Reset discards everything buffered. Used on barge-in, where audio the bot
// is no longer saying must not reach the caller.
func (r *byteRing) Reset() {
	r.head = 0
	r.size = 0
}
