package call

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	pionrtp "github.com/pion/rtp"

	relayrtp "sip-relay/internal/rtp"
)

// The media clock is the ratio of RTP timestamp span to wall-clock time.
// The receiving endpoint plays out on the RTP timestamp clock, so a ratio
// below 1.0 means the relay fills its jitter buffer slower than the endpoint
// drains it: it underruns, conceals the shortfall, and the caller hears
// chopped words. Before the outbound rewrite this measured 0.945 with
// frame-aligned chunks and 0.789 with unaligned ones.
//
// The tolerance is deliberately tight. Loose bounds here would pass the very
// regression these tests exist to catch.
const (
	mediaClockMin = 0.99
	mediaClockMax = 1.01
)

type capturedPacket struct {
	size   int
	marker bool
	ts     uint32
	silent bool
}

type capture struct {
	packets []capturedPacket
	wall    time.Duration
}

// mediaRatio is RTP timestamp span divided by elapsed wall clock, both in
// milliseconds. 1.0 is real time.
func (c capture) mediaRatio() float64 {
	if len(c.packets) < 2 || c.wall <= 0 {
		return 0
	}
	tsSpan := c.packets[len(c.packets)-1].ts - c.packets[0].ts
	return (float64(tsSpan) / float64(relayrtp.SampleRate/1000)) / (float64(c.wall) / float64(time.Millisecond))
}

func (c capture) runts() int {
	n := 0
	for _, p := range c.packets {
		if p.size != relayrtp.SamplesPerFrame {
			n++
		}
	}
	return n
}

// captureOutbound drives the real outboundRTPWriter and a real rtp.Port,
// feeding chunkBytes of audio every `every` for `dur`, and records every RTP
// packet that actually reaches the wire.
func captureOutbound(t *testing.T, chunkBytes int, every, dur time.Duration) capture {
	t.Helper()

	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	port, err := relayrtp.Listen(relayrtp.Config{
		ListenIP:            "127.0.0.1",
		PortMin:             20000,
		PortMax:             40000,
		PayloadType:         0,
		MediaTimeoutInitial: time.Minute,
		MediaTimeout:        time.Minute,
		Log:                 slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer port.Close()
	port.SetRemote(sink.LocalAddr().(*net.UDPAddr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var out capture
	var firstAt, lastAt time.Time
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for {
			_ = sink.SetReadDeadline(time.Now().Add(dur + 2*time.Second))
			n, _, err := sink.ReadFromUDP(buf)
			if err != nil {
				return
			}
			now := time.Now()
			var p pionrtp.Packet
			if p.Unmarshal(buf[:n]) != nil {
				continue
			}
			silent := true
			for _, b := range p.Payload {
				if b != relayrtp.SilenceByte {
					silent = false
					break
				}
			}
			if firstAt.IsZero() {
				firstAt = now
			}
			lastAt = now
			out.packets = append(out.packets, capturedPacket{
				size: len(p.Payload), marker: p.Header.Marker,
				ts: p.Header.Timestamp, silent: silent,
			})
		}
	}()

	w := newOutboundRTPWriter(ctx, port, slog.New(slog.DiscardHandler), &mediaStats{})

	// Feed at exactly real time, the way a live TTS backend streams.
	chunk := make([]byte, chunkBytes)
	for i := range chunk {
		chunk[i] = 0x10 // anything but SilenceByte
	}
	deadline := time.Now().Add(dur)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for time.Now().Before(deadline) {
		<-tick.C
		if !w.Enqueue(chunk) {
			break
		}
	}

	cancel()
	_ = sink.SetReadDeadline(time.Now())
	<-done
	out.wall = lastAt.Sub(firstAt)
	return out
}

func checkMediaClock(t *testing.T, name string, c capture) {
	t.Helper()
	ratio := c.mediaRatio()
	t.Logf("%s: %d packets, media clock ratio %.4f, %d runt packets",
		name, len(c.packets), ratio, c.runts())
	if ratio < mediaClockMin || ratio > mediaClockMax {
		t.Errorf("%s: media clock ratio = %.4f, want within [%.2f, %.2f]. "+
			"Below 1.0 starves the endpoint's jitter buffer and chops words.",
			name, ratio, mediaClockMin, mediaClockMax)
	}
}

// The frame-aligned case isolates pacer drift: every chunk is exactly one
// frame, so nothing but the clock itself can go wrong.
func TestMediaClockHoldsRealTimeAligned(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	c := captureOutbound(t, relayrtp.SamplesPerFrame, relayrtp.FrameDuration, 3*time.Second)
	checkMediaClock(t, "frame-aligned", c)
	if got := c.runts(); got != 0 {
		t.Errorf("frame-aligned: %d packets were not exactly %d bytes", got, relayrtp.SamplesPerFrame)
	}
}

// 533 bytes every 66ms is what a 24kHz->8kHz resampler actually produces:
// chunk lengths that are never a multiple of the frame size. Splitting on
// those boundaries emitted a short packet per chunk, each consuming a full
// frame interval while advancing the RTP clock by a fraction of one.
func TestMediaClockHoldsRealTimeUnaligned(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	c := captureOutbound(t, 533, 66*time.Millisecond, 3*time.Second)
	checkMediaClock(t, "unaligned", c)
	if got := c.runts(); got != 0 {
		t.Errorf("unaligned: %d runt packets; backend chunk boundaries must never reach the wire", got)
	}
}

func TestOutboundMediaNeverSetsMarkerBit(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	c := captureOutbound(t, 533, 66*time.Millisecond, time.Second)
	for i, p := range c.packets {
		if p.marker {
			t.Fatalf("packet %d set the marker bit; receivers flush their jitter buffer on it", i)
		}
	}
}

// Continuous real-time delivery must play as continuous audio. Comfort noise
// appearing mid-stream is the audible stutter this rewrite removes.
func TestNoCommonNoiseInterleavedDuringContinuousAudio(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	c := captureOutbound(t, relayrtp.SamplesPerFrame, relayrtp.FrameDuration, 3*time.Second)

	// Skip the leading cushion, then require an unbroken run of real audio.
	first := -1
	for i, p := range c.packets {
		if !p.silent {
			first = i
			break
		}
	}
	if first < 0 {
		t.Fatal("no real audio was ever emitted")
	}
	// Ignore the tail, where the feed has stopped and comfort noise is correct.
	for i := first; i < len(c.packets)-3; i++ {
		if c.packets[i].silent {
			t.Fatalf("comfort noise at packet %d interrupted continuous real audio", i)
		}
	}
}

func TestBargeInDropsBufferedAudioPromptly(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}

	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	port, err := relayrtp.Listen(relayrtp.Config{
		ListenIP: "127.0.0.1", PortMin: 20000, PortMax: 40000, PayloadType: 0,
		MediaTimeoutInitial: time.Minute, MediaTimeout: time.Minute,
		Log: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer port.Close()
	port.SetRemote(sink.LocalAddr().(*net.UDPAddr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newOutboundRTPWriter(ctx, port, slog.New(slog.DiscardHandler), &mediaStats{})

	// A full second of audio buffered ahead -- the bot mid-utterance.
	audio := make([]byte, relayrtp.SampleRate)
	for i := range audio {
		audio[i] = 0x10
	}
	if !w.Enqueue(audio) {
		t.Fatal("Enqueue returned false")
	}
	time.Sleep(60 * time.Millisecond)
	if !w.Interrupt() {
		t.Fatal("Interrupt returned false")
	}

	// Everything buffered must be discarded, so the stream falls back to
	// comfort noise within a couple of frames rather than talking over the
	// caller for the remaining ~900ms.
	deadline := time.Now().Add(500 * time.Millisecond)
	buf := make([]byte, 2048)
	silentRun := 0
	for time.Now().Before(deadline) {
		_ = sink.SetReadDeadline(deadline)
		n, _, err := sink.ReadFromUDP(buf)
		if err != nil {
			break
		}
		var p pionrtp.Packet
		if p.Unmarshal(buf[:n]) != nil {
			continue
		}
		silent := true
		for _, b := range p.Payload {
			if b != relayrtp.SilenceByte {
				silent = false
				break
			}
		}
		if silent {
			silentRun++
			if silentRun >= 5 {
				return // barge-in took effect
			}
		} else {
			silentRun = 0
		}
	}
	t.Fatal("outbound stream never returned to comfort noise after barge-in")
}
