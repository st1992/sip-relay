package call

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	pionrtp "github.com/pion/rtp"

	"sip-relay/internal/backend"
	relayrtp "sip-relay/internal/rtp"
)

func TestCloseBeforeStartClosesDone(t *testing.T) {
	c := New("call-1", Metadata{}, nil, nil, nil, nil, nil)

	c.Close()

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed call")
	}
}

func TestStartAfterCloseDoesNotReopenCall(t *testing.T) {
	c := New("call-1", Metadata{}, nil, nil, nil, nil, nil)
	c.Close()

	c.Start(context.Background())

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed call")
	}
}

func TestBackendEndReasonsHaveDistinctCallLogReasons(t *testing.T) {
	tests := []struct {
		reason EndReason
		want   string
	}{
		{EndReasonAgent, "AGENT_ENDED"},
		{EndReasonTransfer, "TRANSFERRED"},
		{EndReasonBackend, "BACKEND_ERROR"},
		{EndReasonUser, "USER_ENDED"},
	}
	for _, test := range tests {
		c := New("call-1", Metadata{}, nil, nil, nil, nil, nil)
		c.setEndReason(test.reason)
		if got := c.hangupReason(); got != test.want {
			t.Errorf("hangupReason(%s) = %q, want %q", test.reason, got, test.want)
		}
	}
}

func TestReceiveBackendTreatsClosedEventsAsFailure(t *testing.T) {
	events := make(chan backend.Event)
	close(events)
	stream := &fakeBackendStream{events: events}
	c := New("call-1", Metadata{}, nil, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.receiveBackend(ctx, stream, nil, &mediaStats{})
	if err == nil || err.Error() != "backend events channel closed unexpectedly" {
		t.Fatalf("receiveBackend() error = %v", err)
	}
}

func TestOutboundRTPWriterFillsUnderrunWithSilence(t *testing.T) {
	port, err := relayrtp.Listen(relayrtp.Config{
		ListenIP:            "127.0.0.1",
		PayloadType:         0,
		MediaTimeoutInitial: time.Second,
		MediaTimeout:        time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer port.Close()

	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	port.SetRemote(peer.LocalAddr().(*net.UDPAddr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := newOutboundRTPWriter(ctx, port, slog.Default(), &mediaStats{})

	// No backend audio has been enqueued: the outbound RTP stream must still
	// stay alive with paced silence instead of going quiet.
	first := readCallPacket(t, peer)
	second := readCallPacket(t, peer)
	if len(first.Payload) != relayrtp.SamplesPerFrame || first.Payload[0] != relayrtp.SilenceByte {
		t.Fatalf("expected silence payload while idle, got %v", first.Payload)
	}
	if second.SequenceNumber != first.SequenceNumber+1 {
		t.Fatalf("silence packets were not sent back to back: seq %d then %d", first.SequenceNumber, second.SequenceNumber)
	}

	// Below the minimum buffer threshold, the writer must keep withholding
	// playback (still silence) rather than draining prematurely.
	if !writer.Enqueue([]byte{1, 2, 3}) {
		t.Fatal("Enqueue returned false")
	}
	stillSilence := readCallPacket(t, peer)
	if stillSilence.Payload[0] != relayrtp.SilenceByte {
		t.Fatalf("expected continued silence below the buffer threshold, got %v", stillSilence.Payload)
	}

	// Crossing the threshold must make it drain, in order.
	rest := minOutboundBufferBytes - 3
	if !writer.Enqueue(bytes.Repeat([]byte{2}, rest)) {
		t.Fatal("Enqueue returned false")
	}
	firstReal := readCallPacket(t, peer)
	if string(firstReal.Payload[:3]) != string([]byte{1, 2, 3}) {
		t.Fatalf("expected the first queued chunk to play first once threshold reached, got %v", firstReal.Payload)
	}
}

func TestOutboundRTPWriterRebuffersAfterUnderrun(t *testing.T) {
	port, err := relayrtp.Listen(relayrtp.Config{
		ListenIP:            "127.0.0.1",
		PayloadType:         0,
		MediaTimeoutInitial: time.Second,
		MediaTimeout:        time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer port.Close()

	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	port.SetRemote(peer.LocalAddr().(*net.UDPAddr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := newOutboundRTPWriter(ctx, port, slog.Default(), &mediaStats{})

	// minOutboundBufferBytes divides evenly into SamplesPerFrame-sized RTP
	// packets, so enqueuing exactly that much plays as real audio with no
	// silence interleaved, then the queue empties (underrun).
	full := bytes.Repeat([]byte{1}, minOutboundBufferBytes)
	if !writer.Enqueue(full) {
		t.Fatal("Enqueue returned false")
	}
	for i := 0; i < minOutboundBufferBytes/relayrtp.SamplesPerFrame; i++ {
		pkt := readCallPacket(t, peer)
		if pkt.Payload[0] != 1 {
			t.Fatalf("packet %d: expected real audio content, got %v", i, pkt.Payload)
		}
	}

	// Immediately after the underrun, output must be silence.
	afterUnderrun := readCallPacket(t, peer)
	if afterUnderrun.Payload[0] != relayrtp.SilenceByte {
		t.Fatalf("expected silence immediately after underrun, got %v", afterUnderrun.Payload)
	}

	// A small chunk arriving after the underrun must NOT play immediately --
	// the writer should demand a fresh cushion again, not resume instantly.
	if !writer.Enqueue([]byte{9, 9, 9}) {
		t.Fatal("Enqueue returned false")
	}
	stillSilence := readCallPacket(t, peer)
	if stillSilence.Payload[0] != relayrtp.SilenceByte {
		t.Fatalf("expected continued silence after a post-underrun chunk below the buffer threshold, got %v", stillSilence.Payload)
	}
}

func TestHandleCommandGatesOnMinBufferBeforeDraining(t *testing.T) {
	w := &outboundRTPWriter{log: slog.Default(), port: &relayrtp.Port{}, buffering: true}

	var queue [][]byte
	queue = w.handleCommand(queue, outboundRTPCommand{audio: make([]byte, minOutboundBufferBytes-1)})
	if !w.buffering {
		t.Fatal("buffering should remain true below the minimum buffer threshold")
	}

	queue = w.handleCommand(queue, outboundRTPCommand{audio: []byte{0}})
	if w.buffering {
		t.Fatal("buffering should clear once the minimum buffer threshold is reached")
	}
	if len(queue) != 2 {
		t.Fatalf("queue len = %d, want 2", len(queue))
	}
}

func TestHandleCommandInterruptRearmsBuffering(t *testing.T) {
	w := &outboundRTPWriter{log: slog.Default(), port: &relayrtp.Port{}, buffering: false, queuedBytes: 10}
	queue := [][]byte{make([]byte, 10)}

	queue = w.handleCommand(queue, outboundRTPCommand{interrupt: true})
	if !w.buffering {
		t.Fatal("interrupt should re-arm buffering so playback resumes with a fresh cushion")
	}
	if len(queue) != 0 || w.queuedBytes != 0 {
		t.Fatalf("interrupt should reset queue and queuedBytes, got len=%d queuedBytes=%d", len(queue), w.queuedBytes)
	}
}

func TestOutboundRTPWriterDropsAudioBeyondQueueCap(t *testing.T) {
	stats := &mediaStats{}
	w := &outboundRTPWriter{log: slog.Default(), stats: stats, port: &relayrtp.Port{}}

	chunk := make([]byte, maxQueuedOutboundBytes/2)
	var queue [][]byte
	queue = w.handleCommand(queue, outboundRTPCommand{audio: chunk})
	queue = w.handleCommand(queue, outboundRTPCommand{audio: chunk})
	if len(queue) != 2 || w.queuedBytes != 2*len(chunk) {
		t.Fatalf("expected both chunks accepted within cap: queue len = %d, queuedBytes = %d", len(queue), w.queuedBytes)
	}

	// A backend that keeps producing audio faster than real-time must have
	// the overflow dropped, not buffered without bound.
	queue = w.handleCommand(queue, outboundRTPCommand{audio: chunk})
	if len(queue) != 2 {
		t.Fatalf("expected chunk beyond cap to be dropped, queue len = %d", len(queue))
	}
	if got := stats.outboundAudioBytesDropped.Load(); got != uint64(len(chunk)) {
		t.Fatalf("outboundAudioBytesDropped = %d, want %d", got, len(chunk))
	}

	// Barge-in must still clear the queue and let audio flow again afterward.
	queue = w.handleCommand(queue, outboundRTPCommand{interrupt: true})
	if len(queue) != 0 || w.queuedBytes != 0 {
		t.Fatalf("interrupt should reset queue and queuedBytes, got len=%d queuedBytes=%d", len(queue), w.queuedBytes)
	}
	queue = w.handleCommand(queue, outboundRTPCommand{audio: chunk})
	if len(queue) != 1 {
		t.Fatalf("expected audio to be accepted again after interrupt reset, queue len = %d", len(queue))
	}
}

func readCallPacket(t *testing.T, conn *net.UDPConn) pionrtp.Packet {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	var packet pionrtp.Packet
	if err := packet.Unmarshal(buf[:n]); err != nil {
		t.Fatal(err)
	}
	return packet
}

type fakeBackendStream struct {
	events <-chan backend.Event
}

func (s *fakeBackendStream) Input() chan<- []byte         { return nil }
func (s *fakeBackendStream) Events() <-chan backend.Event { return s.events }
func (s *fakeBackendStream) Done() <-chan error           { return nil }
func (s *fakeBackendStream) Close() error                 { return nil }
