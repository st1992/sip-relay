package call

import (
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

	if !writer.Enqueue([]byte{1, 2, 3}) {
		t.Fatal("Enqueue returned false")
	}
	real := readCallPacket(t, peer)
	if string(real.Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("expected queued real audio to take priority, got %v", real.Payload)
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
