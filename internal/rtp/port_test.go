package rtp

import (
	"context"
	"net"
	"testing"
	"time"

	pionrtp "github.com/pion/rtp"
)

func TestPortForwardsPayloadBytesOnly(t *testing.T) {
	port, err := Listen(Config{
		ListenIP:            "127.0.0.1",
		PayloadType:         0,
		SymmetricRTP:        true,
		OutboundFrameBytes:  160,
		MediaTimeoutInitial: time.Second,
		MediaTimeout:        time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer port.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = port.Run(ctx)
	}()

	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	payload := []byte{0x10, 0x11, 0x12, 0x13}
	packet := pionrtp.Packet{
		Header:  pionrtp.Header{Version: 2, PayloadType: 0, SequenceNumber: 9, Timestamp: 1234, SSRC: 55},
		Payload: payload,
	}
	raw, err := packet.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	_, err = peer.WriteToUDP(raw, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port.LocalPort()})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-port.Payloads():
		if string(got) != string(payload) {
			t.Fatalf("payload = %v, want %v", got, payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for payload")
	}
}

func TestPortWriteFrameUsesFixedTimestampIncrement(t *testing.T) {
	port, peer := writeFramePortAndPeer(t, 3)

	if err := port.WriteFrame([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	// Deliberately short: the caller is expected to pad, but even if it does
	// not, the timestamp must still advance by a whole frame. Deriving the
	// increment from len(payload) is what let short frames advance the media
	// clock by less than the wall-clock time they occupy.
	if err := port.WriteFrame([]byte{4}); err != nil {
		t.Fatal(err)
	}
	if err := port.WriteFrame([]byte{5, 6, 7}); err != nil {
		t.Fatal(err)
	}

	first := readPacket(t, peer)
	second := readPacket(t, peer)
	third := readPacket(t, peer)

	if string(first.Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("first payload = %v, want the frame handed in unsplit", first.Payload)
	}
	if second.SequenceNumber != first.SequenceNumber+1 || third.SequenceNumber != second.SequenceNumber+1 {
		t.Fatalf("sequence did not increment by one per frame: %d, %d, %d",
			first.SequenceNumber, second.SequenceNumber, third.SequenceNumber)
	}
	if got := second.Timestamp - first.Timestamp; got != 3 {
		t.Fatalf("timestamp increment = %d, want the fixed frame size 3", got)
	}
	if got := third.Timestamp - second.Timestamp; got != 3 {
		t.Fatalf("timestamp increment after a short frame = %d, want the fixed frame size 3", got)
	}
}

func TestPortWriteFrameNeverSetsMarker(t *testing.T) {
	port, peer := writeFramePortAndPeer(t, 3)

	// The marker bit means "new talkspurt", and receivers commonly react by
	// resetting their jitter buffer and dropping what it holds. This Port
	// emits one unbroken stream with contiguous timestamps, so there is never
	// a talkspurt boundary to signal -- not on the first frame, not after
	// comfort noise.
	frames := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
		{SilenceByte, SilenceByte, SilenceByte},
		{7, 8, 9},
	}
	for _, f := range frames {
		if err := port.WriteFrame(f); err != nil {
			t.Fatal(err)
		}
	}
	for i := range frames {
		if pkt := readPacket(t, peer); pkt.Marker {
			t.Fatalf("packet %d set the marker bit; media must never signal a talkspurt start", i)
		}
	}
}

func TestPortWriteFrameDoesNotSplitOrPace(t *testing.T) {
	port, peer := writeFramePortAndPeer(t, SamplesPerFrame)

	// One call, one packet, sent immediately: pacing belongs to the caller,
	// which owns the single media clock.
	frame := make([]byte, SamplesPerFrame)
	for i := range frame {
		frame[i] = byte(i)
	}
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := port.WriteFrame(frame); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("WriteFrame blocked for %s; it must not pace", elapsed)
	}
	for i := 0; i < 5; i++ {
		pkt := readPacket(t, peer)
		if len(pkt.Payload) != SamplesPerFrame {
			t.Fatalf("packet %d payload length = %d, want %d unsplit", i, len(pkt.Payload), SamplesPerFrame)
		}
	}
}

func TestPortWriteFrameToleratesNilReceiver(t *testing.T) {
	var port *Port
	if err := port.WriteFrame([]byte{1, 2, 3}); err != nil {
		t.Fatalf("WriteFrame on nil port = %v, want nil", err)
	}
	if got := port.FrameBytes(); got != SamplesPerFrame {
		t.Fatalf("FrameBytes on nil port = %d, want %d", got, SamplesPerFrame)
	}
}

func TestNewSilenceFrameIsAFullFrameOfPCMUSilence(t *testing.T) {
	frame := NewSilenceFrame()
	if len(frame) != SamplesPerFrame {
		t.Fatalf("silence frame length = %d, want %d", len(frame), SamplesPerFrame)
	}
	for i, b := range frame {
		if b != SilenceByte {
			t.Fatalf("silence frame[%d] = %#x, want %#x", i, b, SilenceByte)
		}
	}
}

func writeFramePortAndPeer(t *testing.T, frameBytes int) (*Port, *net.UDPConn) {
	t.Helper()
	port, err := Listen(Config{
		ListenIP: "127.0.0.1",
		// Constrained to the unprivileged ephemeral range: with no bounds,
		// Listen picks anywhere in 1-65535 and intermittently fails to bind
		// a privileged port.
		PortMin:             20000,
		PortMax:             40000,
		PayloadType:         0,
		OutboundFrameBytes:  frameBytes,
		MediaTimeoutInitial: time.Second,
		MediaTimeout:        time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { port.Close() })

	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	port.SetRemote(peer.LocalAddr().(*net.UDPAddr))
	return port, peer
}

func TestListenUsesEvenRTPPortAndSkipsBusyPorts(t *testing.T) {
	minPort, maxPort, blocker := reserveEvenPairRange(t)
	defer blocker.Close()

	conn, err := listenUDPRange("127.0.0.1", minPort, maxPort)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	got := conn.LocalAddr().(*net.UDPAddr).Port
	want := minPort + 2
	if got != want {
		t.Fatalf("allocated port = %d, want %d", got, want)
	}
}

func TestListenFailsWhenEvenRTPPortsAreExhausted(t *testing.T) {
	minPort, maxPort, first := reserveEvenPairRange(t)
	defer first.Close()

	second, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: minPort + 2})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	if _, err := listenUDPRange("127.0.0.1", minPort, maxPort); err == nil {
		t.Fatal("expected exhausted even RTP port range to fail")
	}
}

func readPacket(t *testing.T, conn *net.UDPConn) pionrtp.Packet {
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

func reserveEvenPairRange(t *testing.T) (int, int, *net.UDPConn) {
	t.Helper()
	ip := net.ParseIP("127.0.0.1")
	for port := 30000; port < 50000; port += 2 {
		first, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip, Port: port})
		if err != nil {
			continue
		}
		second, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip, Port: port + 2})
		if err == nil {
			_ = second.Close()
			return port, port + 3, first
		}
		_ = first.Close()
	}
	t.Fatal("could not reserve even RTP test range")
	return 0, 0, nil
}
