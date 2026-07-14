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
		OutboundPacketBytes: 160,
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

func TestPortWritesRTPWithPayloadLengthTimestamps(t *testing.T) {
	port, err := Listen(Config{
		ListenIP:            "127.0.0.1",
		PayloadType:         0,
		OutboundPacketBytes: 3,
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

	if err := port.WritePayload([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}

	first := readPacket(t, peer)
	second := readPacket(t, peer)
	if string(first.Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("first payload = %v", first.Payload)
	}
	if string(second.Payload) != string([]byte{4, 5}) {
		t.Fatalf("second payload = %v", second.Payload)
	}
	if second.SequenceNumber != first.SequenceNumber+1 {
		t.Fatalf("sequence did not increment: %d then %d", first.SequenceNumber, second.SequenceNumber)
	}
	if second.Timestamp != first.Timestamp+uint32(len(first.Payload)) {
		t.Fatalf("timestamp increment = %d, want %d", second.Timestamp-first.Timestamp, len(first.Payload))
	}
}

func TestPortPacesOutboundRTPPackets(t *testing.T) {
	port, err := Listen(Config{
		ListenIP:            "127.0.0.1",
		PayloadType:         0,
		OutboundPacketBytes: 3,
		OutboundPacketEvery: 25 * time.Millisecond,
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

	done := make(chan error, 1)
	go func() {
		done <- port.WritePayload([]byte{1, 2, 3, 4, 5, 6})
	}()

	_ = readPacket(t, peer)
	firstAt := time.Now()
	_ = readPacket(t, peer)
	secondAt := time.Now()

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if elapsed := secondAt.Sub(firstAt); elapsed < 15*time.Millisecond {
		t.Fatalf("packets were not paced: elapsed = %s", elapsed)
	}
}

func TestPortPacesAcrossWritePayloadCalls(t *testing.T) {
	port, err := Listen(Config{
		ListenIP:            "127.0.0.1",
		PayloadType:         0,
		OutboundPacketBytes: 3,
		OutboundPacketEvery: 25 * time.Millisecond,
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

	done := make(chan error, 1)
	go func() {
		if err := port.WritePayload([]byte{1, 2, 3}); err != nil {
			done <- err
			return
		}
		done <- port.WritePayload([]byte{4, 5, 6})
	}()

	_ = readPacket(t, peer)
	firstAt := time.Now()
	_ = readPacket(t, peer)
	secondAt := time.Now()

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if elapsed := secondAt.Sub(firstAt); elapsed < 15*time.Millisecond {
		t.Fatalf("packets across writes were not paced: elapsed = %s", elapsed)
	}
}

func TestPortInterruptsOutboundWritePayload(t *testing.T) {
	port, err := Listen(Config{
		ListenIP:            "127.0.0.1",
		PayloadType:         0,
		OutboundPacketBytes: 3,
		OutboundPacketEvery: 200 * time.Millisecond,
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

	done := make(chan error, 1)
	go func() {
		done <- port.WritePayload([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	}()

	first := readPacket(t, peer)
	if string(first.Payload) != string([]byte{1, 2, 3}) {
		t.Fatalf("first payload = %v", first.Payload)
	}

	port.InterruptOutbound()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for interrupted payload write")
	}

	if err := peer.SetReadDeadline(time.Now().Add(75 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	if _, _, err := peer.ReadFromUDP(buf); err == nil {
		t.Fatal("received packet after outbound write was interrupted")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read after interrupt failed with unexpected error: %v", err)
	}

	if err := port.WritePayload([]byte{10, 11, 12}); err != nil {
		t.Fatal(err)
	}
	next := readPacket(t, peer)
	if string(next.Payload) != string([]byte{10, 11, 12}) {
		t.Fatalf("next payload = %v", next.Payload)
	}
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
