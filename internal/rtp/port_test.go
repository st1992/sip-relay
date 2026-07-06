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
