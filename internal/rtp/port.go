package rtp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pionrtp "github.com/pion/rtp"
)

const (
	SampleRate = 8000
	mtuSize    = 1500
)

type Config struct {
	ListenIP            string
	PortMin             int
	PortMax             int
	SymmetricRTP        bool
	PayloadType         uint8
	OutboundPacketBytes int
	MediaTimeoutInitial time.Duration
	MediaTimeout        time.Duration
	Log                 *slog.Logger
}

type Port struct {
	conn         *net.UDPConn
	log          *slog.Logger
	payloadType  uint8
	symmetricRTP bool
	packetBytes  int
	initialTO    time.Duration
	mediaTO      time.Duration
	remote       atomic.Pointer[net.UDPAddr]
	closed       atomic.Bool

	writeMu sync.Mutex
	seq     uint16
	ts      uint32
	ssrc    uint32

	received       chan []byte
	firstPacket    chan struct{}
	firstPacketSet sync.Once
	packetCount    atomic.Uint64
}

func Listen(conf Config) (*Port, error) {
	if conf.Log == nil {
		conf.Log = slog.Default()
	}
	if conf.ListenIP == "" {
		conf.ListenIP = "0.0.0.0"
	}
	if conf.MediaTimeoutInitial <= 0 {
		conf.MediaTimeoutInitial = 30 * time.Second
	}
	if conf.MediaTimeout <= 0 {
		conf.MediaTimeout = 15 * time.Second
	}
	if conf.OutboundPacketBytes <= 0 {
		conf.OutboundPacketBytes = 160
	}

	conn, err := listenUDPRange(conf.ListenIP, conf.PortMin, conf.PortMax)
	if err != nil {
		return nil, err
	}

	seq := uint16(randomUint32())
	port := &Port{
		conn:         conn,
		log:          conf.Log,
		payloadType:  conf.PayloadType,
		symmetricRTP: conf.SymmetricRTP,
		packetBytes:  conf.OutboundPacketBytes,
		initialTO:    conf.MediaTimeoutInitial,
		mediaTO:      conf.MediaTimeout,
		seq:          seq,
		ts:           randomUint32(),
		ssrc:         randomUint32(),
		received:     make(chan []byte, 128),
		firstPacket:  make(chan struct{}),
	}
	return port, nil
}

func (p *Port) LocalPort() int {
	if p == nil || p.conn == nil {
		return 0
	}
	return p.conn.LocalAddr().(*net.UDPAddr).Port
}

func (p *Port) SetRemote(addr *net.UDPAddr) {
	if p == nil || addr == nil {
		return
	}
	p.remote.Store(addr)
}

func (p *Port) Payloads() <-chan []byte {
	return p.received
}

func (p *Port) FirstPacket() <-chan struct{} {
	return p.firstPacket
}

func (p *Port) Run(ctx context.Context) error {
	defer close(p.received)
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.readLoop()
	}()

	timeout := time.NewTimer(p.mediaTO)
	defer timeout.Stop()
	lastPackets := uint64(0)
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			_ = p.Close()
			<-errCh
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-timeout.C:
			currentPackets := p.packetCount.Load()
			if currentPackets != lastPackets {
				lastPackets = currentPackets
				timeout.Reset(p.mediaTO)
				continue
			}
			if currentPackets == 0 && time.Since(start) < p.initialTO {
				timeout.Reset(p.mediaTO)
				continue
			}
			_ = p.Close()
			return errors.New("media timeout")
		}
	}
}

func (p *Port) WritePayload(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	for len(payload) > 0 {
		n := len(payload)
		if n > p.packetBytes {
			n = p.packetBytes
		}
		if err := p.writePacket(payload[:n]); err != nil {
			return err
		}
		payload = payload[n:]
	}
	return nil
}

func (p *Port) Close() error {
	if p == nil || p.closed.Swap(true) {
		return nil
	}
	return p.conn.Close()
}

func (p *Port) readLoop() error {
	buf := make([]byte, mtuSize)
	for {
		n, src, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			if p.closed.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		var pkt pionrtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			p.log.Debug("dropping malformed RTP packet", "error", err)
			continue
		}
		if pkt.PayloadType != p.payloadType {
			p.log.Debug("dropping RTP packet with unexpected payload type", "payload_type", pkt.PayloadType)
			continue
		}
		if p.symmetricRTP {
			p.remote.Store(src)
		}
		p.packetCount.Add(1)
		p.firstPacketSet.Do(func() { close(p.firstPacket) })

		payload := make([]byte, len(pkt.Payload))
		copy(payload, pkt.Payload)
		select {
		case p.received <- payload:
		default:
			p.log.Warn("dropping RTP payload because relay input queue is full")
		}
	}
}

func (p *Port) writePacket(payload []byte) error {
	remote := p.remote.Load()
	if remote == nil {
		return nil
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	header := pionrtp.Header{
		Version:        2,
		PayloadType:    p.payloadType,
		SequenceNumber: p.seq,
		Timestamp:      p.ts,
		SSRC:           p.ssrc,
	}
	p.seq++
	p.ts += uint32(len(payload))

	raw, err := (&pionrtp.Packet{Header: header, Payload: payload}).Marshal()
	if err != nil {
		return err
	}
	_, err = p.conn.WriteToUDP(raw, remote)
	return err
}

func listenUDPRange(listenIP string, minPort, maxPort int) (*net.UDPConn, error) {
	ip := net.ParseIP(listenIP)
	if ip == nil {
		return nil, fmt.Errorf("invalid RTP listen IP %q", listenIP)
	}
	if minPort == 0 && maxPort == 0 {
		return net.ListenUDP("udp", &net.UDPAddr{IP: ip})
	}
	for port := minPort; port <= maxPort; port++ {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip, Port: port})
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("no free UDP port in range %d-%d", minPort, maxPort)
}

func randomUint32() uint32 {
	var b [4]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint32(b[:])
}
