package rtp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pionrtp "github.com/pion/rtp"
)

const (
	SampleRate      = 8000
	FrameDuration   = 20 * time.Millisecond
	FramesPerSec    = int(time.Second / FrameDuration)
	SamplesPerFrame = SampleRate / FramesPerSec
	mtuSize         = 1500

	// SilenceByte is the PCMU (G.711 mu-law) encoding of a zero-amplitude sample.
	SilenceByte = 0xff
)

var silenceFrame = newSilenceFrame()

func newSilenceFrame() []byte {
	b := make([]byte, SamplesPerFrame)
	for i := range b {
		b[i] = SilenceByte
	}
	return b
}

type Config struct {
	ListenIP            string
	PortMin             int
	PortMax             int
	SymmetricRTP        bool
	PayloadType         uint8
	OutboundPacketBytes int
	OutboundPacketEvery time.Duration
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
	packetEvery  time.Duration
	initialTO    time.Duration
	mediaTO      time.Duration
	remote       atomic.Pointer[net.UDPAddr]
	closed       atomic.Bool

	writeMu   sync.Mutex
	seq       uint16
	ts        uint32
	ssrc      uint32
	nextAt    time.Time
	paceSeq   uint64
	talkspurt bool

	interruptMu  sync.Mutex
	interruptCh  chan struct{}
	interruptSeq atomic.Uint64

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
		conf.OutboundPacketBytes = SamplesPerFrame
	}
	if conf.OutboundPacketEvery <= 0 {
		conf.OutboundPacketEvery = FrameDuration
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
		packetEvery:  conf.OutboundPacketEvery,
		initialTO:    conf.MediaTimeoutInitial,
		mediaTO:      conf.MediaTimeout,
		seq:          seq,
		ts:           randomUint32(),
		ssrc:         randomUint32(),
		received:     make(chan []byte, 128),
		firstPacket:  make(chan struct{}),
		interruptCh:  make(chan struct{}),
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
	if p == nil || len(payload) == 0 {
		return nil
	}
	interruptSeq := p.interruptSeq.Load()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	for len(payload) > 0 {
		if p.outboundInterrupted(interruptSeq) {
			return nil
		}
		n := len(payload)
		if n > p.packetBytes {
			n = p.packetBytes
		}
		marker := !p.talkspurt
		if err := p.writePacketLocked(payload[:n], interruptSeq, marker); err != nil {
			if errors.Is(err, errOutboundInterrupted) {
				return nil
			}
			return err
		}
		p.talkspurt = true
		payload = payload[n:]
	}
	return nil
}

// WriteSilenceFrame sends one paced frame of PCMU silence. The outbound RTP
// writer calls this whenever no real backend audio is available so the RTP
// stream to the caller never goes quiet for a whole packet interval: gaps in
// backend delivery (network jitter, TTS generation pauses) would otherwise
// starve the pacer in WritePayload and produce audible dead air instead of a
// continuous, smooth stream.
func (p *Port) WriteSilenceFrame() error {
	if p == nil {
		return nil
	}
	interruptSeq := p.interruptSeq.Load()

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	if err := p.writePacketLocked(silenceFrame, interruptSeq, false); err != nil {
		if errors.Is(err, errOutboundInterrupted) {
			return nil
		}
		return err
	}
	p.talkspurt = false
	return nil
}

func (p *Port) InterruptOutbound() {
	if p == nil {
		return
	}
	p.interruptSeq.Add(1)

	p.interruptMu.Lock()
	if p.interruptCh != nil {
		close(p.interruptCh)
	}
	p.interruptCh = make(chan struct{})
	p.interruptMu.Unlock()
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

var errOutboundInterrupted = errors.New("outbound RTP interrupted")

func (p *Port) writePacketLocked(payload []byte, interruptSeq uint64, marker bool) error {
	remote := p.remote.Load()
	if remote == nil {
		return nil
	}

	if p.paceSeq != interruptSeq {
		p.nextAt = time.Time{}
		p.paceSeq = interruptSeq
	}

	if p.packetEvery > 0 {
		now := time.Now()
		if !p.nextAt.IsZero() && now.Before(p.nextAt) {
			if p.waitForOutboundTurn(time.Until(p.nextAt), interruptSeq) {
				return errOutboundInterrupted
			}
		}
	}
	if p.outboundInterrupted(interruptSeq) {
		return errOutboundInterrupted
	}

	header := pionrtp.Header{
		Version:        2,
		PayloadType:    p.payloadType,
		SequenceNumber: p.seq,
		Timestamp:      p.ts,
		SSRC:           p.ssrc,
		Marker:         marker,
	}
	p.seq++
	p.ts += uint32(len(payload))

	raw, err := (&pionrtp.Packet{Header: header, Payload: payload}).Marshal()
	if err != nil {
		return err
	}
	_, err = p.conn.WriteToUDP(raw, remote)
	if err == nil && p.packetEvery > 0 {
		p.nextAt = time.Now().Add(p.packetEvery)
	}
	return err
}

func (p *Port) waitForOutboundTurn(d time.Duration, interruptSeq uint64) bool {
	if d <= 0 {
		return p.outboundInterrupted(interruptSeq)
	}
	if p.outboundInterrupted(interruptSeq) {
		return true
	}

	p.interruptMu.Lock()
	interruptCh := p.interruptCh
	p.interruptMu.Unlock()
	if p.outboundInterrupted(interruptSeq) {
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return p.outboundInterrupted(interruptSeq)
	case <-interruptCh:
		return true
	}
}

func (p *Port) outboundInterrupted(interruptSeq uint64) bool {
	return p.interruptSeq.Load() != interruptSeq
}

func listenUDPRange(listenIP string, minPort, maxPort int) (*net.UDPConn, error) {
	ip := net.ParseIP(listenIP)
	if ip == nil {
		return nil, fmt.Errorf("invalid RTP listen IP %q", listenIP)
	}
	if minPort <= 0 {
		minPort = 1
	}
	if maxPort <= 0 || maxPort > 0xFFFF {
		maxPort = 0xFFFF
	}
	evenMin := (minPort + 1) &^ 1
	evenMax := maxPort &^ 1
	if evenMin > evenMax {
		return nil, fmt.Errorf("no free even UDP port in range %d-%d", minPort, maxPort)
	}

	ports := (evenMax-evenMin)/2 + 1
	port := evenMin + 2*mathrand.Intn(ports)
	for try := 0; try < ports; try++ {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip, Port: port})
		if err == nil {
			return conn, nil
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, err
		}
		port += 2
		if port > evenMax {
			port = evenMin
		}
	}
	return nil, fmt.Errorf("no free even UDP port in range %d-%d", minPort, maxPort)
}

func randomUint32() uint32 {
	var b [4]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint32(b[:])
}
