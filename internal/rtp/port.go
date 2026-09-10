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

// NewSilenceFrame returns one fresh frame of PCMU silence, for callers that
// need to emit comfort noise or pad a partially filled frame.
func NewSilenceFrame() []byte {
	b := make([]byte, SamplesPerFrame)
	for i := range b {
		b[i] = SilenceByte
	}
	return b
}

type Config struct {
	ListenIP     string
	PortMin      int
	PortMax      int
	SymmetricRTP bool
	PayloadType  uint8
	// OutboundFrameBytes is the fixed payload size of every outbound RTP
	// packet, and equally the amount the RTP timestamp advances per packet.
	// Defaults to SamplesPerFrame (one 20ms PCMU frame).
	OutboundFrameBytes  int
	MediaTimeoutInitial time.Duration
	MediaTimeout        time.Duration
	Log                 *slog.Logger
}

type Port struct {
	conn         *net.UDPConn
	log          *slog.Logger
	payloadType  uint8
	symmetricRTP bool
	frameBytes   int
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
	if conf.OutboundFrameBytes <= 0 {
		conf.OutboundFrameBytes = SamplesPerFrame
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
		frameBytes:   conf.OutboundFrameBytes,
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

// FrameBytes reports the fixed outbound payload size this Port emits.
func (p *Port) FrameBytes() int {
	if p == nil || p.frameBytes <= 0 {
		return SamplesPerFrame
	}
	return p.frameBytes
}

// WriteFrame sends exactly one RTP packet, immediately. It never blocks and
// never paces: the caller owns the media clock, so that pacing lives in one
// place instead of being split across two clocks that drift against each
// other.
//
// Two invariants make the outbound stream a clean, continuous media stream:
//
//   - The timestamp advances by a fixed frame every packet, never by
//     len(payload). Deriving it from the payload length lets a short frame
//     advance the media clock by less than the wall-clock time it occupied,
//     which starves the receiver's jitter buffer. Callers must therefore hand
//     WriteFrame exactly FrameBytes() bytes, padding with SilenceByte when
//     they have less real audio than that.
//
//   - The marker bit is never set. It means "start of a new talkspurt", and
//     receivers commonly react by resetting their jitter buffer and dropping
//     what it holds. Since this Port emits an unbroken stream (comfort noise
//     included) with contiguous timestamps, there is no talkspurt boundary to
//     signal, and setting it mid-call would cut off audio already in flight.
func (p *Port) WriteFrame(payload []byte) error {
	if p == nil || len(payload) == 0 {
		return nil
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	remote := p.remote.Load()
	if remote == nil {
		return nil
	}

	header := pionrtp.Header{
		Version:        2,
		PayloadType:    p.payloadType,
		SequenceNumber: p.seq,
		Timestamp:      p.ts,
		SSRC:           p.ssrc,
		Marker:         false,
	}
	p.seq++
	p.ts += uint32(p.FrameBytes())

	raw, err := (&pionrtp.Packet{Header: header, Payload: payload}).Marshal()
	if err != nil {
		return err
	}
	_, err = p.conn.WriteToUDP(raw, remote)
	return err
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
