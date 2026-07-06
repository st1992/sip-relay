package sip

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/livekit/sipgo"
	sipmsg "github.com/livekit/sipgo/sip"

	"sip-relay/internal/call"
	"sip-relay/internal/config"
	relayrtp "sip-relay/internal/rtp"
	relaysdp "sip-relay/internal/sdp"
)

const ackTimeout = 10 * time.Second

var sessionIDCleaner = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

type Server struct {
	cfg *config.Config
	log *slog.Logger

	ua        *sipgo.UserAgent
	srv       *sipgo.Server
	listeners []net.Listener
	udp       *net.UDPConn

	mu      sync.Mutex
	byTag   map[string]*entry
	byCall  map[string]*entry
	stopped bool
}

type entry struct {
	localTag string
	callID   string
	call     *call.Call
	start    sync.Once
	close    sync.Once
	ack      chan struct{}
}

func NewServer(cfg *config.Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		cfg:    cfg,
		log:    log,
		byTag:  make(map[string]*entry),
		byCall: make(map[string]*entry),
	}
}

func (s *Server) Start(ctx context.Context) error {
	ua, err := sipgo.NewUA(
		sipgo.WithUserAgent(s.cfg.SIP.UserAgent),
	)
	if err != nil {
		return err
	}
	s.ua = ua

	srv, err := sipgo.NewServer(ua)
	if err != nil {
		return err
	}
	s.srv = srv
	srv.OnOptions(s.onOptions)
	srv.OnInvite(s.onInvite)
	srv.OnAck(s.onAck)
	srv.OnBye(s.onBye)
	srv.OnNoRoute(s.onNoRoute)

	addr := fmt.Sprintf("%s:%d", s.cfg.SIP.ListenIP, s.cfg.SIP.ListenPort)
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	s.udp = udpConn
	go func() {
		if err := srv.ServeUDP(udpConn); err != nil {
			s.log.Error("SIP UDP listener stopped", "error", err)
		}
	}()

	tcpLis, err := net.Listen("tcp", addr)
	if err != nil {
		_ = udpConn.Close()
		return err
	}
	s.listeners = append(s.listeners, tcpLis)
	go func() {
		if err := srv.ServeTCP(tcpLis); err != nil {
			s.log.Error("SIP TCP listener stopped", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	s.log.Info("SIP relay listening", "addr", addr)
	return nil
}

func (s *Server) Close() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	entries := make([]*entry, 0, len(s.byTag))
	for _, e := range s.byTag {
		entries = append(entries, e)
	}
	s.byTag = make(map[string]*entry)
	s.byCall = make(map[string]*entry)
	s.mu.Unlock()

	for _, e := range entries {
		e.call.Close()
	}
	if s.udp != nil {
		_ = s.udp.Close()
	}
	for _, l := range s.listeners {
		_ = l.Close()
	}
	if s.srv != nil {
		_ = s.srv.Close()
	}
	if s.ua != nil {
		s.ua.Close()
	}
}

func (s *Server) onOptions(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	_ = tx.Respond(sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", nil))
	tx.Terminate()
}

func (s *Server) onInvite(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	callID := callID(req)
	if callID == "" {
		respond(req, tx, 400, "Missing Call-ID", nil)
		return
	}
	if len(req.Body()) == 0 {
		respond(req, tx, 400, "Missing SDP", nil)
		return
	}

	advertisedIP, err := netip.ParseAddr(s.cfg.SIP.AdvertisedIP)
	if err != nil {
		respond(req, tx, 500, "Invalid advertised media IP", nil)
		return
	}

	localTag := randomTag()
	port, err := relayrtp.Listen(relayrtp.Config{
		ListenIP:            s.cfg.RTP.ListenIP,
		PortMin:             s.cfg.RTP.PortMin,
		PortMax:             s.cfg.RTP.PortMax,
		SymmetricRTP:        s.cfg.RTP.SymmetricRTP,
		PayloadType:         0,
		OutboundPacketBytes: s.cfg.RTP.OutboundPacketMS * relayrtp.SampleRate / 1000,
		Log:                 s.log,
	})
	if err != nil {
		respond(req, tx, 503, "No RTP port available", nil)
		return
	}

	answer, err := relaysdp.AnswerOffer(req.Body(), advertisedIP, port.LocalPort())
	if err != nil {
		_ = port.Close()
		respond(req, tx, 488, "PCMU/8000 required", nil)
		return
	}
	port.SetRemote(&net.UDPAddr{
		IP:   net.ParseIP(answer.Offer.RemoteAddr.String()),
		Port: answer.Offer.RemotePort,
	})

	portPayloadType := answer.Offer.PayloadType
	if portPayloadType != 0 {
		_ = port.Close()
		respond(req, tx, 488, "PCMU static payload type required", nil)
		return
	}

	sessionID := sessionID(s.cfg.CES.SessionPrefix, callID, localTag)
	metadata := call.Metadata{
		CallID: callID,
		ANI:    ani(req),
		DNIS:   dnis(req),
	}
	c := call.New(sessionID, metadata, s.cfg, port, s.log.With("call_id", callID, "session_id", sessionID))
	e := &entry{
		localTag: localTag,
		callID:   callID,
		call:     c,
		ack:      make(chan struct{}),
	}
	s.store(e)

	resp := sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", answer.Payload)
	if to := resp.To(); to != nil {
		to.Params.Add("tag", localTag)
	}
	resp.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
	resp.AppendHeader(sipmsg.NewHeader("Contact", s.contactHeader()))
	if err := tx.Respond(resp); err != nil {
		s.remove(e)
		c.Close()
		return
	}

	go s.awaitACK(context.Background(), e, tx)
	go func() {
		<-c.Done()
		s.remove(e)
	}()
}

func (s *Server) onAck(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	e := s.lookup(req)
	if e == nil {
		return
	}
	select {
	case e.ack <- struct{}{}:
	default:
	}
}

func (s *Server) onBye(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	e := s.lookup(req)
	_ = tx.Respond(sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", nil))
	tx.Terminate()
	if e == nil {
		return
	}
	e.close.Do(func() {
		e.call.Close()
		s.remove(e)
	})
}

func (s *Server) onNoRoute(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	if req.Method == sipmsg.CANCEL {
		_ = tx.Respond(sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", nil))
		tx.Terminate()
		if e := s.lookup(req); e != nil {
			e.call.Close()
			s.remove(e)
		}
		return
	}
	respond(req, tx, 405, "Method Not Allowed", nil)
}

func (s *Server) awaitACK(ctx context.Context, e *entry, tx sipmsg.ServerTransaction) {
	timer := time.NewTimer(ackTimeout)
	defer timer.Stop()

	select {
	case <-e.ack:
		e.start.Do(func() {
			s.log.Info("SIP call ACK received; starting media relay", "call_id", e.callID)
			e.call.Start(ctx)
		})
	case <-tx.Acks():
		e.start.Do(func() {
			s.log.Info("SIP transaction ACK received; starting media relay", "call_id", e.callID)
			e.call.Start(ctx)
		})
	case cancelReq := <-tx.Cancels():
		_ = tx.Respond(sipmsg.NewResponseFromRequest(cancelReq, sipmsg.StatusOK, "OK", nil))
		e.call.Close()
		s.remove(e)
	case <-timer.C:
		s.log.Warn("timed out waiting for ACK", "call_id", e.callID)
		e.call.Close()
		s.remove(e)
	}
}

func (s *Server) store(e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byTag[e.localTag] = e
	s.byCall[e.callID] = e
}

func (s *Server) remove(e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byTag[e.localTag] == e {
		delete(s.byTag, e.localTag)
	}
	if s.byCall[e.callID] == e {
		delete(s.byCall, e.callID)
	}
}

func (s *Server) lookup(req *sipmsg.Request) *entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if to := req.To(); to != nil {
		if tag := to.Params.GetOr("tag", ""); tag != "" {
			if e := s.byTag[tag]; e != nil {
				return e
			}
		}
	}
	if id := callID(req); id != "" {
		return s.byCall[id]
	}
	return nil
}

func (s *Server) contactHeader() string {
	return fmt.Sprintf("<sip:%s@%s:%d>", s.cfg.SIP.UserAgent, s.cfg.SIP.AdvertisedIP, s.cfg.SIP.ListenPort)
}

func callID(req *sipmsg.Request) string {
	if h := req.CallID(); h != nil {
		return h.Value()
	}
	return ""
}

func ani(req *sipmsg.Request) string {
	if from := req.From(); from != nil {
		return from.Address.User
	}
	return ""
}

func dnis(req *sipmsg.Request) string {
	if req.Recipient.User != "" {
		return req.Recipient.User
	}
	if to := req.To(); to != nil {
		return to.Address.User
	}
	return ""
}

func respond(req *sipmsg.Request, tx sipmsg.ServerTransaction, status sipmsg.StatusCode, reason string, body []byte) {
	resp := sipmsg.NewResponseFromRequest(req, status, reason, body)
	if body != nil {
		resp.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
	}
	_ = tx.Respond(resp)
	tx.Terminate()
}

func randomTag() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func sessionID(prefix string, parts ...string) string {
	raw := strings.Join(parts, "-")
	clean := sessionIDCleaner.ReplaceAllString(raw, "-")
	clean = strings.Trim(clean, "-")
	if clean == "" {
		clean = randomTag()
	}
	if prefix != "" {
		return prefix + "-" + clean
	}
	return clean
}
