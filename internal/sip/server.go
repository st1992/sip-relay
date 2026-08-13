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

	"sip-relay/internal/backend"
	"sip-relay/internal/call"
	"sip-relay/internal/calllog"
	"sip-relay/internal/ces"
	"sip-relay/internal/config"
	relayrtp "sip-relay/internal/rtp"
	relaysdp "sip-relay/internal/sdp"
	telephonyws "sip-relay/internal/websocket"
)

const ackTimeout = 10 * time.Second

var sessionIDCleaner = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

type Server struct {
	cfg *config.Config
	log *slog.Logger

	ua        *sipgo.UserAgent
	cli       *sipgo.Client
	srv       *sipgo.Server
	listeners []net.Listener
	udp       *net.UDPConn
	callLog   *calllog.Clients

	mu      sync.Mutex
	byTag   map[string]*entry
	byCall  map[string]*entry
	stopped bool
}

type entry struct {
	localTag string
	callID   string
	call     *call.Call
	dialog   *dialog
	start    sync.Once
	close    sync.Once
	ack      chan struct{}
}

type dialog struct {
	invite *sipmsg.Request
	ok     *sipmsg.Response
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
	// Best-effort: call recording/logging is not on the media path, so a
	// broken GCS/Pub-Sub config or a transient dial failure here must not
	// stop the relay from serving calls. Building this once at startup
	// (rather than per call, as before) avoids a fresh client dial+auth on
	// every single call teardown.
	callLog, err := calllog.NewClients(ctx, s.cfg)
	if err != nil {
		s.log.Error("failed to create call log/recording clients; recording and call logs are disabled", "error", err)
	} else {
		s.callLog = callLog
	}

	ua, err := sipgo.NewUA(
		sipgo.WithUserAgent(s.cfg.SIP.UserAgent),
		sipgo.WithUserAgentIP(net.ParseIP(s.cfg.SIP.AdvertisedIP)),
	)
	if err != nil {
		return err
	}
	s.ua = ua
	cli, err := sipgo.NewClient(
		ua,
		sipgo.WithClientHostname(s.cfg.SIP.AdvertisedIP),
		sipgo.WithClientPort(s.cfg.SIP.ListenPort),
	)
	if err != nil {
		return err
	}
	s.cli = cli

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
	s.log.Info("starting SIP listeners", "addr", addr)
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
		s.log.Info("SIP UDP listener started", "addr", addr)
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
		s.log.Info("SIP TCP listener started", "addr", addr)
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
	if s.cli != nil {
		_ = s.cli.Close()
	}
	if s.ua != nil {
		s.ua.Close()
	}
	s.callLog.Close()
}

func (s *Server) onOptions(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	s.log.Info("SIP OPTIONS received", "source", req.Source())
	_ = tx.Respond(sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", nil))
	tx.Terminate()
}

func (s *Server) onInvite(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	callID := callID(req)
	extension := dnis(req)
	s.log.Info("SIP INVITE received", "call_id", callID, "ani", ani(req), "dnis", extension, "source", req.Source())
	respondTrying(req, tx)
	if callID == "" {
		respond(req, tx, 400, "Missing Call-ID", nil)
		return
	}
	if len(req.Body()) == 0 {
		respond(req, tx, 400, "Missing SDP", nil)
		return
	}
	route, ok := s.cfg.Route(extension)
	if !ok {
		s.log.Warn("rejecting SIP INVITE for unconfigured extension", "call_id", callID, "extension", extension)
		respond(req, tx, 404, "Unknown extension", nil)
		return
	}
	profile := config.ResolveProfile(route)
	mediaBackend, sessionPrefix, err := backendForRoute(s.cfg, route)
	if err != nil {
		respond(req, tx, 500, "Invalid extension backend", nil)
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
		MediaTimeoutInitial: s.cfg.RTP.MediaTimeoutInitial,
		MediaTimeout:        s.cfg.RTP.MediaTimeout,
		PayloadType:         0,
		Log:                 s.log,
	})
	if err != nil {
		respond(req, tx, 503, "No RTP port available", nil)
		return
	}
	s.log.Info("RTP port allocated", "call_id", callID, "port", port.LocalPort())

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

	sessionID := sessionID(sessionPrefix, callID, localTag)
	metadata := call.Metadata{
		CallID:  callID,
		ANI:     ani(req),
		DNIS:    extension,
		Headers: headers(req),
		Profile: profile,
	}
	c := call.New(sessionID, metadata, s.cfg, mediaBackend, s.callLog, port, s.log.With("call_id", callID, "session_id", sessionID, "extension", extension, "backend", route.Backend, "profile", profile))
	e := &entry{
		localTag: localTag,
		callID:   callID,
		call:     c,
		ack:      make(chan struct{}, 1),
	}
	s.store(e)

	resp := sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", answer.Payload)
	if to := resp.To(); to != nil {
		to.Params.Add("tag", localTag)
	}
	resp.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
	resp.AppendHeader(sipmsg.NewHeader("Contact", s.contactHeader()))
	e.dialog = &dialog{invite: req.Clone(), ok: resp.Clone()}
	s.log.Info("responding to SIP INVITE", "call_id", callID, "session_id", sessionID)
	if err := tx.Respond(resp); err != nil {
		s.remove(e)
		c.Close()
		return
	}
	s.log.Info("SIP INVITE accepted; waiting for ACK", "call_id", callID, "session_id", sessionID)

	go s.awaitACK(context.Background(), e, tx)
	go func() {
		<-c.Done()
		if shouldSendBYE(c.EndReason()) {
			e.close.Do(func() {
				s.sendBYE(e)
				s.remove(e)
			})
			return
		}
		s.remove(e)
	}()
}

func (s *Server) onAck(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	e := s.lookup(req)
	if e == nil {
		s.log.Warn("SIP ACK received for unknown call", "call_id", callID(req))
		return
	}
	s.log.Info("SIP ACK received", "call_id", e.callID)
	select {
	case e.ack <- struct{}{}:
	default:
	}
}

func (s *Server) onBye(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	e := s.lookup(req)
	s.log.Info("SIP BYE received", "call_id", callID(req))
	_ = tx.Respond(sipmsg.NewResponseFromRequest(req, sipmsg.StatusOK, "OK", nil))
	tx.Terminate()
	if e == nil {
		return
	}
	e.close.Do(func() {
		e.call.CloseWithReason(call.EndReasonUser)
		s.remove(e)
	})
}

func (s *Server) onNoRoute(log *slog.Logger, req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	s.log.Info("SIP request received with no route", "method", req.Method, "call_id", callID(req))
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
		e.call.CloseWithReason(call.EndReasonUser)
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

func (s *Server) sendBYE(e *entry) {
	if e == nil || e.dialog == nil || s.cli == nil {
		return
	}
	req := newServerBYE(e.dialog)
	if req == nil {
		s.log.Warn("cannot send SIP BYE; dialog is incomplete", "call_id", e.callID)
		return
	}

	s.log.Info("sending SIP BYE after backend ended call", "call_id", e.callID, "reason", e.call.EndReason())
	tx, err := s.cli.TransactionRequest(req)
	if err != nil {
		s.log.Warn("failed to send SIP BYE", "call_id", e.callID, "error", err)
		return
	}
	defer tx.Terminate()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case resp := <-tx.Responses():
			if resp != nil && resp.StatusCode >= 200 {
				s.log.Info("SIP BYE completed", "call_id", e.callID, "status", resp.StatusCode)
				return
			}
		case <-tx.Done():
			return
		case <-timer.C:
			s.log.Warn("timed out waiting for SIP BYE response", "call_id", e.callID)
			return
		}
	}
}

func newServerBYE(d *dialog) *sipmsg.Request {
	if d == nil || d.invite == nil || d.ok == nil {
		return nil
	}

	recipient := d.invite.Recipient
	if contact := d.invite.Contact(); contact != nil {
		recipient = *contact.Address.Clone()
	} else if from := d.invite.From(); from != nil {
		recipient = *from.Address.Clone()
	}

	req := sipmsg.NewRequest(sipmsg.BYE, recipient)
	req.SipVersion = d.invite.SipVersion

	for _, header := range d.invite.GetHeaders("Record-Route") {
		req.AppendHeader(sipmsg.NewHeader("Route", header.Value()))
	}

	maxForwards := sipmsg.MaxForwardsHeader(70)
	req.AppendHeader(&maxForwards)
	if to := d.ok.To(); to != nil {
		from := to.AsFrom()
		req.AppendHeader(&from)
	}
	if from := d.invite.From(); from != nil {
		to := from.AsTo()
		req.AppendHeader(&to)
	}
	if callID := d.invite.CallID(); callID != nil {
		req.AppendHeader(sipmsg.HeaderClone(callID))
	}
	cseqNo := uint32(1)
	if cseq := d.invite.CSeq(); cseq != nil {
		cseqNo = cseq.SeqNo + 1
	}
	req.AppendHeader(&sipmsg.CSeqHeader{SeqNo: cseqNo, MethodName: sipmsg.BYE})
	if contact := d.ok.Contact(); contact != nil {
		req.AppendHeader(sipmsg.HeaderClone(contact))
	}

	req.SetBody(nil)
	req.SetTransport(d.invite.Transport())
	if route := req.Route(); route != nil {
		req.SetDestination(route.Address.HostPort())
	} else {
		req.SetDestination(recipient.HostPort())
	}
	return req
}

func (s *Server) contactHeader() string {
	return fmt.Sprintf("<sip:%s@%s:%d>", s.cfg.SIP.UserAgent, s.cfg.SIP.AdvertisedIP, s.cfg.SIP.ListenPort)
}

func backendForRoute(cfg *config.Config, route config.ExtensionConfig) (backend.Dialer, string, error) {
	profile := config.ResolveProfile(route)
	switch route.Backend {
	case config.BackendCES:
		cesCfg, ok := cfg.CES[profile]
		if !ok {
			return nil, "", fmt.Errorf("ces profile %q not configured", profile)
		}
		return ces.Dialer{Config: cesCfg}, cesCfg.SessionPrefix, nil
	case config.BackendWebSocket:
		wsCfg, ok := cfg.WebSocket[profile]
		if !ok {
			return nil, "", fmt.Errorf("websocket profile %q not configured", profile)
		}
		return telephonyws.Dialer{Config: wsCfg}, "sip", nil
	default:
		return nil, "", fmt.Errorf("unsupported backend %q", route.Backend)
	}
}

func shouldSendBYE(reason call.EndReason) bool {
	switch reason {
	case call.EndReasonAgent, call.EndReasonTransfer, call.EndReasonBackend:
		return true
	default:
		return false
	}
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

func headers(req *sipmsg.Request) map[string][]string {
	if req == nil {
		return nil
	}
	out := make(map[string][]string)
	for _, header := range req.Headers() {
		out[header.Name()] = append(out[header.Name()], header.Value())
	}
	return out
}

func respond(req *sipmsg.Request, tx sipmsg.ServerTransaction, status sipmsg.StatusCode, reason string, body []byte) {
	resp := sipmsg.NewResponseFromRequest(req, status, reason, body)
	if body != nil {
		resp.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
	}
	_ = tx.Respond(resp)
	tx.Terminate()
}

func respondTrying(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	_ = tx.Respond(sipmsg.NewResponseFromRequest(req, sipmsg.StatusTrying, "Trying", nil))
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
