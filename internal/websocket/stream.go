package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	gorillaws "github.com/gorilla/websocket"

	"sip-relay/internal/audio"
	"sip-relay/internal/backend"
	"sip-relay/internal/config"
)

const (
	sessionPath   = "/api/v1/telephony/session"
	websocketPath = "/api/v1/telephony/ws/"
	writeTimeout  = 5 * time.Second
)

type Dialer struct {
	Config config.WebSocketConfig
}

func (d Dialer) Name() string { return config.BackendWebSocket }

func (d Dialer) Metadata() map[string]string {
	return map[string]string{"base_url": d.Config.BaseURL}
}

func (d Dialer) ConnectTimeout() time.Duration {
	return d.Config.SessionTimeout + d.Config.ConnectTimeout
}

func (d Dialer) Dial(ctx context.Context, _ string, log *slog.Logger) (backend.Stream, error) {
	if log == nil {
		log = slog.Default()
	}
	sessionID, err := createSession(ctx, d.Config)
	if err != nil {
		return nil, err
	}
	return connect(ctx, d.Config, sessionID, log)
}

type sessionResponse struct {
	SessionID string `json:"session_id"`
}

func createSession(ctx context.Context, conf config.WebSocketConfig) (string, error) {
	sessionCtx, cancel := context.WithTimeout(ctx, conf.SessionTimeout)
	defer cancel()

	endpoint, err := endpointURL(conf.BaseURL, sessionPath)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(sessionCtx, http.MethodPost, endpoint, bytes.NewReader(nil))
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("create telephony session: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("create telephony session: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload sessionResponse
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(&payload); err != nil {
		return "", fmt.Errorf("decode telephony session: %w", err)
	}
	if _, err := uuid.Parse(payload.SessionID); err != nil {
		return "", fmt.Errorf("invalid telephony session_id %q: %w", payload.SessionID, err)
	}
	return payload.SessionID, nil
}

type stream struct {
	ctx    context.Context
	input  chan []byte
	events chan backend.Event
	done   chan error
	closed chan struct{}
	cancel context.CancelFunc
	conn   *gorillaws.Conn
	log    *slog.Logger
	once   sync.Once
	mu     sync.Mutex

	// inputTranscoder and outputTranscoder are nil unless the matching
	// websocket.transcode direction is enabled, in which case audio flows
	// through them at the point it crosses the WebSocket boundary; PCMU
	// 8kHz bytes are all either side of this package ever sees or sends.
	inputTranscoder  *audio.InputTranscoder
	outputTranscoder *audio.OutputTranscoder
}

func connect(ctx context.Context, conf config.WebSocketConfig, sessionID string, log *slog.Logger) (*stream, error) {
	connectCtx, cancelConnect := context.WithTimeout(ctx, conf.ConnectTimeout)
	defer cancelConnect()

	rawURL, err := endpointURL(conf.BaseURL, websocketPath+url.PathEscape(sessionID))
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else {
		u.Scheme = "wss"
	}

	conn, resp, err := gorillaws.DefaultDialer.DialContext(connectCtx, u.String(), nil)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return nil, fmt.Errorf("connect telephony websocket: %w", err)
	}
	conn.SetReadLimit(conf.MaxMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(conf.ConnectTimeout))
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("wait for telephony ready event: %w", err)
	}
	if messageType != gorillaws.TextMessage {
		_ = conn.Close()
		return nil, errors.New("telephony websocket sent audio before ready")
	}
	event, err := decodeEvent(data)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if event.Type == "error" {
		_ = conn.Close()
		return nil, errors.New(event.Message)
	}
	if event.Type != "ready" || event.SessionID != sessionID {
		_ = conn.Close()
		return nil, fmt.Errorf("expected ready for session %s, received %q", sessionID, event.Type)
	}
	_ = conn.SetReadDeadline(time.Time{})

	streamCtx, cancel := context.WithCancel(ctx)
	out := &stream{
		ctx:    streamCtx,
		input:  make(chan []byte),
		events: make(chan backend.Event, 32),
		done:   make(chan error, 1),
		closed: make(chan struct{}),
		cancel: cancel,
		conn:   conn,
		log:    log,
	}
	if conf.Transcode.Input.Enabled {
		out.inputTranscoder = audio.NewInputTranscoder(conf.Transcode.Input.SampleRate)
	}
	if conf.Transcode.Output.Enabled {
		out.outputTranscoder = audio.NewOutputTranscoder(conf.Transcode.Output.SampleRate)
	}
	go out.sendLoop(streamCtx)
	go out.recvLoop(streamCtx)
	go func() {
		<-streamCtx.Done()
		_ = conn.Close()
	}()
	log.Info("telephony WebSocket ready", "session_id", sessionID,
		"transcode_input", conf.Transcode.Input.Enabled, "transcode_output", conf.Transcode.Output.Enabled)
	return out, nil
}

func (s *stream) Input() chan<- []byte         { return s.input }
func (s *stream) Events() <-chan backend.Event { return s.events }
func (s *stream) Done() <-chan error           { return s.done }

func (s *stream) Close() error {
	s.cancel()
	return s.conn.Close()
}

func (s *stream) finish(err error) {
	s.once.Do(func() {
		if s.ctx.Err() != nil || errors.Is(err, context.Canceled) || gorillaws.IsCloseError(err, gorillaws.CloseNormalClosure, gorillaws.CloseGoingAway) {
			err = nil
		}
		s.done <- err
		close(s.done)
		close(s.closed)
		s.cancel()
	})
}

func (s *stream) sendLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-s.input:
			if len(payload) == 0 {
				continue
			}
			var data []byte
			if s.inputTranscoder != nil {
				data = s.inputTranscoder.Transcode(payload)
				if len(data) == 0 {
					continue
				}
			} else {
				data = append([]byte(nil), payload...)
			}
			s.mu.Lock()
			_ = s.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			err := s.conn.WriteMessage(gorillaws.BinaryMessage, data)
			s.mu.Unlock()
			if err != nil {
				s.finish(err)
				return
			}
		}
	}
}

func (s *stream) recvLoop(ctx context.Context) {
	for {
		messageType, data, err := s.conn.ReadMessage()
		if err != nil {
			s.finish(err)
			return
		}
		switch messageType {
		case gorillaws.BinaryMessage:
			var audioBytes []byte
			if s.outputTranscoder != nil {
				audioBytes = s.outputTranscoder.Transcode(data)
				if len(audioBytes) == 0 {
					continue
				}
			} else {
				audioBytes = append([]byte(nil), data...)
			}
			if !s.emit(ctx, backend.Event{Type: backend.EventAudio, Audio: audioBytes}) {
				return
			}
		case gorillaws.TextMessage:
			event, err := decodeEvent(data)
			if err != nil {
				s.finish(err)
				return
			}
			stop, err := s.handleEvent(ctx, event)
			if err != nil {
				s.finish(err)
				return
			}
			if stop {
				s.finish(nil)
				return
			}
		default:
			s.finish(fmt.Errorf("unsupported telephony websocket frame type %d", messageType))
			return
		}
	}
}

type wireEvent struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
	Delta     string `json:"delta"`
	Message   string `json:"message"`
}

func decodeEvent(data []byte) (wireEvent, error) {
	var event wireEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return event, fmt.Errorf("decode telephony websocket event: %w", err)
	}
	if event.Type == "" {
		return event, errors.New("telephony websocket event has no type")
	}
	return event, nil
}

func (s *stream) handleEvent(ctx context.Context, event wireEvent) (bool, error) {
	switch event.Type {
	case "user_transcript":
		return false, s.emitError(ctx, backend.Event{Type: backend.EventUserTranscript, Text: event.Text})
	case "bot_transcript":
		return false, s.emitError(ctx, backend.Event{Type: backend.EventBotTranscript, Text: event.Delta})
	case "bot_done":
		return false, s.emitError(ctx, backend.Event{Type: backend.EventTurnComplete})
	case "barge_in":
		return false, s.emitError(ctx, backend.Event{Type: backend.EventBargeIn})
	case "transfer":
		if !s.emit(ctx, backend.Event{Type: backend.EventTransfer}) {
			return true, ctx.Err()
		}
		return true, backend.ErrTransfer
	case "end_session":
		if !s.emit(ctx, backend.Event{Type: backend.EventEndSession}) {
			return true, ctx.Err()
		}
		return true, backend.ErrAgentEnded
	case "go_away":
		if !s.emit(ctx, backend.Event{Type: backend.EventGoAway}) {
			return true, ctx.Err()
		}
		return true, backend.ErrGoAway
	case "error":
		return true, fmt.Errorf("telephony websocket error: %s", event.Message)
	case "ready":
		return true, errors.New("telephony websocket sent duplicate ready event")
	default:
		// Unrecognized event types are ignored rather than treated as fatal:
		// the backend may emit internal/lifecycle events outside this
		// relay's documented contract (e.g. orchestration signals), and none
		// of those should be able to tear down an otherwise-healthy call.
		s.log.Info("ignoring unrecognized telephony websocket event type", "type", event.Type)
		return false, nil
	}
}

func (s *stream) emitError(ctx context.Context, event backend.Event) error {
	if s.emit(ctx, event) {
		return nil
	}
	return ctx.Err()
}

func (s *stream) emit(ctx context.Context, event backend.Event) bool {
	select {
	case s.events <- event:
		return true
	case <-ctx.Done():
		return false
	case <-s.closed:
		return false
	}
}

func endpointURL(baseURL, path string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	u.Path = path
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
