package ces

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	cesapi "cloud.google.com/go/ces/apiv1"
	cespb "cloud.google.com/go/ces/apiv1/cespb"
	"github.com/gorilla/websocket"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"sip-relay/internal/config"
)

const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

type EventType int

const (
	EventAudio EventType = iota
	EventRecognition
	EventInterruption
	EventTurnComplete
	EventEndSession
	EventGoAway
)

type Event struct {
	Type  EventType
	Audio []byte
	Text  string
}

type Stream interface {
	Input() chan<- []byte
	Events() <-chan Event
	Done() <-chan error
	Close() error
}

type Options struct {
	Config    config.CESConfig
	SessionID string
	Log       *slog.Logger
}

func Dial(ctx context.Context, opts Options) (Stream, error) {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	switch opts.Config.Transport {
	case config.TransportGRPC:
		return dialGRPC(ctx, opts)
	case config.TransportWebSocket:
		return dialWebSocket(ctx, opts)
	default:
		return nil, fmt.Errorf("unsupported CES transport %q", opts.Config.Transport)
	}
}

func ConfigMessage(conf config.CESConfig, sessionID string) *cespb.BidiSessionClientMessage {
	sessionConfig := &cespb.SessionConfig{
		Session: conf.SessionResource(sessionID),
		InputAudioConfig: &cespb.InputAudioConfig{
			AudioEncoding:         cespb.AudioEncoding_MULAW,
			SampleRateHertz:       8000,
			NoiseSuppressionLevel: conf.NoiseSuppressionLevel,
		},
		OutputAudioConfig: &cespb.OutputAudioConfig{
			AudioEncoding:   cespb.AudioEncoding_MULAW,
			SampleRateHertz: 8000,
		},
		Deployment:   conf.DeploymentResource(),
		TimeZone:     conf.TimeZone,
		UseToolFakes: conf.UseToolFakes,
	}
	return &cespb.BidiSessionClientMessage{
		MessageType: &cespb.BidiSessionClientMessage_Config{Config: sessionConfig},
	}
}

func AudioMessage(payload []byte) *cespb.BidiSessionClientMessage {
	audio := make([]byte, len(payload))
	copy(audio, payload)
	return &cespb.BidiSessionClientMessage{
		MessageType: &cespb.BidiSessionClientMessage_RealtimeInput{
			RealtimeInput: &cespb.SessionInput{
				InputType: &cespb.SessionInput_Audio{Audio: audio},
			},
		},
	}
}

type baseStream struct {
	input  chan []byte
	events chan Event
	done   chan error
	cancel context.CancelFunc
	once   sync.Once
}

func newBaseStream(cancel context.CancelFunc) baseStream {
	return baseStream{
		input:  make(chan []byte, 128),
		events: make(chan Event),
		done:   make(chan error, 1),
		cancel: cancel,
	}
}

func (s *baseStream) Input() chan<- []byte {
	return s.input
}

func (s *baseStream) Events() <-chan Event {
	return s.events
}

func (s *baseStream) Done() <-chan error {
	return s.done
}

func (s *baseStream) finish(err error) {
	s.once.Do(func() {
		if err == context.Canceled {
			err = nil
		}
		s.done <- err
		close(s.done)
		close(s.events)
		s.cancel()
	})
}

type grpcStream struct {
	baseStream
	client *cesapi.SessionClient
}

func dialGRPC(ctx context.Context, opts Options) (*grpcStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	clientOpts := []option.ClientOption{}
	if opts.Config.Endpoint != "" {
		clientOpts = append(clientOpts, option.WithEndpoint(opts.Config.Endpoint))
	}
	if opts.Config.CredentialsFile != "" {
		clientOpts = append(clientOpts, option.WithCredentialsFile(opts.Config.CredentialsFile))
	}

	client, err := cesapi.NewSessionClient(ctx, clientOpts...)
	if err != nil {
		cancel()
		return nil, err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", "location=locations/"+opts.Config.Location)
	rpcStream, err := client.BidiRunSession(ctx)
	if err != nil {
		_ = client.Close()
		cancel()
		return nil, err
	}
	if err := rpcStream.Send(ConfigMessage(opts.Config, opts.SessionID)); err != nil {
		_ = client.Close()
		cancel()
		return nil, err
	}

	out := &grpcStream{baseStream: newBaseStream(cancel), client: client}
	go out.sendLoop(rpcStream)
	go out.recvLoop(rpcStream, opts.Log)
	return out, nil
}

func (s *grpcStream) Close() error {
	s.cancel()
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *grpcStream) sendLoop(rpcStream cespb.SessionService_BidiRunSessionClient) {
	defer func() { _ = rpcStream.CloseSend() }()
	for {
		select {
		case <-rpcStream.Context().Done():
			return
		case payload := <-s.input:
			if err := rpcStream.Send(AudioMessage(payload)); err != nil {
				s.finish(err)
				return
			}
		}
	}
}

func (s *grpcStream) recvLoop(rpcStream cespb.SessionService_BidiRunSessionClient, log *slog.Logger) {
	for {
		msg, err := rpcStream.Recv()
		if errors.Is(err, io.EOF) {
			s.finish(nil)
			return
		}
		if err != nil {
			s.finish(err)
			return
		}
		if stop := emitServerMessage(msg, s.events, log); stop {
			_ = rpcStream.CloseSend()
			s.finish(nil)
			return
		}
	}
}

type wsStream struct {
	baseStream
	conn *websocket.Conn
	mu   sync.Mutex
}

func dialWebSocket(ctx context.Context, opts Options) (*wsStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	tokenSource, err := tokenSource(ctx, opts.Config)
	if err != nil {
		cancel()
		return nil, err
	}
	token, err := tokenSource.Token()
	if err != nil {
		cancel()
		return nil, err
	}

	header := http.Header{}
	header.Set("Authorization", "Bearer "+token.AccessToken)
	header.Set("x-goog-request-params", "location=locations/"+opts.Config.Location)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, websocketURL(opts.Config), header)
	if err != nil {
		cancel()
		return nil, err
	}

	out := &wsStream{baseStream: newBaseStream(cancel), conn: conn}
	if err := out.writeProto(ConfigMessage(opts.Config, opts.SessionID)); err != nil {
		_ = conn.Close()
		cancel()
		return nil, err
	}
	go out.sendLoop()
	go out.recvLoop(opts.Log)
	return out, nil
}

func (s *wsStream) Close() error {
	s.cancel()
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *wsStream) sendLoop() {
	for {
		select {
		case <-s.done:
			return
		case payload := <-s.input:
			if err := s.writeProto(AudioMessage(payload)); err != nil {
				s.finish(err)
				return
			}
		}
	}
}

func (s *wsStream) recvLoop(log *slog.Logger) {
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				s.finish(nil)
			} else {
				s.finish(err)
			}
			return
		}
		var msg cespb.BidiSessionServerMessage
		if err := protojson.Unmarshal(data, &msg); err != nil {
			log.Debug("dropping malformed CES WebSocket message", "error", err)
			continue
		}
		if stop := emitServerMessage(&msg, s.events, log); stop {
			s.finish(nil)
			return
		}
	}
}

func (s *wsStream) writeProto(msg *cespb.BidiSessionClientMessage) error {
	data, err := protojson.MarshalOptions{UseProtoNames: false}.Marshal(msg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(websocket.TextMessage, data)
}

func emitServerMessage(msg *cespb.BidiSessionServerMessage, events chan<- Event, log *slog.Logger) bool {
	if out := msg.GetSessionOutput(); out != nil {
		if audio := out.GetAudio(); len(audio) > 0 {
			events <- Event{Type: EventAudio, Audio: append([]byte(nil), audio...)}
		}
		if text := out.GetText(); text != "" {
			log.Debug("CES text output", "text", text)
		}
		if out.GetTurnCompleted() {
			events <- Event{Type: EventTurnComplete}
		}
		if out.GetEndSession() != nil {
			events <- Event{Type: EventEndSession}
			return true
		}
		return false
	}
	if rec := msg.GetRecognitionResult(); rec != nil {
		events <- Event{Type: EventRecognition}
		return false
	}
	if msg.GetInterruptionSignal() != nil {
		events <- Event{Type: EventInterruption}
		return false
	}
	if msg.GetEndSession() != nil {
		events <- Event{Type: EventEndSession}
		return true
	}
	if msg.GetGoAway() != nil {
		events <- Event{Type: EventGoAway}
		return true
	}
	return false
}

func tokenSource(ctx context.Context, conf config.CESConfig) (oauth2.TokenSource, error) {
	if conf.CredentialsFile != "" {
		data, err := os.ReadFile(conf.CredentialsFile)
		if err != nil {
			return nil, err
		}
		creds, err := google.CredentialsFromJSON(ctx, data, cloudPlatformScope)
		if err != nil {
			return nil, err
		}
		return creds.TokenSource, nil
	}
	creds, err := google.FindDefaultCredentials(ctx, cloudPlatformScope)
	if err != nil {
		return nil, err
	}
	return creds.TokenSource, nil
}

func websocketURL(conf config.CESConfig) string {
	if strings.HasPrefix(conf.Endpoint, "ws://") || strings.HasPrefix(conf.Endpoint, "wss://") {
		return conf.Endpoint
	}
	host := conf.Endpoint
	if host == "" {
		host = "ces.googleapis.com"
	}
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimSuffix(host, ":443")
	return fmt.Sprintf("wss://%s/ws/google.cloud.ces.v1.SessionService/BidiRunSession/locations/%s", host, conf.Location)
}
