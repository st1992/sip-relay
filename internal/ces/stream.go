package ces

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	cesapi "cloud.google.com/go/ces/apiv1"
	cespb "cloud.google.com/go/ces/apiv1/cespb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/metadata"

	"sip-relay/internal/backend"
	"sip-relay/internal/config"
)

const initialTextInput = "hello"

type Options struct {
	Config    config.CESConfig
	SessionID string
	Log       *slog.Logger
}

type Dialer struct {
	Config config.CESConfig
}

func (d Dialer) Dial(ctx context.Context, sessionID string, log *slog.Logger) (backend.Stream, error) {
	return Dial(ctx, Options{Config: d.Config, SessionID: sessionID, Log: log})
}

func (d Dialer) Name() string { return config.BackendCES }

func (d Dialer) Metadata() map[string]string {
	return map[string]string{
		"project_id":    d.Config.ProjectID,
		"location":      d.Config.Location,
		"app_id":        d.Config.AppID,
		"deployment_id": d.Config.DeploymentID,
	}
}

func (d Dialer) ConnectTimeout() time.Duration { return 15 * time.Second }

func Dial(ctx context.Context, opts Options) (backend.Stream, error) {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return dialGRPC(ctx, opts)
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

func TextMessage(text string) *cespb.BidiSessionClientMessage {
	return &cespb.BidiSessionClientMessage{
		MessageType: &cespb.BidiSessionClientMessage_RealtimeInput{
			RealtimeInput: &cespb.SessionInput{
				InputType: &cespb.SessionInput_Text{Text: text},
			},
		},
	}
}

type baseStream struct {
	input  chan []byte
	events chan backend.Event
	done   chan error
	closed chan struct{}
	cancel context.CancelFunc
	once   sync.Once
}

func newBaseStream(cancel context.CancelFunc) baseStream {
	return baseStream{
		input:  make(chan []byte),
		events: make(chan backend.Event, 32),
		done:   make(chan error, 1),
		closed: make(chan struct{}),
		cancel: cancel,
	}
}

func (s *baseStream) Input() chan<- []byte {
	return s.input
}

func (s *baseStream) Events() <-chan backend.Event {
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
		close(s.closed)
		s.cancel()
		s.done <- err
		close(s.done)
	})
}

func (s *baseStream) emit(event backend.Event) bool {
	select {
	case s.events <- event:
		return true
	case <-s.closed:
		return false
	}
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

	opts.Log.Info("creating CES gRPC client", "endpoint", opts.Config.Endpoint, "location", opts.Config.Location)
	client, err := cesapi.NewSessionClient(ctx, clientOpts...)
	if err != nil {
		cancel()
		return nil, err
	}

	opts.Log.Info("opening CES BidiRunSession")
	ctx = metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", "location=locations/"+opts.Config.Location)
	rpcStream, err := client.BidiRunSession(ctx)
	if err != nil {
		_ = client.Close()
		cancel()
		return nil, err
	}
	opts.Log.Info("sending CES session config")
	if err := rpcStream.Send(ConfigMessage(opts.Config, opts.SessionID)); err != nil {
		_ = client.Close()
		cancel()
		return nil, err
	}
	opts.Log.Info("CES session config sent")
	opts.Log.Info("sending initial CES text input", "text", initialTextInput)
	if err := rpcStream.Send(TextMessage(initialTextInput)); err != nil {
		_ = client.Close()
		cancel()
		return nil, err
	}
	opts.Log.Info("initial CES text input sent")

	out := &grpcStream{baseStream: newBaseStream(cancel), client: client}
	go out.sendLoop(rpcStream)
	go out.recvLoop(rpcStream)
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
		case payload, ok := <-s.input:
			if !ok {
				return
			}
			if err := rpcStream.Send(AudioMessage(payload)); err != nil {
				s.finish(err)
				return
			}
		}
	}
}

func (s *grpcStream) recvLoop(rpcStream cespb.SessionService_BidiRunSessionClient) {
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
		if terminalErr := emitServerMessage(msg, s.emit); terminalErr != nil {
			_ = rpcStream.CloseSend()
			s.finish(terminalErr)
			return
		}
	}
}

func emitServerMessage(msg *cespb.BidiSessionServerMessage, emit func(backend.Event) bool) error {
	if out := msg.GetSessionOutput(); out != nil {
		if audio := out.GetAudio(); len(audio) > 0 {
			if !emit(backend.Event{Type: backend.EventAudio, Audio: append([]byte(nil), audio...)}) {
				return context.Canceled
			}
		}
		if text := out.GetText(); text != "" {
			if !emit(backend.Event{Type: backend.EventBotTranscript, Text: text}) {
				return context.Canceled
			}
		}
		if out.GetTurnCompleted() {
			if !emit(backend.Event{Type: backend.EventTurnComplete}) {
				return context.Canceled
			}
		}
		if out.GetEndSession() != nil {
			if !emit(backend.Event{Type: backend.EventEndSession}) {
				return context.Canceled
			}
			return backend.ErrAgentEnded
		}
		return nil
	}
	if rec := msg.GetRecognitionResult(); rec != nil {
		if !emit(backend.Event{Type: backend.EventUserTranscript}) {
			return context.Canceled
		}
		return nil
	}
	if msg.GetInterruptionSignal() != nil {
		if !emit(backend.Event{Type: backend.EventBargeIn}) {
			return context.Canceled
		}
		return nil
	}
	if msg.GetEndSession() != nil {
		if !emit(backend.Event{Type: backend.EventEndSession}) {
			return context.Canceled
		}
		return backend.ErrAgentEnded
	}
	if msg.GetGoAway() != nil {
		if !emit(backend.Event{Type: backend.EventGoAway}) {
			return context.Canceled
		}
		return backend.ErrGoAway
	}
	return nil
}
