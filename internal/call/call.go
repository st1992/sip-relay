package call

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"sip-relay/internal/calllog"
	"sip-relay/internal/ces"
	"sip-relay/internal/config"
	relayrtp "sip-relay/internal/rtp"
)

const (
	cesConnectTimeout   = 15 * time.Second
	finalizationTimeout = 2 * time.Minute
)

type Metadata struct {
	CallID  string
	ANI     string
	DNIS    string
	Headers map[string][]string
}

type Call struct {
	ID       string
	Metadata Metadata
	RTP      *relayrtp.Port
	Config   *config.Config
	Log      *slog.Logger
	done     chan struct{}
	cancel   context.CancelFunc
	closeMux sync.Once
}

func New(id string, metadata Metadata, cfg *config.Config, port *relayrtp.Port, log *slog.Logger) *Call {
	if log == nil {
		log = slog.Default()
	}
	return &Call{
		ID:       id,
		Metadata: metadata,
		RTP:      port,
		Config:   cfg,
		Log:      log,
		done:     make(chan struct{}),
	}
}

func (c *Call) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	go c.run(ctx)
}

func (c *Call) Done() <-chan struct{} {
	return c.done
}

func (c *Call) Close() {
	c.closeMux.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		_ = c.RTP.Close()
	})
}

func (c *Call) run(ctx context.Context) {
	defer close(c.done)
	defer c.Close()

	startedAt := time.Now().UTC()
	recorder, err := calllog.NewRecorder(c.Config.CallLog.RecordingBucket != "")
	if err != nil {
		c.Log.Error("failed to create call recorder", "error", err)
	}
	defer c.finish(startedAt, recorder)

	c.Log.Info("opening CES stream")
	stream, cancelCES, err := c.dialCES(ctx)
	if err != nil {
		c.Log.Error("failed to open CES stream", "error", err)
		return
	}
	defer cancelCES()
	defer stream.Close()
	c.Log.Info("CES stream opened")

	rtpErr := make(chan error, 1)
	go func() {
		c.Log.Info("starting RTP media loop")
		err := c.RTP.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		c.Log.Info("RTP media loop stopped", "error", err)
		rtpErr <- err
	}()

	inputChunkBytes := c.Config.RTP.InputChunkMS * relayrtp.SampleRate / 1000
	if inputChunkBytes <= 0 {
		inputChunkBytes = 320
	}
	var inputBuf []byte

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-rtpErr:
			if err != nil {
				c.Log.Warn("RTP loop ended", "error", err)
			}
			return
		case err := <-stream.Done():
			if err != nil {
				c.Log.Warn("CES stream ended", "error", err)
			}
			return
		case payload, ok := <-c.RTP.Payloads():
			if !ok {
				return
			}
			c.Log.Debug("received RTP audio payload", "bytes", len(payload))
			if err := recorder.Write(payload); err != nil {
				c.Log.Warn("failed to write inbound call recording", "error", err)
			}
			inputBuf = append(inputBuf, payload...)
			for len(inputBuf) >= inputChunkBytes {
				chunk := append([]byte(nil), inputBuf[:inputChunkBytes]...)
				inputBuf = inputBuf[inputChunkBytes:]
				select {
				case stream.Input() <- chunk:
				case <-ctx.Done():
					return
				}
			}
		case event, ok := <-stream.Events():
			if !ok {
				return
			}
			switch event.Type {
			case ces.EventAudio:
				c.Log.Debug("received CES audio payload", "bytes", len(event.Audio))
				if err := recorder.Write(event.Audio); err != nil {
					c.Log.Warn("failed to write outbound call recording", "error", err)
				}
				if err := c.RTP.WritePayload(event.Audio); err != nil {
					c.Log.Warn("failed to write RTP audio", "error", err)
				}
			case ces.EventInterruption:
				c.Log.Debug("CES interruption signal received")
			case ces.EventTurnComplete:
				c.Log.Debug("CES turn completed")
			case ces.EventRecognition:
				c.Log.Debug("CES recognition result received")
			case ces.EventEndSession:
				c.Log.Info("CES ended session")
				return
			case ces.EventGoAway:
				c.Log.Info("CES sent go-away")
				return
			}
		}
	}
}

func (c *Call) dialCES(ctx context.Context) (ces.Stream, context.CancelFunc, error) {
	dialCtx, cancel := context.WithCancel(ctx)
	results := make(chan dialResult, 1)
	go func() {
		stream, err := ces.Dial(dialCtx, ces.Options{
			Config:    c.Config.CES,
			SessionID: c.ID,
			Log:       c.Log,
		})
		if dialCtx.Err() != nil {
			if stream != nil {
				_ = stream.Close()
			}
			return
		}
		result := dialResult{stream: stream, err: err}
		results <- result
	}()

	timer := time.NewTimer(cesConnectTimeout)
	defer timer.Stop()

	select {
	case result := <-results:
		if result.err != nil {
			cancel()
			return nil, nil, result.err
		}
		return result.stream, cancel, nil
	case <-timer.C:
		cancel()
		return nil, nil, errors.New("timed out opening CES stream")
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	}
}

type dialResult struct {
	stream ces.Stream
	err    error
}

func (c *Call) finish(startedAt time.Time, recorder *calllog.Recorder) {
	endedAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer cancel()
	if recorder != nil {
		if uri, err := recorder.Upload(ctx, c.Config, c.logCallID()); err != nil {
			c.Log.Error("failed to upload call recording", "error", err)
		} else if uri != "" {
			c.Log.Info("uploaded call recording", "uri", uri)
		}
		if err := recorder.Remove(); err != nil {
			c.Log.Warn("failed to remove temporary call recording", "error", err)
		}
	}
	if err := calllog.Publish(ctx, c.Config, calllog.Entry{
		CallID:    c.logCallID(),
		ANI:       c.Metadata.ANI,
		DNIS:      c.Metadata.DNIS,
		StartedAt: startedAt,
		EndedAt:   endedAt,
		Metadata:  c.Metadata.Headers,
	}); err != nil {
		c.Log.Error("failed to publish call log", "error", err)
	}
}

func (c *Call) logCallID() string {
	if c.Metadata.CallID != "" {
		return c.Metadata.CallID
	}
	return c.ID
}
