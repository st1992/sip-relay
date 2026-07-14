package call

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"sip-relay/internal/calllog"
	"sip-relay/internal/ces"
	"sip-relay/internal/config"
	relayrtp "sip-relay/internal/rtp"
)

const (
	cesConnectTimeout   = 15 * time.Second
	finalizationTimeout = 2 * time.Minute
	pcmuSilenceByte     = 0xff
)

type Metadata struct {
	CallID  string
	ANI     string
	DNIS    string
	Headers map[string][]string
}

type EndReason string

const (
	EndReasonNone     EndReason = ""
	EndReasonUser     EndReason = "user_ended"
	EndReasonCESEnded EndReason = "ces_ended"
)

type Call struct {
	ID       string
	Metadata Metadata
	RTP      *relayrtp.Port
	Config   *config.Config
	Log      *slog.Logger
	done     chan struct{}
	mu       sync.Mutex
	cancel   context.CancelFunc
	started  bool
	closed   bool
	reason   EndReason
	doneOnce sync.Once
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
	c.mu.Lock()
	if c.closed || c.started {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.started = true
	c.mu.Unlock()

	go c.run(ctx)
}

func (c *Call) Done() <-chan struct{} {
	return c.done
}

func (c *Call) EndReason() EndReason {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

func (c *Call) Close() {
	c.CloseWithReason(EndReasonNone)
}

func (c *Call) CloseWithReason(reason EndReason) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if reason != EndReasonNone && c.reason == EndReasonNone {
		c.reason = reason
	}
	c.closed = true
	cancel := c.cancel
	started := c.started
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	_ = c.RTP.Close()
	if !started {
		c.finishDone()
	}
}

func (c *Call) run(ctx context.Context) {
	defer c.finishDone()
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

	mediaCtx, stopMedia := context.WithCancel(ctx)
	defer stopMedia()

	stats := &mediaStats{}
	mediaErr := make(chan error, 2)
	go func() {
		mediaErr <- c.sendRTPToCES(mediaCtx, stream, recorder, stats)
	}()
	go func() {
		mediaErr <- c.receiveCES(mediaCtx, stream, recorder, stats)
	}()

	defer func() {
		snapshot := stats.snapshot()
		c.Log.Info(
			"call media summary",
			"inbound_rtp_packets", snapshot.inboundPackets,
			"inbound_rtp_bytes", snapshot.inboundBytes,
			"ces_chunks_sent", snapshot.cesChunksSent,
			"ces_bytes_sent", snapshot.cesBytesSent,
			"ces_silence_chunks_sent", snapshot.silenceChunksSent,
			"ces_audio_events", snapshot.cesAudioEvents,
			"ces_audio_bytes", snapshot.cesAudioBytes,
		)
	}()

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
		case err := <-mediaErr:
			if err != nil {
				c.Log.Warn("media relay ended", "error", err)
			}
			return
		}
	}
}

func (c *Call) sendRTPToCES(ctx context.Context, stream ces.Stream, recorder *calllog.Recorder, stats *mediaStats) error {
	silence := make([]byte, relayrtp.SamplesPerFrame)
	for i := range silence {
		silence[i] = pcmuSilenceByte
	}

	ticker := time.NewTicker(relayrtp.FrameDuration)
	defer ticker.Stop()

	var firstRTPReceived bool
	var nextSilenceAt time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case payload, ok := <-c.RTP.Payloads():
			if !ok {
				return nil
			}
			stats.inboundPackets.Add(1)
			stats.inboundBytes.Add(uint64(len(payload)))
			if !firstRTPReceived {
				firstRTPReceived = true
				c.Log.Info("received first RTP audio payload", "bytes", len(payload))
			}
			c.Log.Debug("received RTP audio payload", "bytes", len(payload))
			if err := recorder.Write(payload); err != nil {
				c.Log.Warn("failed to write inbound call recording", "error", err)
			}
			if !c.sendCESAudio(ctx, stream, payload, stats, false) {
				return nil
			}
			nextSilenceAt = time.Now().Add(audioDuration(len(payload)))
		case now := <-ticker.C:
			if !firstRTPReceived || now.Before(nextSilenceAt) {
				continue
			}
			if stats.silenceChunksSent.Add(1) == 1 {
				c.Log.Info("padding CES input with PCMU silence", "bytes", len(silence))
			}
			if !c.sendCESAudio(ctx, stream, silence, stats, true) {
				return nil
			}
			nextSilenceAt = now.Add(relayrtp.FrameDuration)
		}
	}
}

func (c *Call) sendCESAudio(ctx context.Context, stream ces.Stream, payload []byte, stats *mediaStats, silence bool) bool {
	select {
	case stream.Input() <- payload:
		stats.cesChunksSent.Add(1)
		stats.cesBytesSent.Add(uint64(len(payload)))
		if stats.cesChunksSent.Load() == 1 {
			c.Log.Info("sent first audio chunk to CES", "bytes", len(payload))
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func audioDuration(bytes int) time.Duration {
	if bytes <= 0 {
		return relayrtp.FrameDuration
	}
	return time.Duration(bytes) * time.Second / relayrtp.SampleRate
}

type mediaStats struct {
	inboundPackets    atomic.Uint64
	inboundBytes      atomic.Uint64
	cesChunksSent     atomic.Uint64
	cesBytesSent      atomic.Uint64
	silenceChunksSent atomic.Uint64
	cesAudioEvents    atomic.Uint64
	cesAudioBytes     atomic.Uint64
}

type mediaStatsSnapshot struct {
	inboundPackets    uint64
	inboundBytes      uint64
	cesChunksSent     uint64
	cesBytesSent      uint64
	silenceChunksSent uint64
	cesAudioEvents    uint64
	cesAudioBytes     uint64
}

func (s *mediaStats) snapshot() mediaStatsSnapshot {
	return mediaStatsSnapshot{
		inboundPackets:    s.inboundPackets.Load(),
		inboundBytes:      s.inboundBytes.Load(),
		cesChunksSent:     s.cesChunksSent.Load(),
		cesBytesSent:      s.cesBytesSent.Load(),
		silenceChunksSent: s.silenceChunksSent.Load(),
		cesAudioEvents:    s.cesAudioEvents.Load(),
		cesAudioBytes:     s.cesAudioBytes.Load(),
	}
}

func (c *Call) receiveCES(ctx context.Context, stream ces.Stream, recorder *calllog.Recorder, stats *mediaStats) error {
	outboundRTP := newOutboundRTPWriter(ctx, c.RTP, c.Log)
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-stream.Events():
			if !ok {
				return nil
			}
			switch event.Type {
			case ces.EventAudio:
				stats.cesAudioEvents.Add(1)
				stats.cesAudioBytes.Add(uint64(len(event.Audio)))
				if err := recorder.Write(event.Audio); err != nil {
					c.Log.Warn("failed to write outbound call recording", "error", err)
				}
				if !outboundRTP.Enqueue(event.Audio) {
					return nil
				}
			case ces.EventText:
				c.Log.Info("received CES text output", "text", event.Text)
			case ces.EventInterruption:
				c.Log.Info("CES interruption signal received")
				if !outboundRTP.Interrupt() {
					return nil
				}
			case ces.EventTurnComplete:
				c.Log.Info("CES turn completed")
			case ces.EventRecognition:
				c.Log.Info("CES recognition result received")
			case ces.EventEndSession:
				c.Log.Info("CES ended session")
				c.setEndReason(EndReasonCESEnded)
				return nil
			case ces.EventGoAway:
				c.Log.Info("CES sent go-away")
				return nil
			}
		}
	}
}

type outboundRTPCommand struct {
	audio     []byte
	interrupt bool
}

type outboundRTPWriter struct {
	ctx      context.Context
	port     *relayrtp.Port
	log      *slog.Logger
	commands chan outboundRTPCommand
	write    chan []byte
}

func newOutboundRTPWriter(ctx context.Context, port *relayrtp.Port, log *slog.Logger) *outboundRTPWriter {
	writer := &outboundRTPWriter{
		ctx:      ctx,
		port:     port,
		log:      log,
		commands: make(chan outboundRTPCommand),
		write:    make(chan []byte),
	}
	go writer.manage()
	go writer.writeLoop()
	return writer
}

func (w *outboundRTPWriter) Enqueue(audio []byte) bool {
	select {
	case w.commands <- outboundRTPCommand{audio: audio}:
		return true
	case <-w.ctx.Done():
		return false
	}
}

func (w *outboundRTPWriter) Interrupt() bool {
	select {
	case w.commands <- outboundRTPCommand{interrupt: true}:
		return true
	case <-w.ctx.Done():
		return false
	}
}

func (w *outboundRTPWriter) manage() {
	defer close(w.write)

	var queue [][]byte
	for {
		if len(queue) == 0 {
			select {
			case <-w.ctx.Done():
				return
			case cmd := <-w.commands:
				queue = w.handleCommand(queue, cmd)
			}
			continue
		}

		select {
		case cmd := <-w.commands:
			queue = w.handleCommand(queue, cmd)
			continue
		default:
		}

		select {
		case <-w.ctx.Done():
			return
		case cmd := <-w.commands:
			queue = w.handleCommand(queue, cmd)
		case w.write <- queue[0]:
			queue[0] = nil
			queue = queue[1:]
		}
	}
}

func (w *outboundRTPWriter) handleCommand(queue [][]byte, cmd outboundRTPCommand) [][]byte {
	if cmd.interrupt {
		w.port.InterruptOutbound()
		for i := range queue {
			queue[i] = nil
		}
		return queue[:0]
	}
	if len(cmd.audio) == 0 {
		return queue
	}
	return append(queue, cmd.audio)
}

func (w *outboundRTPWriter) writeLoop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case audio, ok := <-w.write:
			if !ok {
				return
			}
			if err := w.port.WritePayload(audio); err != nil {
				w.log.Warn("failed to write RTP audio", "error", err)
			}
		}
	}
}

func (c *Call) setEndReason(reason EndReason) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reason == EndReasonNone {
		c.reason = reason
	}
}

func (c *Call) finishDone() {
	c.doneOnce.Do(func() {
		close(c.done)
	})
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
		ProjectID:      c.Config.CES.ProjectID,
		Location:       c.Config.CES.Location,
		AppID:          c.Config.CES.AppID,
		DeploymentID:   c.Config.CES.DeploymentID,
		ConversationID: c.ID,
		ANI:            c.Metadata.ANI,
		DNIS:           c.Metadata.DNIS,
		StartTime:      startedAt,
		EndTime:        endedAt,
		HangupReason:   c.hangupReason(),
	}); err != nil {
		c.Log.Error("failed to publish call log", "error", err)
	}
}

func (c *Call) hangupReason() string {
	switch c.EndReason() {
	case EndReasonCESEnded:
		return "AGENT_ENDED"
	default:
		return "USER_ENDED"
	}
}

func (c *Call) logCallID() string {
	if c.Metadata.CallID != "" {
		return c.Metadata.CallID
	}
	return c.ID
}
