package call

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sip-relay/internal/backend"
	"sip-relay/internal/calllog"
	"sip-relay/internal/config"
	relayrtp "sip-relay/internal/rtp"
)

const finalizationTimeout = 2 * time.Minute

type Metadata struct {
	CallID  string
	ANI     string
	DNIS    string
	Headers map[string][]string
	Profile string
}

type EndReason string

const (
	EndReasonNone     EndReason = ""
	EndReasonUser     EndReason = "user_ended"
	EndReasonAgent    EndReason = "agent_ended"
	EndReasonTransfer EndReason = "transfer"
	EndReasonBackend  EndReason = "backend_error"
)

type Call struct {
	ID       string
	Metadata Metadata
	RTP      *relayrtp.Port
	Config   *config.Config
	Backend  backend.Dialer
	CallLog  *calllog.Clients
	Log      *slog.Logger
	done     chan struct{}
	mu       sync.Mutex
	cancel   context.CancelFunc
	started  bool
	closed   bool
	reason   EndReason
	doneOnce sync.Once
}

func New(id string, metadata Metadata, cfg *config.Config, dialer backend.Dialer, callLog *calllog.Clients, port *relayrtp.Port, log *slog.Logger) *Call {
	if log == nil {
		log = slog.Default()
	}
	return &Call{
		ID:       id,
		Metadata: metadata,
		RTP:      port,
		Config:   cfg,
		Backend:  dialer,
		CallLog:  callLog,
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
	if c.RTP != nil {
		_ = c.RTP.Close()
	}
	if !started {
		c.finishDone()
	}
}

func (c *Call) run(ctx context.Context) {
	startedAt := time.Now().UTC()
	recorder, err := calllog.NewRecorder(c.Config.CallLog.RecordingBucket != "")
	if err != nil {
		c.Log.Error("failed to create call recorder", "error", err)
	}
	history := &conversationHistory{}
	// Register finalization first so media teardown and Done notification happen
	// before potentially slow storage or Pub/Sub operations.
	defer c.finish(startedAt, recorder, history)
	defer c.finishDone()
	defer c.Close()

	c.Log.Info("opening media backend", "backend", c.Backend.Name())
	stream, cancelBackend, err := c.dialBackend(ctx)
	if err != nil {
		c.setEndReason(EndReasonBackend)
		c.Log.Error("failed to open media backend", "backend", c.Backend.Name(), "error", err)
		return
	}
	defer cancelBackend()
	defer stream.Close()
	c.Log.Info("media backend opened", "backend", c.Backend.Name())

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
	// mediaWG.Wait, once mediaCtx is canceled, guarantees receiveBackend has
	// fully stopped (and so is done writing to history) before finish()
	// reads it below -- required for a correct, race-free conversation
	// history, not just tidiness. Defers run LIFO, so registering Wait
	// before stopMedia here means stopMedia (the cancel) runs first during
	// unwind and Wait (which needs that cancel to unblock the goroutines)
	// runs second; reversing this order would deadlock.
	//
	// Known tradeoff: recorder.Write (used by both goroutines below) is not
	// context-aware -- if it ever stalled, Wait would block the whole
	// call's finalization (Done(), Pub/Sub publish, recording upload)
	// instead of just leaking one goroutine as it would have before this
	// change. Accepted: it's a local temp-file write, not expected to
	// stall in practice.
	var mediaWG sync.WaitGroup
	defer mediaWG.Wait()
	defer stopMedia()

	stats := &mediaStats{}
	mediaErr := make(chan error, 2)
	mediaWG.Add(2)
	go func() {
		defer mediaWG.Done()
		mediaErr <- c.sendRTPToBackend(mediaCtx, stream, recorder, stats)
	}()
	go func() {
		defer mediaWG.Done()
		mediaErr <- c.receiveBackend(mediaCtx, stream, recorder, stats, history)
	}()

	defer func() {
		snapshot := stats.snapshot()
		c.Log.Info(
			"call media summary",
			"inbound_rtp_packets", snapshot.inboundPackets,
			"inbound_rtp_bytes", snapshot.inboundBytes,
			"backend_chunks_sent", snapshot.cesChunksSent,
			"backend_bytes_sent", snapshot.cesBytesSent,
			"backend_silence_chunks_sent", snapshot.silenceChunksSent,
			"backend_audio_events", snapshot.cesAudioEvents,
			"backend_audio_bytes", snapshot.cesAudioBytes,
			"outbound_audio_bytes_dropped", snapshot.outboundAudioBytesDropped,
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
			switch {
			case errors.Is(err, backend.ErrAgentEnded):
				c.setEndReason(EndReasonAgent)
			case errors.Is(err, backend.ErrTransfer):
				c.setEndReason(EndReasonTransfer)
			case errors.Is(err, backend.ErrGoAway):
				c.setEndReason(EndReasonBackend)
			case err != nil:
				c.Log.Warn("media backend stream ended", "backend", c.Backend.Name(), "error", err)
			}
			if ctx.Err() == nil && c.EndReason() == EndReasonNone {
				c.setEndReason(EndReasonBackend)
			}
			return
		case err := <-mediaErr:
			if err != nil {
				c.Log.Warn("media relay ended", "error", err)
				if ctx.Err() == nil && c.EndReason() == EndReasonNone {
					c.setEndReason(EndReasonBackend)
				}
			}
			return
		}
	}
}

func (c *Call) sendRTPToBackend(ctx context.Context, stream backend.Stream, recorder *calllog.Recorder, stats *mediaStats) error {
	silence := relayrtp.NewSilenceFrame()

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
			if !c.sendBackendAudio(ctx, stream, payload, stats) {
				return nil
			}
			nextSilenceAt = time.Now().Add(audioDuration(len(payload)))
		case now := <-ticker.C:
			if !firstRTPReceived || now.Before(nextSilenceAt) {
				continue
			}
			if stats.silenceChunksSent.Add(1) == 1 {
				c.Log.Info("padding backend input with PCMU silence", "bytes", len(silence))
			}
			if !c.sendBackendAudio(ctx, stream, silence, stats) {
				return nil
			}
			nextSilenceAt = now.Add(relayrtp.FrameDuration)
		}
	}
}

func (c *Call) sendBackendAudio(ctx context.Context, stream backend.Stream, payload []byte, stats *mediaStats) bool {
	select {
	case stream.Input() <- payload:
		stats.cesChunksSent.Add(1)
		stats.cesBytesSent.Add(uint64(len(payload)))
		if stats.cesChunksSent.Load() == 1 {
			c.Log.Info("sent first audio chunk to backend", "backend", c.Backend.Name(), "bytes", len(payload))
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
	inboundPackets            atomic.Uint64
	inboundBytes              atomic.Uint64
	cesChunksSent             atomic.Uint64
	cesBytesSent              atomic.Uint64
	silenceChunksSent         atomic.Uint64
	cesAudioEvents            atomic.Uint64
	cesAudioBytes             atomic.Uint64
	outboundAudioBytesDropped atomic.Uint64
}

type mediaStatsSnapshot struct {
	inboundPackets            uint64
	inboundBytes              uint64
	cesChunksSent             uint64
	cesBytesSent              uint64
	silenceChunksSent         uint64
	cesAudioEvents            uint64
	cesAudioBytes             uint64
	outboundAudioBytesDropped uint64
}

func (s *mediaStats) snapshot() mediaStatsSnapshot {
	return mediaStatsSnapshot{
		inboundPackets:            s.inboundPackets.Load(),
		inboundBytes:              s.inboundBytes.Load(),
		cesChunksSent:             s.cesChunksSent.Load(),
		cesBytesSent:              s.cesBytesSent.Load(),
		silenceChunksSent:         s.silenceChunksSent.Load(),
		cesAudioEvents:            s.cesAudioEvents.Load(),
		cesAudioBytes:             s.cesAudioBytes.Load(),
		outboundAudioBytesDropped: s.outboundAudioBytesDropped.Load(),
	}
}

// conversationHistory accumulates the call's spoken transcript for
// inclusion in the Pub/Sub call log. Bot transcript text arrives as
// streaming deltas (not complete messages) on both backends, so deltas are
// buffered and only turned into one ConversationEvent when a turn boundary
// is reached (EventTurnComplete, or EventBargeIn cutting a turn short).
// User transcript text has no equivalent turn-boundary signal on either
// backend, so each occurrence becomes its own entry immediately.
//
// Always used via *conversationHistory: it embeds a strings.Builder, which
// panics if copied after its first write.
type conversationHistory struct {
	entries  []calllog.ConversationEvent
	botText  strings.Builder
	botStart time.Time
	botEnd   time.Time
}

func (h *conversationHistory) appendBotDelta(text string) {
	if text == "" {
		return
	}
	if h.botText.Len() == 0 {
		h.botStart = time.Now().UTC()
	}
	h.botText.WriteString(text)
	h.botEnd = time.Now().UTC()
}

func (h *conversationHistory) flushBot() {
	if h.botText.Len() == 0 {
		return
	}
	h.entries = append(h.entries, calllog.ConversationEvent{
		Type:      "message",
		Role:      "bot",
		Text:      h.botText.String(),
		StartTime: h.botStart,
		EndTime:   h.botEnd,
	})
	h.botText.Reset()
	h.botStart = time.Time{}
	h.botEnd = time.Time{}
}

func (h *conversationHistory) appendUser(text string) {
	if text == "" {
		return
	}
	now := time.Now().UTC()
	h.entries = append(h.entries, calllog.ConversationEvent{
		Type:      "message",
		Role:      "user",
		Text:      text,
		StartTime: now,
		EndTime:   now,
	})
}

func (h *conversationHistory) snapshot() []calllog.ConversationEvent {
	return h.entries
}

func (c *Call) receiveBackend(ctx context.Context, stream backend.Stream, recorder *calllog.Recorder, stats *mediaStats, history *conversationHistory) error {
	// Catches a bot utterance still in progress on any exit path below
	// (ctx cancellation, events channel closing, transfer/end/go-away)
	// rather than silently dropping it.
	defer history.flushBot()

	outboundRTP := newOutboundRTPWriter(ctx, c.RTP, c.Log, stats)
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-stream.Events():
			if !ok {
				return errors.New("backend events channel closed unexpectedly")
			}
			switch event.Type {
			case backend.EventAudio:
				stats.cesAudioEvents.Add(1)
				stats.cesAudioBytes.Add(uint64(len(event.Audio)))
				if err := recorder.Write(event.Audio); err != nil {
					c.Log.Warn("failed to write outbound call recording", "error", err)
				}
				if !outboundRTP.Enqueue(event.Audio) {
					return nil
				}
			case backend.EventBotTranscript:
				c.Log.Info("received bot transcript", "text", event.Text)
				history.appendBotDelta(event.Text)
			case backend.EventUserTranscript:
				c.Log.Info("received user transcript", "text", event.Text)
				history.appendUser(event.Text)
			case backend.EventBargeIn:
				c.Log.Info("barge-in signal received")
				// A turn boundary too: without this, a bot utterance cut
				// off by barge-in would silently merge with the next
				// turn's deltas into one bogus entry.
				history.flushBot()
				if !outboundRTP.Interrupt() {
					return nil
				}
			case backend.EventTurnComplete:
				c.Log.Info("bot turn completed")
				history.flushBot()
			case backend.EventEndSession:
				c.Log.Info("backend ended session")
				c.setEndReason(EndReasonAgent)
				return nil
			case backend.EventTransfer:
				c.Log.Info("backend requested transfer")
				_ = outboundRTP.Interrupt()
				c.setEndReason(EndReasonTransfer)
				return nil
			case backend.EventGoAway:
				c.Log.Info("backend sent go-away")
				c.setEndReason(EndReasonBackend)
				return nil
			}
		}
	}
}

// maxQueuedOutboundBytes bounds how much backend audio can sit buffered,
// waiting to be paced out to the caller. It's sized generously (30s of
// PCMU) to absorb a legitimately bursty backend that streams a whole
// utterance faster than real-time; only a backend that persistently
// produces audio faster than the call can play it back will ever hit it.
const maxQueuedOutboundBytes = 30 * relayrtp.SampleRate

// minOutboundBufferBytes is the minimum amount of backend audio held in
// reserve before the pacer starts draining it to the caller -- and again
// after the buffer runs completely dry. Without this, real audio gets played
// the instant it arrives with nothing in reserve, so any further delivery
// jitter immediately causes another audible dropout: a repeating
// burst/silence stutter under chronically uneven backend delivery, not just
// an occasional glitch. This mirrors LiveKit SIP's own outbound jitter
// buffer (media-sdk's mixer.Input, gated on inputBufferMin and re-armed
// whenever a read comes back empty). 60ms (3 frames) absorbs typical
// delivery jitter at the cost of a small, fixed, and imperceptible amount of
// onset latency per utterance.
const minOutboundBufferBytes = 3 * relayrtp.SamplesPerFrame

// maxCatchUpFrames bounds how many frames a single tick may emit while
// repaying a scheduling deficit. Catch-up is what keeps the media clock on
// real time (see emitDue), but after a pathological stall -- a long GC pause,
// a descheduled container -- repaying the whole debt at once would burst
// hundreds of packets at the caller. Past this point the audio is too stale
// to be worth delivering, so the clock is re-anchored to now and the
// remaining debt written off. 5 frames (100ms) matches LiveKit's
// mixer.inputBufferFrames clamp.
const maxCatchUpFrames = 5

type outboundRTPCommand struct {
	audio     []byte
	interrupt bool
}

type outboundRTPWriter struct {
	ctx      context.Context
	port     *relayrtp.Port
	log      *slog.Logger
	stats    *mediaStats
	commands chan outboundRTPCommand

	// Everything below is owned exclusively by the run() goroutine and so
	// needs no synchronization of its own.
	ring      *byteRing
	frameBuf  []byte
	overCap   bool
	buffering bool
	// lastFrameEnd is the absolute deadline the media clock is anchored to:
	// the wall-clock instant the most recently emitted frame was due to end.
	// It advances by exact multiples of the frame duration, never by "now",
	// which is what stops timer overshoot from accumulating into drift.
	lastFrameEnd time.Time
}

func newOutboundRTPWriter(ctx context.Context, port *relayrtp.Port, log *slog.Logger, stats *mediaStats) *outboundRTPWriter {
	writer := &outboundRTPWriter{
		ctx:   ctx,
		port:  port,
		log:   log,
		stats: stats,
		// Buffered so a bursty backend can hand off several chunks without
		// blocking on the tick loop. Ordering is still exact: every command
		// is applied by run(), in the order it was sent.
		commands:  make(chan outboundRTPCommand, 64),
		ring:      newByteRing(maxQueuedOutboundBytes),
		frameBuf:  make([]byte, relayrtp.SamplesPerFrame),
		buffering: true,
	}
	go writer.run()
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

// run is the single owner of the outbound media clock. Buffering backend
// audio and emitting RTP both happen here, on one goroutine driven by one
// ticker, so there is exactly one clock deciding when a frame goes out.
//
// The previous design split this across two goroutines with two independent
// 20ms clocks -- a ticker here and a pacer inside rtp.Port -- which beat
// against each other: a pending tick could win the race against real audio
// that was already available and emit comfort noise in the middle of a word.
func (w *outboundRTPWriter) run() {
	ticker := time.NewTicker(relayrtp.FrameDuration)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case cmd := <-w.commands:
			w.handleCommand(cmd)
		case <-ticker.C:
			w.emitDue()
		}
	}
}

// emitDue emits however many frames the media clock currently owes.
//
// Timers only ever fire late, so scheduling each frame relative to when the
// previous one actually went out silently bakes every wake-up overshoot into
// the cadence: at a measured ~1.25ms of overshoot per 20ms frame the stream
// runs ~6% slow, the receiver's jitter buffer drains faster than it fills,
// and it conceals the shortfall by chopping words. Anchoring to an absolute
// deadline and emitting the backlog instead keeps the media clock locked to
// real time no matter how imprecise the timer is. This mirrors LiveKit's
// mixer.mixUpdate.
func (w *outboundRTPWriter) emitDue() {
	now := time.Now()
	if w.lastFrameEnd.IsZero() {
		w.lastFrameEnd = now
		w.emitFrame()
		return
	}

	dt := now.Sub(w.lastFrameEnd)
	if dt < 0 {
		// Woke a shade early; the frame isn't due yet.
		return
	}
	// Fuzz, so a tick arriving a hair under the deadline still counts as the
	// frame it was meant to be rather than deferring to the next tick.
	dt += relayrtp.FrameDuration / 4

	n := int(dt / relayrtp.FrameDuration)
	w.lastFrameEnd = w.lastFrameEnd.Add(time.Duration(n) * relayrtp.FrameDuration)
	if n > maxCatchUpFrames {
		n = maxCatchUpFrames
		w.lastFrameEnd = now
	}
	for i := 0; i < n; i++ {
		w.emitFrame()
	}
}

// emitFrame sends exactly one full frame, always. Whatever real audio is
// available fills the front of it and PCMU silence pads the rest, so backend
// chunk boundaries never reach the wire as short RTP packets: a short packet
// advances the RTP timestamp by less than the wall-clock time it occupies,
// which is the same starvation as drift by another route.
func (w *outboundRTPWriter) emitFrame() {
	frame := w.frameBuf
	n := 0
	if w.buffering {
		// Hold playout until the cushion is restocked.
		if w.ring.Len() >= minOutboundBufferBytes {
			w.buffering = false
			n = w.ring.Read(frame)
		}
	} else {
		n = w.ring.Read(frame)
		if n == 0 {
			// Genuinely dry -- not merely short. A partial read still plays
			// (padded); only a completely empty buffer re-arms the cushion.
			w.buffering = true
		}
	}
	for i := n; i < len(frame); i++ {
		frame[i] = relayrtp.SilenceByte
	}
	if err := w.port.WriteFrame(frame); err != nil {
		w.log.Warn("failed to write RTP frame", "error", err)
	}
}

func (w *outboundRTPWriter) handleCommand(cmd outboundRTPCommand) {
	if cmd.interrupt {
		// Barge-in: the bot is no longer saying this, so drop it rather than
		// playing it over the caller. Nothing is in flight inside the port --
		// WriteFrame never blocks -- so clearing the buffer is the whole job.
		w.ring.Reset()
		w.overCap = false
		w.buffering = true
		return
	}
	if len(cmd.audio) == 0 {
		return
	}

	accepted := w.ring.Write(cmd.audio)
	if dropped := len(cmd.audio) - accepted; dropped > 0 {
		if !w.overCap {
			w.overCap = true
			w.log.Warn("dropping backend audio: outbound RTP buffer exceeded its cap",
				"buffered_bytes", w.ring.Len(), "cap_bytes", w.ring.Cap())
		}
		if w.stats != nil {
			w.stats.outboundAudioBytesDropped.Add(uint64(dropped))
		}
		return
	}
	w.overCap = false
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

func (c *Call) dialBackend(ctx context.Context) (backend.Stream, context.CancelFunc, error) {
	if c.Backend == nil {
		return nil, nil, errors.New("media backend is not configured")
	}
	dialCtx, cancel := context.WithCancel(ctx)
	results := make(chan dialResult, 1)
	go func() {
		stream, err := c.Backend.Dial(dialCtx, c.ID, c.Log)
		if dialCtx.Err() != nil {
			if stream != nil {
				_ = stream.Close()
			}
			return
		}
		result := dialResult{stream: stream, err: err}
		results <- result
	}()

	timer := time.NewTimer(c.Backend.ConnectTimeout())
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
		return nil, nil, fmt.Errorf("timed out opening %s backend", c.Backend.Name())
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	}
}

type dialResult struct {
	stream backend.Stream
	err    error
}

func (c *Call) finish(startedAt time.Time, recorder *calllog.Recorder, history *conversationHistory) {
	endedAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), finalizationTimeout)
	defer cancel()
	if recorder != nil {
		if uri, err := recorder.Upload(ctx, c.CallLog, c.ID); err != nil {
			c.Log.Error("failed to upload call recording", "error", err)
		} else if uri != "" {
			c.Log.Info("uploaded call recording", "uri", uri)
		}
		if err := recorder.Remove(); err != nil {
			c.Log.Warn("failed to remove temporary call recording", "error", err)
		}
	}
	if err := c.CallLog.Publish(ctx, calllog.Entry{
		Backend:             c.Backend.Name(),
		Profile:             c.Metadata.Profile,
		Provider:            c.Backend.Metadata(),
		ConversationID:      c.ID,
		ANI:                 c.Metadata.ANI,
		DNIS:                c.Metadata.DNIS,
		StartTime:           startedAt,
		EndTime:             endedAt,
		HangupReason:        c.hangupReason(),
		ConversationHistory: history.snapshot(),
	}); err != nil {
		c.Log.Error("failed to publish call log", "error", err)
	}
}

func (c *Call) hangupReason() string {
	switch c.EndReason() {
	case EndReasonAgent:
		return "AGENT_ENDED"
	case EndReasonTransfer:
		return "TRANSFERRED"
	case EndReasonBackend:
		return "BACKEND_ERROR"
	default:
		return "USER_ENDED"
	}
}
