// Command loadtest drives many concurrent call.Call instances directly
// against an in-process mock backend that delivers audio in bursty, jittery
// chunks (simulating TTS generation pauses and network jitter to CES or the
// telephony WebSocket). It measures the RTP inter-packet gaps and
// comfort-noise fill ratio actually produced on the wire to the "phone",
// which is the thing that determines whether playout sounds smooth.
//
// It bypasses SIP signaling and the real CES/WebSocket backends on purpose:
// what's under test is the pacing/underrun behavior in internal/call and
// internal/rtp, not the SIP stack or third-party services.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	pionrtp "github.com/pion/rtp"

	"sip-relay/internal/backend"
	"sip-relay/internal/call"
	"sip-relay/internal/config"
	relayrtp "sip-relay/internal/rtp"
)

func main() {
	calls := flag.Int("calls", 50, "number of concurrent simulated calls")
	duration := flag.Duration("duration", 20*time.Second, "how long each simulated call runs")
	stallProb := flag.Float64("stall-prob", 0.15, "probability that a backend delivery step stalls")
	stallMin := flag.Duration("stall-min", 150*time.Millisecond, "minimum injected backend stall")
	stallMax := flag.Duration("stall-max", 400*time.Millisecond, "maximum injected backend stall")
	seed := flag.Int64("seed", time.Now().UnixNano(), "PRNG seed")
	flag.Parse()

	if *stallMax < *stallMin {
		fmt.Fprintln(os.Stderr, "stall-max must be >= stall-min")
		os.Exit(1)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	jitter := jitterProfile{stallProb: *stallProb, stallMin: *stallMin, stallMax: *stallMax}

	fmt.Printf("running %d concurrent simulated calls for %s (backend stall: %.0f%% chance, %s-%s)\n",
		*calls, *duration, *stallProb*100, *stallMin, *stallMax)

	results := make(chan callResult, *calls)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *calls; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(*seed + int64(idx)))
			results <- runSimulatedCall(idx, *duration, jitter, rng, log)
		}(i)
	}
	wg.Wait()
	close(results)

	report(time.Since(start), *calls, results)
}

type jitterProfile struct {
	stallProb          float64
	stallMin, stallMax time.Duration
}

type callResult struct {
	idx           int
	gaps          []time.Duration
	silenceFrames int
	realFrames    int
	err           error
}

// runSimulatedCall wires up one call.Call against a real loopback RTP port
// (standing in for the SIP phone) and a mock backend, then records what
// actually arrives at the "phone" socket for the duration of the call.
func runSimulatedCall(idx int, duration time.Duration, jitter jitterProfile, rng *rand.Rand, log *slog.Logger) callResult {
	port, err := relayrtp.Listen(relayrtp.Config{
		ListenIP:            "127.0.0.1",
		PortMin:             30000,
		PortMax:             40000,
		PayloadType:         0,
		MediaTimeoutInitial: duration + 5*time.Second,
		MediaTimeout:        duration + 5*time.Second,
		Log:                 log,
	})
	if err != nil {
		return callResult{idx: idx, err: fmt.Errorf("listen: %w", err)}
	}
	defer port.Close()

	phone, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		return callResult{idx: idx, err: fmt.Errorf("phone listen: %w", err)}
	}
	defer phone.Close()
	port.SetRemote(phone.LocalAddr().(*net.UDPAddr))

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	dialer := &mockDialer{jitter: jitter, rng: rng}
	c := call.New(
		fmt.Sprintf("loadtest-%d", idx),
		call.Metadata{CallID: fmt.Sprintf("loadtest-%d", idx)},
		&config.Config{},
		dialer,
		nil,
		port,
		log,
	)
	c.Start(ctx)

	go feedInboundRTP(ctx, port.LocalPort(), phone)

	col := collectOutbound(phone, duration)
	<-c.Done()
	c.Close()

	return callResult{idx: idx, gaps: col.gaps, silenceFrames: col.silence, realFrames: col.real}
}

// feedInboundRTP simulates the caller sending steady PCMU audio so the
// relay's own media timeout never fires and the caller->backend path stays
// exercised, matching a real call.
func feedInboundRTP(ctx context.Context, dstPort int, phone *net.UDPConn) {
	dst := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: dstPort}
	ticker := time.NewTicker(relayrtp.FrameDuration)
	defer ticker.Stop()

	payload := make([]byte, relayrtp.SamplesPerFrame)
	for i := range payload {
		payload[i] = relayrtp.SilenceByte
	}

	var seq uint16 = 1
	var ts uint32
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pkt := pionrtp.Packet{
				Header:  pionrtp.Header{Version: 2, PayloadType: 0, SequenceNumber: seq, Timestamp: ts, SSRC: 99},
				Payload: payload,
			}
			if raw, err := pkt.Marshal(); err == nil {
				_, _ = phone.WriteToUDP(raw, dst)
			}
			seq++
			ts += uint32(len(payload))
		}
	}
}

type collected struct {
	gaps    []time.Duration
	silence int
	real    int
}

// collectOutbound reads every RTP packet the relay sends to the simulated
// phone and records the wall-clock gap between consecutive arrivals -- the
// thing a listener would actually perceive as smooth or choppy audio.
func collectOutbound(phone *net.UDPConn, duration time.Duration) collected {
	var res collected
	var last time.Time
	buf := make([]byte, 1500)
	deadline := time.Now().Add(duration + time.Second)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return res
		}
		_ = phone.SetReadDeadline(time.Now().Add(remaining))
		n, _, err := phone.ReadFromUDP(buf)
		if err != nil {
			return res
		}
		now := time.Now()

		var pkt pionrtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		if !last.IsZero() {
			res.gaps = append(res.gaps, now.Sub(last))
		}
		last = now
		if isSilence(pkt.Payload) {
			res.silence++
		} else {
			res.real++
		}
	}
}

func isSilence(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	for _, b := range payload {
		if b != relayrtp.SilenceByte {
			return false
		}
	}
	return true
}

// mockDialer stands in for the CES/WebSocket backend.Dialer. Each call gets
// its own instance so the per-call *rand.Rand is never shared across
// goroutines.
type mockDialer struct {
	jitter jitterProfile
	rng    *rand.Rand
}

func (d *mockDialer) Dial(ctx context.Context, sessionID string, log *slog.Logger) (backend.Stream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	s := &mockStream{
		input:  make(chan []byte),
		events: make(chan backend.Event, 32),
		done:   make(chan error, 1),
		cancel: cancel,
	}
	go s.drainInput(streamCtx)
	go s.talk(streamCtx, d.jitter, d.rng)
	return s, nil
}

func (d *mockDialer) Name() string                  { return "loadtest" }
func (d *mockDialer) Metadata() map[string]string   { return nil }
func (d *mockDialer) ConnectTimeout() time.Duration { return 5 * time.Second }

// mockStream simulates a conversational backend: it alternates "thinking"
// pauses with utterances streamed as jittery, bursty chunks -- never
// perfectly paced 20ms frames, which is how real TTS/network delivery
// actually behaves. It deliberately never closes its events channel, since
// neither real backend does; the call ends via context cancellation instead.
type mockStream struct {
	input  chan []byte
	events chan backend.Event
	done   chan error
	cancel context.CancelFunc
}

func (s *mockStream) Input() chan<- []byte         { return s.input }
func (s *mockStream) Events() <-chan backend.Event { return s.events }
func (s *mockStream) Done() <-chan error           { return s.done }
func (s *mockStream) Close() error                 { s.cancel(); return nil }

func (s *mockStream) drainInput(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.input:
		}
	}
}

func (s *mockStream) talk(ctx context.Context, jitter jitterProfile, rng *rand.Rand) {
	for {
		think := time.Duration(300+rng.Intn(700)) * time.Millisecond
		if !sleepCtx(ctx, think) {
			return
		}
		if !s.speakUtterance(ctx, jitter, rng) {
			return
		}
	}
}

// speakUtterance streams ~2 seconds of synthetic PCMU audio as ~100ms
// delivery chunks, each delayed by a jittered interval that occasionally
// (per jitterProfile) stalls hard -- the network/TTS hiccup this whole
// exercise is meant to expose.
func (s *mockStream) speakUtterance(ctx context.Context, jitter jitterProfile, rng *rand.Rand) bool {
	const totalBytes = 8000 * 2
	const chunkBytes = 800

	sent := 0
	for sent < totalBytes {
		n := chunkBytes
		if sent+n > totalBytes {
			n = totalBytes - sent
		}
		chunk := make([]byte, n)
		for i := range chunk {
			chunk[i] = 0x01 // any non-silence byte marks this as "real" audio
		}

		delay := time.Duration(80+rng.Intn(40)) * time.Millisecond
		if rng.Float64() < jitter.stallProb {
			delay += jitter.stallMin + time.Duration(rng.Int63n(int64(jitter.stallMax-jitter.stallMin+1)))
		}
		if !sleepCtx(ctx, delay) {
			return false
		}

		select {
		case s.events <- backend.Event{Type: backend.EventAudio, Audio: chunk}:
		case <-ctx.Done():
			return false
		}
		sent += n
	}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func report(elapsed time.Duration, numCalls int, results <-chan callResult) {
	var allGaps []time.Duration
	var totalSilence, totalReal, errs int
	for r := range results {
		if r.err != nil {
			errs++
			fmt.Fprintf(os.Stderr, "call %d error: %v\n", r.idx, r.err)
			continue
		}
		allGaps = append(allGaps, r.gaps...)
		totalSilence += r.silenceFrames
		totalReal += r.realFrames
	}
	sort.Slice(allGaps, func(i, j int) bool { return allGaps[i] < allGaps[j] })

	fmt.Printf("\nsimulated calls: %d (errors: %d)\n", numCalls, errs)
	fmt.Printf("wall time: %s\n", elapsed)
	if errs == numCalls {
		fmt.Println("all calls failed; no RTP data collected")
		return
	}
	total := totalReal + totalSilence
	fillPct := 0.0
	if total > 0 {
		fillPct = 100 * float64(totalSilence) / float64(total)
	}
	fmt.Printf("outbound RTP frames observed: %d real, %d comfort-noise (%.1f%% fill)\n", totalReal, totalSilence, fillPct)

	if len(allGaps) == 0 {
		fmt.Println("no inter-packet gaps recorded")
		return
	}
	fmt.Printf("inter-packet gap: p50=%s p95=%s p99=%s max=%s\n",
		pct(allGaps, 0.50), pct(allGaps, 0.95), pct(allGaps, 0.99), allGaps[len(allGaps)-1])

	const threshold = 30 * time.Millisecond
	over := 0
	for _, g := range allGaps {
		if g > threshold {
			over++
		}
	}
	fmt.Printf("gaps over %s (missed a 20ms outbound slot): %d / %d (%.3f%%)\n",
		threshold, over, len(allGaps), 100*float64(over)/float64(len(allGaps)))

	if over == 0 {
		fmt.Println("=> outbound RTP stream stayed continuous through every injected backend stall")
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}
