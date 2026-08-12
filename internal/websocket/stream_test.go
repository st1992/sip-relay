package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"sip-relay/internal/audio"
	"sip-relay/internal/backend"
	"sip-relay/internal/config"
)

const testSessionID = "a1b2c3d4-e5f6-4890-abcd-ef1234567890"

func TestDialRelaysBinaryAudioAndEvents(t *testing.T) {
	upgrader := gorillaws.Upgrader{}
	inbound := make(chan []byte, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("session method = %s", r.Method)
		}
		if r.ContentLength != 0 {
			t.Errorf("session content length = %d", r.ContentLength)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("session request unexpectedly included authorization")
		}
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(wireEvent{Type: "ready", SessionID: testSessionID})
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != gorillaws.BinaryMessage {
			t.Errorf("message type = %d", messageType)
		}
		inbound <- payload
		_ = conn.WriteMessage(gorillaws.BinaryMessage, []byte{4, 5, 6})
		_ = conn.WriteJSON(wireEvent{Type: "user_transcript", Text: "hello"})
		_ = conn.WriteJSON(wireEvent{Type: "bot_transcript", Delta: "hi"})
		_ = conn.WriteJSON(wireEvent{Type: "bot_done"})
		_ = conn.WriteJSON(wireEvent{Type: "barge_in"})
		_ = conn.WriteJSON(wireEvent{Type: "transfer"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	stream, err := Dialer{Config: testConfig(server.URL)}.Dial(context.Background(), "local-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	stream.Input() <- []byte{1, 2, 3}
	if got := <-inbound; string(got) != string([]byte{1, 2, 3}) {
		t.Fatalf("inbound audio = %v", got)
	}

	want := []backend.EventType{
		backend.EventAudio,
		backend.EventUserTranscript,
		backend.EventBotTranscript,
		backend.EventTurnComplete,
		backend.EventBargeIn,
		backend.EventTransfer,
	}
	for _, eventType := range want {
		select {
		case event := <-stream.Events():
			if event.Type != eventType {
				t.Fatalf("event = %v, want %v", event.Type, eventType)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %v", eventType)
		}
	}
	if err := <-stream.Done(); !errors.Is(err, backend.ErrTransfer) {
		t.Fatalf("stream error = %v, want transfer", err)
	}
}

func TestDialRejectsServerErrorBeforeReady(t *testing.T) {
	upgrader := gorillaws.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(wireEvent{Type: "error", Message: "Unknown session"})
		_ = conn.WriteControl(gorillaws.CloseMessage, gorillaws.FormatCloseMessage(4404, "unknown"), time.Now().Add(time.Second))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	if _, err := (Dialer{Config: testConfig(server.URL)}).Dial(context.Background(), "", nil); err == nil {
		t.Fatal("Dial() succeeded")
	}
}

func TestCreateSessionRejectsNonOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	if _, err := createSession(context.Background(), testConfig(server.URL)); err == nil {
		t.Fatal("createSession() succeeded")
	}
}

func TestDialRejectsAudioBeforeReady(t *testing.T) {
	upgrader := gorillaws.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(gorillaws.BinaryMessage, []byte{1})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	if _, err := (Dialer{Config: testConfig(server.URL)}).Dial(context.Background(), "", nil); err == nil {
		t.Fatal("Dial() accepted audio before ready")
	}
}

func TestStreamReportsMalformedJSONEvent(t *testing.T) {
	upgrader := gorillaws.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(wireEvent{Type: "ready", SessionID: testSessionID})
		_ = conn.WriteMessage(gorillaws.TextMessage, []byte("{"))
		<-time.After(100 * time.Millisecond)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	stream, err := (Dialer{Config: testConfig(server.URL)}).Dial(context.Background(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err := <-stream.Done(); err == nil {
		t.Fatal("malformed event did not fail stream")
	}
}

func TestLifecycleEventsTerminateWithTypedReasons(t *testing.T) {
	tests := []struct {
		eventType string
		wantEvent backend.EventType
		wantErr   error
	}{
		{"end_session", backend.EventEndSession, backend.ErrAgentEnded},
		{"go_away", backend.EventGoAway, backend.ErrGoAway},
	}
	for _, test := range tests {
		t.Run(test.eventType, func(t *testing.T) {
			upgrader := gorillaws.Upgrader{}
			mux := http.NewServeMux()
			mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
			})
			mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.WriteJSON(wireEvent{Type: "ready", SessionID: testSessionID})
				_ = conn.WriteJSON(wireEvent{Type: test.eventType})
				<-time.After(100 * time.Millisecond)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			stream, err := (Dialer{Config: testConfig(server.URL)}).Dial(context.Background(), "", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			if event := <-stream.Events(); event.Type != test.wantEvent {
				t.Fatalf("event = %v, want %v", event.Type, test.wantEvent)
			}
			if err := <-stream.Done(); !errors.Is(err, test.wantErr) {
				t.Fatalf("Done() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestTranscodeDisabledByDefaultIsByteExact(t *testing.T) {
	upgrader := gorillaws.Upgrader{}
	inbound := make(chan []byte, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(wireEvent{Type: "ready", SessionID: testSessionID})
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		inbound <- payload
		_ = conn.WriteMessage(gorillaws.BinaryMessage, []byte{4, 5, 6})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	// testConfig leaves Transcode at its zero value (both directions
	// disabled), matching config.Default()'s opt-out posture.
	stream, err := Dialer{Config: testConfig(server.URL)}.Dial(context.Background(), "local-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	sentPCMU := []byte{1, 2, 3}
	stream.Input() <- sentPCMU
	if got := <-inbound; string(got) != string(sentPCMU) {
		t.Fatalf("sent bytes = %v, want unchanged %v", got, sentPCMU)
	}

	select {
	case event := <-stream.Events():
		if event.Type != backend.EventAudio || string(event.Audio) != string([]byte{4, 5, 6}) {
			t.Fatalf("event = %+v, want EventAudio with unchanged bytes [4 5 6]", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for audio event")
	}
}

func TestSendLoopTranscodesInputWhenEnabled(t *testing.T) {
	upgrader := gorillaws.Upgrader{}
	inbound := make(chan []byte, 1)
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(wireEvent{Type: "ready", SessionID: testSessionID})
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		inbound <- payload
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.Transcode.Input = config.TranscodeDirectionConfig{Enabled: true, SampleRate: 16000}

	stream, err := Dialer{Config: cfg}.Dial(context.Background(), "local-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	pcmu := []byte{0xFF, 0x7F, 0x00, 0x80, 0x55, 0xAA}
	stream.Input() <- pcmu

	want := audio.NewInputTranscoder(16000).Transcode(pcmu)
	select {
	case got := <-inbound:
		if string(got) != string(want) {
			t.Fatalf("transcoded bytes sent = %v, want %v", got, want)
		}
		if len(got) == len(pcmu) {
			t.Fatalf("sent bytes look untranscoded (same length as source PCMU): %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for transcoded audio")
	}
}

func TestRecvLoopTranscodesOutputWhenEnabled(t *testing.T) {
	tone := make([]int16, 240) // 10ms @ 24kHz
	for i := range tone {
		tone[i] = int16(10000 * (i % 50))
	}
	pcm16 := audio.SamplesToBytesLE(nil, tone)
	want := audio.NewOutputTranscoder(24000).Transcode(pcm16)

	upgrader := gorillaws.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(wireEvent{Type: "ready", SessionID: testSessionID})
		_ = conn.WriteMessage(gorillaws.BinaryMessage, pcm16)
		<-time.After(100 * time.Millisecond)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.Transcode.Output = config.TranscodeDirectionConfig{Enabled: true, SampleRate: 24000}

	stream, err := Dialer{Config: cfg}.Dial(context.Background(), "local-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if event.Type != backend.EventAudio {
			t.Fatalf("event type = %v, want EventAudio", event.Type)
		}
		if string(event.Audio) != string(want) {
			t.Fatalf("transcoded audio = %v, want %v", event.Audio, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for transcoded audio event")
	}
}

func TestRecvLoopBuffersOddTrailingByteAcrossWSMessages(t *testing.T) {
	tone := make([]int16, 100)
	for i := range tone {
		tone[i] = int16(5000 * (i % 20))
	}
	whole := audio.SamplesToBytesLE(nil, tone)
	want := audio.NewOutputTranscoder(24000).Transcode(whole)

	splitAt := 51 // odd byte offset, guarantees a sample straddles the two messages
	first, second := whole[:splitAt], whole[splitAt:]

	upgrader := gorillaws.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(sessionPath, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(sessionResponse{SessionID: testSessionID})
	})
	mux.HandleFunc(websocketPath+testSessionID, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(wireEvent{Type: "ready", SessionID: testSessionID})
		_ = conn.WriteMessage(gorillaws.BinaryMessage, first)
		_ = conn.WriteMessage(gorillaws.BinaryMessage, second)
		<-time.After(100 * time.Millisecond)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := testConfig(server.URL)
	cfg.Transcode.Output = config.TranscodeDirectionConfig{Enabled: true, SampleRate: 24000}

	stream, err := Dialer{Config: cfg}.Dial(context.Background(), "local-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for len(got) < len(want) {
		select {
		case event := <-stream.Events():
			if event.Type != backend.EventAudio {
				t.Fatalf("event type = %v, want EventAudio", event.Type)
			}
			got = append(got, event.Audio...)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for audio; got %d of %d expected bytes so far", len(got), len(want))
		}
	}
	if string(got) != string(want) {
		t.Fatalf("reassembled audio across split messages = %v, want %v", got, want)
	}
}

func testConfig(baseURL string) config.WebSocketConfig {
	return config.WebSocketConfig{
		BaseURL:         baseURL,
		SessionTimeout:  time.Second,
		ConnectTimeout:  time.Second,
		MaxMessageBytes: 1 << 20,
	}
}
