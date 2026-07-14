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

func testConfig(baseURL string) config.WebSocketConfig {
	return config.WebSocketConfig{
		BaseURL:         baseURL,
		SessionTimeout:  time.Second,
		ConnectTimeout:  time.Second,
		MaxMessageBytes: 1 << 20,
	}
}
