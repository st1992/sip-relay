package calllog

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sip-relay/internal/config"
)

func TestRecordingObjectNameIsFlatInTheBucketRoot(t *testing.T) {
	got := RecordingObjectName("call/123@example.com")
	want := "call-123-example.com.ulaw"
	if got != want {
		t.Fatalf("RecordingObjectName() = %q, want %q", got, want)
	}
}

func TestRecordingObjectNameFallsBackForEmptyCallID(t *testing.T) {
	got := RecordingObjectName("")
	want := "unknown.ulaw"
	if got != want {
		t.Fatalf("RecordingObjectName() = %q, want %q", got, want)
	}
}

func TestNewClientsSkipsUnconfiguredServices(t *testing.T) {
	clients, err := NewClients(context.Background(), &config.Config{})
	if err != nil {
		t.Fatalf("NewClients() error = %v, want nil (no network call for an unconfigured call_log)", err)
	}
	if clients.storage != nil {
		t.Fatal("storage client should not be created when recording_bucket is unset")
	}
	if clients.publisher != nil {
		t.Fatal("publisher should not be created when pubsub_topic_id is unset")
	}

	// Both Upload and Publish must be safe no-ops when their backing client
	// was never created, since neither service is configured.
	if err := clients.Publish(context.Background(), Entry{}); err != nil {
		t.Fatalf("Publish() with no publisher = %v, want nil", err)
	}
	recorder := &Recorder{}
	if uri, err := recorder.Upload(context.Background(), clients, "call-1"); err != nil || uri != "" {
		t.Fatalf("Upload() with no storage client = (%q, %v), want (\"\", nil)", uri, err)
	}

	clients.Close()
}

func TestClientsPublishAndCloseToleratesNilReceiver(t *testing.T) {
	var clients *Clients
	if err := clients.Publish(context.Background(), Entry{}); err != nil {
		t.Fatalf("Publish() on nil *Clients = %v, want nil", err)
	}
	clients.Close()
}

func TestRecorderUploadToleratesNilRecorder(t *testing.T) {
	var recorder *Recorder
	if uri, err := recorder.Upload(context.Background(), &Clients{}, "call-1"); err != nil || uri != "" {
		t.Fatalf("Upload() on nil *Recorder = (%q, %v), want (\"\", nil)", uri, err)
	}
}

func TestEntryJSONConversationHistoryFieldNamesAndTypes(t *testing.T) {
	start := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	data, err := json.Marshal(Entry{
		ConversationHistory: []ConversationEvent{
			{Type: "message", Role: "bot", Text: "hi there", StartTime: start},
			{Type: "message", Role: "user", Text: "hello", StartTime: start.Add(time.Second)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	history, ok := got["conversation_history"].([]any)
	if !ok || len(history) != 2 {
		t.Fatalf("conversation_history = %#v", got["conversation_history"])
	}
	first, ok := history[0].(map[string]any)
	if !ok {
		t.Fatalf("history[0] = %#v", history[0])
	}
	want := map[string]any{
		"type":       "message",
		"role":       "bot",
		"text":       "hi there",
		"start_time": "2026-07-07T12:00:00Z",
	}
	if len(first) != len(want) {
		t.Fatalf("history[0] fields = %v, want exactly %v", first, want)
	}
	for key, value := range want {
		if first[key] != value {
			t.Errorf("history[0][%q] = %v, want %v", key, first[key], value)
		}
	}
}

func TestEntryJSONOmitsConversationHistoryWhenEmpty(t *testing.T) {
	data, err := json.Marshal(Entry{Backend: "ces"})
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["conversation_history"]; ok {
		t.Fatalf("conversation_history present when ConversationHistory was nil: %v", got["conversation_history"])
	}
}

func TestEntryJSONIncludesOnlyCallEventFields(t *testing.T) {
	start := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	data, err := json.Marshal(Entry{
		Backend:        "ces",
		Provider:       map[string]string{"app_id": "app"},
		ConversationID: "session-1",
		ANI:            "caller",
		DNIS:           "1014",
		StartTime:      start,
		EndTime:        end,
		HangupReason:   "USER_ENDED",
	})
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"backend":         "ces",
		"conversation_id": "session-1",
		"ani":             "caller",
		"dnis":            "1014",
		"start_time":      "2026-07-07T12:00:00Z",
		"end_time":        "2026-07-07T12:01:00Z",
		"hangup_reason":   "USER_ENDED",
	}
	provider, ok := got["provider"].(map[string]any)
	if !ok || provider["app_id"] != "app" {
		t.Fatalf("provider = %#v", got["provider"])
	}
	delete(got, "provider")
	if len(got) != len(want) {
		t.Fatalf("entry fields = %v, want exactly %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s = %#v, want %#v", key, got[key], value)
		}
	}
}
