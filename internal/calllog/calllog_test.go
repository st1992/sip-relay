package calllog

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sip-relay/internal/config"
)

func TestRecordingObjectNameStoresUnderBackend(t *testing.T) {
	got := RecordingObjectName("websocket", "call/123@example.com")
	want := "websocket/call-123-example.com.ulaw"
	if got != want {
		t.Fatalf("RecordingObjectName() = %q, want %q", got, want)
	}
}

func TestRecordingObjectNameFallsBackForEmptyParts(t *testing.T) {
	got := RecordingObjectName("", "")
	want := "unknown/unknown.ulaw"
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
	if uri, err := recorder.Upload(context.Background(), clients, "ces", "call-1"); err != nil || uri != "" {
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
	if uri, err := recorder.Upload(context.Background(), &Clients{}, "ces", "call-1"); err != nil || uri != "" {
		t.Fatalf("Upload() on nil *Recorder = (%q, %v), want (\"\", nil)", uri, err)
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
