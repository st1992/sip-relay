package calllog

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRecordingObjectNameStoresUnderAppID(t *testing.T) {
	got := RecordingObjectName("voice-app", "call/123@example.com")
	want := "voice-app/call-123-example.com.ulaw"
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

func TestEntryJSONIncludesOnlyCallEventFields(t *testing.T) {
	start := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	data, err := json.Marshal(Entry{
		ProjectID:      "project",
		Location:       "us",
		AppID:          "app",
		DeploymentID:   "deployment",
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
		"project_id":      "project",
		"location":        "us",
		"app_id":          "app",
		"deployment_id":   "deployment",
		"conversation_id": "session-1",
		"ani":             "caller",
		"dnis":            "1014",
		"start_time":      "2026-07-07T12:00:00Z",
		"end_time":        "2026-07-07T12:01:00Z",
		"hangup_reason":   "USER_ENDED",
	}
	if len(got) != len(want) {
		t.Fatalf("entry fields = %v, want exactly %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s = %#v, want %#v", key, got[key], value)
		}
	}
}
