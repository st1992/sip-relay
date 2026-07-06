package calllog

import "testing"

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
