package calllog

import (
	"encoding/json"
	"strings"
	"testing"
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

func TestEntryJSONIncludesMetadata(t *testing.T) {
	data, err := json.Marshal(Entry{
		CallID: "call-1",
		Metadata: map[string][]string{
			"Via": []string{"first", "second"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(data) {
		t.Fatalf("entry marshaled invalid JSON: %s", data)
	}
	if got := string(data); !strings.Contains(got, `"metadata":{"Via":["first","second"]}`) {
		t.Fatalf("entry JSON = %s", got)
	}
}
