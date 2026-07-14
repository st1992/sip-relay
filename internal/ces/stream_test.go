package ces

import (
	"errors"
	"testing"

	cespb "cloud.google.com/go/ces/apiv1/cespb"

	"sip-relay/internal/config"
)

func TestConfigMessageUsesMULAW8000(t *testing.T) {
	conf := config.CESConfig{
		ProjectID:    "project",
		Location:     "us",
		AppID:        "app",
		DeploymentID: "deployment",
	}

	msg := ConfigMessage(conf, "session")
	cfg := msg.GetConfig()
	if cfg == nil {
		t.Fatal("missing config")
	}
	if cfg.GetSession() != "projects/project/locations/us/apps/app/sessions/session" {
		t.Fatalf("session = %q", cfg.GetSession())
	}
	if cfg.GetDeployment() != "projects/project/locations/us/apps/app/deployments/deployment" {
		t.Fatalf("deployment = %q", cfg.GetDeployment())
	}
	if got := cfg.GetInputAudioConfig().GetAudioEncoding(); got != cespb.AudioEncoding_MULAW {
		t.Fatalf("input encoding = %v", got)
	}
	if got := cfg.GetInputAudioConfig().GetSampleRateHertz(); got != 8000 {
		t.Fatalf("input sample rate = %d", got)
	}
	if got := cfg.GetOutputAudioConfig().GetAudioEncoding(); got != cespb.AudioEncoding_MULAW {
		t.Fatalf("output encoding = %v", got)
	}
	if got := cfg.GetOutputAudioConfig().GetSampleRateHertz(); got != 8000 {
		t.Fatalf("output sample rate = %d", got)
	}
}

func TestAudioMessageCopiesPayload(t *testing.T) {
	payload := []byte{1, 2, 3}
	msg := AudioMessage(payload)
	payload[0] = 9

	got := msg.GetRealtimeInput().GetAudio()
	if string(got) != string([]byte{1, 2, 3}) {
		t.Fatalf("audio payload = %v", got)
	}
}

func TestTextMessageUsesRealtimeTextInput(t *testing.T) {
	msg := TextMessage("hello")

	got := msg.GetRealtimeInput().GetText()
	if got != "hello" {
		t.Fatalf("text input = %q, want hello", got)
	}
}

func TestBaseStreamClosedSignalDoesNotConsumeDoneError(t *testing.T) {
	stream := newBaseStream(func() {})
	want := errors.New("stream failed")

	stream.finish(want)

	select {
	case <-stream.closed:
	default:
		t.Fatal("stream closed signal was not closed")
	}

	got, ok := <-stream.Done()
	if !ok {
		t.Fatal("stream done channel closed before result was read")
	}
	if !errors.Is(got, want) {
		t.Fatalf("stream done error = %v, want %v", got, want)
	}
}
