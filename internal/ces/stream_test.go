package ces

import (
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
