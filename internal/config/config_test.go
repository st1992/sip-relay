package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// validWebSocketConfig returns a WebSocketConfig with the fields Validate()
// requires already set, as if applyWebSocketDefaults had run (i.e. as it
// would look after Load()). Tests that exercise Validate() directly (without
// going through Load()) build on top of this instead of relying on
// defaulting, since defaulting is now a Load()-only concern.
func validWebSocketConfig(baseURL string) WebSocketConfig {
	return WebSocketConfig{
		BaseURL:         baseURL,
		SessionTimeout:  15 * time.Second,
		ConnectTimeout:  15 * time.Second,
		MaxMessageBytes: 4 << 20,
	}
}

func TestRouteSelectsConfiguredBackend(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{
		"1111": {Backend: BackendCES},
		"2222": {Backend: BackendWebSocket},
	}

	got, ok := cfg.Route("2222")
	if !ok || got.Backend != BackendWebSocket {
		t.Fatalf("Route(2222) = %#v, %v", got, ok)
	}
	if _, ok := cfg.Route("3333"); ok {
		t.Fatal("unknown extension was accepted")
	}
}

func TestValidateOnlyRequiresSelectedBackendConfig(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendWebSocket}}
	cfg.WebSocket = map[string]WebSocketConfig{"default": validWebSocketConfig("http://localhost:8001")}

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRequiresCESConfigWhenSelected(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendCES}}
	cfg.CES = map[string]CESConfig{"default": {}}

	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "ces.project_id") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsUnknownBackend(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: "other"}}

	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "extensions.1111.backend") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsMissingProfile(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendWebSocket, Profile: "missing"}}
	cfg.WebSocket = map[string]WebSocketConfig{"default": {BaseURL: "http://localhost:8001"}}

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), `websocket profile "missing" not defined`) {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestWebSocketConfigRejectsBaseURLPath(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendWebSocket}}
	cfg.WebSocket = map[string]WebSocketConfig{"default": {BaseURL: "http://localhost:8001/prefix"}}

	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must not contain a path") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestApplyWebSocketDefaultsSetsTranscodeDisabledWithSaneRates(t *testing.T) {
	var ws WebSocketConfig
	applyWebSocketDefaults(&ws)
	if ws.Transcode.Input.Enabled || ws.Transcode.Output.Enabled {
		t.Fatal("transcoding must default to disabled in both directions")
	}
	if ws.Transcode.Input.SampleRate != 16000 {
		t.Fatalf("default input sample_rate = %d, want 16000", ws.Transcode.Input.SampleRate)
	}
	if ws.Transcode.Output.SampleRate != 24000 {
		t.Fatalf("default output sample_rate = %d, want 24000", ws.Transcode.Output.SampleRate)
	}
}

func TestValidateAllowsDisabledTranscodeWithZeroSampleRate(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendWebSocket}}
	ws := validWebSocketConfig("http://localhost:8001")
	ws.Transcode = TranscodeConfig{}
	cfg.WebSocket = map[string]WebSocketConfig{"default": ws}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with disabled transcode and zero sample rates = %v, want nil", err)
	}
}

func TestValidateRejectsEnabledTranscodeWithNonPositiveSampleRate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*TranscodeConfig)
		wantErr string
	}{
		{"input zero", func(c *TranscodeConfig) { c.Input = TranscodeDirectionConfig{Enabled: true, SampleRate: 0} }, "websocket.transcode.input.sample_rate"},
		{"input negative", func(c *TranscodeConfig) { c.Input = TranscodeDirectionConfig{Enabled: true, SampleRate: -1} }, "websocket.transcode.input.sample_rate"},
		{"output zero", func(c *TranscodeConfig) { c.Output = TranscodeDirectionConfig{Enabled: true, SampleRate: 0} }, "websocket.transcode.output.sample_rate"},
		{"output negative", func(c *TranscodeConfig) { c.Output = TranscodeDirectionConfig{Enabled: true, SampleRate: -1} }, "websocket.transcode.output.sample_rate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Default()
			cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendWebSocket}}
			ws := validWebSocketConfig("http://localhost:8001")
			test.mutate(&ws.Transcode)
			cfg.WebSocket = map[string]WebSocketConfig{"default": ws}

			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestLoadTranscodePartialOverrideInheritsDefaultSampleRate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`
sip:
  advertised_ip: 127.0.0.1
rtp:
  listen_ip: 0.0.0.0
extensions:
  "1111":
    backend: websocket
websocket:
  default:
    base_url: http://localhost:8001
    transcode:
      input:
        enabled: true
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	ws := cfg.WebSocket["default"]
	if !ws.Transcode.Input.Enabled {
		t.Fatal("input.enabled did not load as true")
	}
	if ws.Transcode.Input.SampleRate != 16000 {
		t.Fatalf("input.sample_rate = %d, want inherited default 16000", ws.Transcode.Input.SampleRate)
	}
	if ws.Transcode.Output.Enabled {
		t.Fatal("output.enabled should remain false when left unset")
	}
}

func TestLoadRejectsRemovedCESTransport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`
sip:
  advertised_ip: 127.0.0.1
rtp:
  listen_ip: 0.0.0.0
ces:
  default:
    transport: websocket
extensions:
  "1111":
    backend: websocket
websocket:
  default:
    base_url: http://localhost:8001
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "field transport not found") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadResolvesDistinctProfilesIndependently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`
sip:
  advertised_ip: 127.0.0.1
rtp:
  listen_ip: 0.0.0.0
extensions:
  "1111":
    backend: websocket
  "2222":
    backend: websocket
    profile: secondary
websocket:
  default:
    base_url: http://localhost:8001
  secondary:
    base_url: http://localhost:8002
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.WebSocket["default"].BaseURL; got != "http://localhost:8001" {
		t.Fatalf("default profile base_url = %q, want http://localhost:8001", got)
	}
	if got := cfg.WebSocket["secondary"].BaseURL; got != "http://localhost:8002" {
		t.Fatalf("secondary profile base_url = %q, want http://localhost:8002", got)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	if _, err := Load("../../config.example.yaml"); err != nil {
		t.Fatalf("Load(config.example.yaml) = %v, want nil", err)
	}
}
