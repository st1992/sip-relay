package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	cfg.WebSocket.BaseURL = "http://localhost:8001"

	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRequiresCESConfigWhenSelected(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendCES}}

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

func TestWebSocketConfigRejectsBaseURLPath(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendWebSocket}}
	cfg.WebSocket.BaseURL = "http://localhost:8001/prefix"

	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must not contain a path") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestDefaultHasTranscodeDisabledWithSaneRates(t *testing.T) {
	cfg := Default()
	if cfg.WebSocket.Transcode.Input.Enabled || cfg.WebSocket.Transcode.Output.Enabled {
		t.Fatal("transcoding must default to disabled in both directions")
	}
	if cfg.WebSocket.Transcode.Input.SampleRate != 16000 {
		t.Fatalf("default input sample_rate = %d, want 16000", cfg.WebSocket.Transcode.Input.SampleRate)
	}
	if cfg.WebSocket.Transcode.Output.SampleRate != 24000 {
		t.Fatalf("default output sample_rate = %d, want 24000", cfg.WebSocket.Transcode.Output.SampleRate)
	}
}

func TestValidateAllowsDisabledTranscodeWithZeroSampleRate(t *testing.T) {
	cfg := Default()
	cfg.Extensions = map[string]ExtensionConfig{"1111": {Backend: BackendWebSocket}}
	cfg.WebSocket.BaseURL = "http://localhost:8001"
	cfg.WebSocket.Transcode = TranscodeConfig{}

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
			cfg.WebSocket.BaseURL = "http://localhost:8001"
			test.mutate(&cfg.WebSocket.Transcode)

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
	if !cfg.WebSocket.Transcode.Input.Enabled {
		t.Fatal("input.enabled did not load as true")
	}
	if cfg.WebSocket.Transcode.Input.SampleRate != 16000 {
		t.Fatalf("input.sample_rate = %d, want inherited default 16000", cfg.WebSocket.Transcode.Input.SampleRate)
	}
	if cfg.WebSocket.Transcode.Output.Enabled {
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
  transport: websocket
extensions:
  "1111":
    backend: websocket
websocket:
  base_url: http://localhost:8001
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "field transport not found") {
		t.Fatalf("Load() error = %v", err)
	}
}
