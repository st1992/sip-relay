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
