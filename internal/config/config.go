package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	BackendCES       = "ces"
	BackendWebSocket = "websocket"

	// DefaultProfile is the implicit ces/websocket profile name used by an
	// extension that omits `profile:`.
	DefaultProfile = "default"
)

type Config struct {
	SIP        SIPConfig                  `yaml:"sip"`
	RTP        RTPConfig                  `yaml:"rtp"`
	CES        map[string]CESConfig       `yaml:"ces"`
	WebSocket  map[string]WebSocketConfig `yaml:"websocket"`
	Extensions map[string]ExtensionConfig `yaml:"extensions"`
	CallLog    CallLogConfig              `yaml:"call_log"`
}

type SIPConfig struct {
	ListenIP     string `yaml:"listen_ip"`
	ListenPort   int    `yaml:"listen_port"`
	AdvertisedIP string `yaml:"advertised_ip"`
	UserAgent    string `yaml:"user_agent"`
}

type RTPConfig struct {
	ListenIP            string        `yaml:"listen_ip"`
	PortMin             int           `yaml:"port_min"`
	PortMax             int           `yaml:"port_max"`
	SymmetricRTP        bool          `yaml:"symmetric_rtp"`
	MediaTimeoutInitial time.Duration `yaml:"media_timeout_initial"`
	MediaTimeout        time.Duration `yaml:"media_timeout"`
}

type CESConfig struct {
	ProjectID             string `yaml:"project_id"`
	Location              string `yaml:"location"`
	AppID                 string `yaml:"app_id"`
	DeploymentID          string `yaml:"deployment_id"`
	Endpoint              string `yaml:"endpoint"`
	CredentialsFile       string `yaml:"credentials_file"`
	SessionPrefix         string `yaml:"session_prefix"`
	NoiseSuppressionLevel string `yaml:"noise_suppression_level"`
	TimeZone              string `yaml:"time_zone"`
	UseToolFakes          bool   `yaml:"use_tool_fakes"`
}

type WebSocketConfig struct {
	BaseURL         string          `yaml:"base_url"`
	SessionTimeout  time.Duration   `yaml:"session_timeout"`
	ConnectTimeout  time.Duration   `yaml:"connect_timeout"`
	MaxMessageBytes int64           `yaml:"max_message_bytes"`
	Transcode       TranscodeConfig `yaml:"transcode"`
}

// TranscodeConfig controls optional PCM transcoding for the WebSocket
// backend only. Both directions default disabled: audio flows as raw PCMU
// 8kHz bytes unchanged unless a direction is explicitly enabled.
type TranscodeConfig struct {
	Input  TranscodeDirectionConfig `yaml:"input"`  // caller -> backend
	Output TranscodeDirectionConfig `yaml:"output"` // backend -> caller
}

type TranscodeDirectionConfig struct {
	Enabled    bool `yaml:"enabled"`
	SampleRate int  `yaml:"sample_rate"`
}

type ExtensionConfig struct {
	Backend string `yaml:"backend"`
	// Profile selects which named ces/websocket config this extension uses.
	// Empty means DefaultProfile.
	Profile string `yaml:"profile"`
}

// ResolveProfile returns the effective ces/websocket profile name for a
// route, applying the DefaultProfile fallback when Profile is unset.
func ResolveProfile(route ExtensionConfig) string {
	if route.Profile == "" {
		return DefaultProfile
	}
	return route.Profile
}

type CallLogConfig struct {
	PubSubProjectID string `yaml:"pubsub_project_id"`
	PubSubTopicID   string `yaml:"pubsub_topic_id"`
	RecordingBucket string `yaml:"recording_bucket"`
	CredentialsFile string `yaml:"credentials_file"`
}

func Load(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("config path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := Default()
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, err
	}
	for name, ces := range cfg.CES {
		applyCESDefaults(&ces)
		cfg.CES[name] = ces
	}
	for name, ws := range cfg.WebSocket {
		applyWebSocketDefaults(&ws)
		cfg.WebSocket[name] = ws
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func applyCESDefaults(c *CESConfig) {
	if c.Location == "" {
		c.Location = "us"
	}
	if c.Endpoint == "" {
		c.Endpoint = "ces.googleapis.com:443"
	}
	if c.SessionPrefix == "" {
		c.SessionPrefix = "sip"
	}
}

func applyWebSocketDefaults(c *WebSocketConfig) {
	if c.SessionTimeout == 0 {
		c.SessionTimeout = 15 * time.Second
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 15 * time.Second
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = 4 << 20
	}
	if c.Transcode.Input.SampleRate == 0 {
		c.Transcode.Input.SampleRate = 16000
	}
	if c.Transcode.Output.SampleRate == 0 {
		c.Transcode.Output.SampleRate = 24000
	}
}

func Default() *Config {
	return &Config{
		SIP: SIPConfig{
			ListenIP:     "0.0.0.0",
			ListenPort:   5060,
			AdvertisedIP: "127.0.0.1",
			UserAgent:    "sip-relay",
		},
		RTP: RTPConfig{
			ListenIP:            "0.0.0.0",
			PortMin:             10000,
			PortMax:             20000,
			SymmetricRTP:        true,
			MediaTimeoutInitial: 30 * time.Second,
			MediaTimeout:        15 * time.Second,
		},
	}
}

func (c *Config) Validate() error {
	if c.SIP.ListenPort <= 0 || c.SIP.ListenPort > 65535 {
		return fmt.Errorf("sip.listen_port must be in range 1-65535")
	}
	if _, err := netip.ParseAddr(c.SIP.AdvertisedIP); err != nil {
		return fmt.Errorf("sip.advertised_ip must be a valid IP address: %w", err)
	}
	if _, err := netip.ParseAddr(c.RTP.ListenIP); err != nil {
		return fmt.Errorf("rtp.listen_ip must be a valid IP address: %w", err)
	}
	if c.RTP.PortMin <= 0 || c.RTP.PortMax < c.RTP.PortMin || c.RTP.PortMax > 65535 {
		return fmt.Errorf("rtp port range is invalid")
	}
	if c.RTP.MediaTimeoutInitial <= 0 || c.RTP.MediaTimeout <= 0 {
		return fmt.Errorf("rtp media timeouts must be positive")
	}
	if len(c.Extensions) == 0 {
		return fmt.Errorf("extensions must configure at least one extension")
	}
	for extension, route := range c.Extensions {
		if extension == "" {
			return fmt.Errorf("extensions key must not be empty")
		}
		profile := ResolveProfile(route)
		switch route.Backend {
		case BackendCES:
			cesCfg, ok := c.CES[profile]
			if !ok {
				return fmt.Errorf("extensions.%s: ces profile %q not defined", extension, profile)
			}
			if err := cesCfg.Validate(); err != nil {
				return err
			}
		case BackendWebSocket:
			wsCfg, ok := c.WebSocket[profile]
			if !ok {
				return fmt.Errorf("extensions.%s: websocket profile %q not defined", extension, profile)
			}
			if err := wsCfg.Validate(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("extensions.%s.backend must be %q or %q", extension, BackendCES, BackendWebSocket)
		}
	}
	if c.CallLog.PubSubTopicID != "" && c.CallLog.PubSubProjectID == "" {
		return fmt.Errorf("call_log.pubsub_project_id is required when pubsub_topic_id is set")
	}
	return nil
}

func (c CESConfig) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("ces.project_id is required")
	}
	if c.Location == "" {
		return fmt.Errorf("ces.location is required")
	}
	if c.AppID == "" {
		return fmt.Errorf("ces.app_id is required")
	}
	return nil
}

func (c WebSocketConfig) Validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("websocket.base_url must be an absolute http or https URL")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("websocket.base_url must not contain a query or fragment")
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Errorf("websocket.base_url must not contain a path")
	}
	if c.SessionTimeout <= 0 || c.ConnectTimeout <= 0 {
		return fmt.Errorf("websocket timeouts must be positive")
	}
	if c.MaxMessageBytes <= 0 {
		return fmt.Errorf("websocket.max_message_bytes must be positive")
	}
	return c.Transcode.Validate()
}

func (c TranscodeConfig) Validate() error {
	if err := c.Input.validate("websocket.transcode.input"); err != nil {
		return err
	}
	return c.Output.validate("websocket.transcode.output")
}

func (c TranscodeDirectionConfig) validate(field string) error {
	if !c.Enabled {
		return nil
	}
	if c.SampleRate <= 0 {
		return fmt.Errorf("%s.sample_rate must be positive", field)
	}
	return nil
}

func (c *Config) Route(extension string) (ExtensionConfig, bool) {
	route, ok := c.Extensions[extension]
	return route, ok
}

func (c CESConfig) AppResource() string {
	return fmt.Sprintf("projects/%s/locations/%s/apps/%s", c.ProjectID, c.Location, c.AppID)
}

func (c CESConfig) DeploymentResource() string {
	if c.DeploymentID == "" {
		return ""
	}
	return fmt.Sprintf("%s/deployments/%s", c.AppResource(), c.DeploymentID)
}

func (c CESConfig) SessionResource(sessionID string) string {
	return fmt.Sprintf("%s/sessions/%s", c.AppResource(), sessionID)
}
