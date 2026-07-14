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
)

type Config struct {
	SIP        SIPConfig                  `yaml:"sip"`
	RTP        RTPConfig                  `yaml:"rtp"`
	CES        CESConfig                  `yaml:"ces"`
	WebSocket  WebSocketConfig            `yaml:"websocket"`
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
	BaseURL         string        `yaml:"base_url"`
	SessionTimeout  time.Duration `yaml:"session_timeout"`
	ConnectTimeout  time.Duration `yaml:"connect_timeout"`
	MaxMessageBytes int64         `yaml:"max_message_bytes"`
}

type ExtensionConfig struct {
	Backend string `yaml:"backend"`
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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
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
		CES: CESConfig{
			Location:      "us",
			Endpoint:      "ces.googleapis.com:443",
			SessionPrefix: "sip",
		},
		WebSocket: WebSocketConfig{
			SessionTimeout:  15 * time.Second,
			ConnectTimeout:  15 * time.Second,
			MaxMessageBytes: 4 << 20,
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
	usesCES, usesWebSocket := false, false
	for extension, route := range c.Extensions {
		if extension == "" {
			return fmt.Errorf("extensions key must not be empty")
		}
		switch route.Backend {
		case BackendCES:
			usesCES = true
		case BackendWebSocket:
			usesWebSocket = true
		default:
			return fmt.Errorf("extensions.%s.backend must be %q or %q", extension, BackendCES, BackendWebSocket)
		}
	}
	if usesCES {
		if err := c.CES.Validate(); err != nil {
			return err
		}
	}
	if usesWebSocket {
		if err := c.WebSocket.Validate(); err != nil {
			return err
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
