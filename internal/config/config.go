package config

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	TransportGRPC      = "grpc"
	TransportWebSocket = "websocket"
)

type Config struct {
	SIP     SIPConfig     `yaml:"sip"`
	RTP     RTPConfig     `yaml:"rtp"`
	CES     CESConfig     `yaml:"ces"`
	CallLog CallLogConfig `yaml:"call_log"`
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
	ProjectID             string                        `yaml:"project_id"`
	Location              string                        `yaml:"location"`
	AppID                 string                        `yaml:"app_id"`
	DeploymentID          string                        `yaml:"deployment_id"`
	Endpoint              string                        `yaml:"endpoint"`
	Transport             string                        `yaml:"transport"`
	CredentialsFile       string                        `yaml:"credentials_file"`
	SessionPrefix         string                        `yaml:"session_prefix"`
	NoiseSuppressionLevel string                        `yaml:"noise_suppression_level"`
	TimeZone              string                        `yaml:"time_zone"`
	UseToolFakes          bool                          `yaml:"use_tool_fakes"`
	RestartOnGoAway       bool                          `yaml:"restart_on_goaway"`
	Extensions            map[string]CESExtensionConfig `yaml:"extensions"`
}

type CESExtensionConfig struct {
	ProjectID    string `yaml:"project_id"`
	Location     string `yaml:"location"`
	AppID        string `yaml:"app_id"`
	DeploymentID string `yaml:"deployment_id"`
}

type CallLogConfig struct {
	PubSubProjectID string `yaml:"pubsub_project_id"`
	PubSubTopicID   string `yaml:"pubsub_topic_id"`
	RecordingBucket string `yaml:"recording_bucket"`
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
	if err := yaml.Unmarshal(data, cfg); err != nil {
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
			Transport:     TransportGRPC,
			SessionPrefix: "sip",
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
	if err := c.CES.Validate(); err != nil {
		return err
	}
	switch c.CES.Transport {
	case TransportGRPC, TransportWebSocket:
	default:
		return fmt.Errorf("ces.transport must be %q or %q", TransportGRPC, TransportWebSocket)
	}
	if c.CallLog.PubSubTopicID != "" && c.CallLog.PubSubProjectID == "" && c.CES.ProjectID == "" {
		return fmt.Errorf("call_log.pubsub_project_id is required when ces.project_id is empty")
	}
	return nil
}

func (c CESConfig) Validate() error {
	if len(c.Extensions) == 0 {
		return validateCESConfig(c, "ces")
	}
	for extension := range c.Extensions {
		if extension == "" {
			return fmt.Errorf("ces.extensions key must not be empty")
		}
		effective, ok := c.ForExtension(extension)
		if !ok {
			return fmt.Errorf("ces.extensions.%s is invalid", extension)
		}
		if err := validateCESConfig(effective, "ces.extensions."+extension); err != nil {
			return err
		}
	}
	return nil
}

func validateCESConfig(c CESConfig, prefix string) error {
	if c.ProjectID == "" {
		return fmt.Errorf("%s.project_id is required", prefix)
	}
	if c.Location == "" {
		return fmt.Errorf("%s.location is required", prefix)
	}
	if c.AppID == "" {
		return fmt.Errorf("%s.app_id is required", prefix)
	}
	return nil
}

func (c CESConfig) ForExtension(extension string) (CESConfig, bool) {
	if len(c.Extensions) == 0 {
		c.Extensions = nil
		return c, true
	}
	ext, ok := c.Extensions[extension]
	if !ok {
		return CESConfig{}, false
	}
	c.Extensions = nil
	if ext.ProjectID != "" {
		c.ProjectID = ext.ProjectID
	}
	if ext.Location != "" {
		c.Location = ext.Location
	}
	if ext.AppID != "" {
		c.AppID = ext.AppID
	}
	if ext.DeploymentID != "" {
		c.DeploymentID = ext.DeploymentID
	}
	return c, true
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
