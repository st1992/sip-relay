package calllog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"sip-relay/internal/config"
)

const uploadContentType = "audio/basic"

var objectPartCleaner = regexp.MustCompile(`[^a-zA-Z0-9._=-]+`)

type Entry struct {
	Backend             string              `json:"backend"`
	Profile             string              `json:"profile,omitempty"`
	Provider            map[string]string   `json:"provider,omitempty"`
	ConversationID      string              `json:"conversation_id"`
	ANI                 string              `json:"ani"`
	DNIS                string              `json:"dnis"`
	StartTime           time.Time           `json:"start_time"`
	EndTime             time.Time           `json:"end_time"`
	HangupReason        string              `json:"hangup_reason"`
	ConversationHistory []ConversationEvent `json:"conversation_history,omitempty"`
}

// ConversationEvent is one entry in a call's conversation_history: a single
// spoken message attributed to either the bot or the caller.
type ConversationEvent struct {
	Type      string    `json:"type"`
	Role      string    `json:"role"`
	Text      string    `json:"text"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
}

type Recorder struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewRecorder(enabled bool) (*Recorder, error) {
	if !enabled {
		return nil, nil
	}
	file, err := os.CreateTemp("", "sip-relay-*.ulaw")
	if err != nil {
		return nil, err
	}
	return &Recorder{file: file, path: file.Name()}, nil
}

func (r *Recorder) Write(payload []byte) error {
	if r == nil || len(payload) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.file.Write(payload)
	return err
}

func (r *Recorder) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeLocked()
}

func (r *Recorder) Upload(ctx context.Context, clients *Clients, callID string) (string, error) {
	if r == nil || clients == nil || clients.storage == nil {
		return "", nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.file.Sync(); err != nil {
		return "", err
	}
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	objectName := RecordingObjectName(callID)
	writer := clients.storage.Bucket(clients.bucket).Object(objectName).NewWriter(ctx)
	writer.ContentType = uploadContentType
	if _, err := io.Copy(writer, r.file); err != nil {
		_ = writer.Close()
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	return fmt.Sprintf("gs://%s/%s", clients.bucket, objectName), nil
}

func (r *Recorder) Remove() error {
	if r == nil {
		return nil
	}
	if err := r.Close(); err != nil {
		_ = os.Remove(r.path)
		return err
	}
	return os.Remove(r.path)
}

// Clients holds the GCS and Pub/Sub clients shared by every call for the
// life of the process. Building these involves a real dial+auth handshake,
// so they're created once at startup (NewClients) instead of per call.
type Clients struct {
	bucket  string
	storage *storage.Client

	pubsubClient *pubsub.Client
	publisher    *pubsub.Publisher
}

// NewClients builds the shared upload/publish clients described by cfg.
// A client is only created for the services actually configured (a nil
// RecordingBucket or PubSubTopicID leaves the corresponding field nil, and
// the matching Upload/Publish call becomes a no-op).
func NewClients(ctx context.Context, cfg *config.Config) (*Clients, error) {
	c := &Clients{bucket: cfg.CallLog.RecordingBucket}

	if cfg.CallLog.RecordingBucket != "" {
		client, err := storage.NewClient(ctx, clientOptions(cfg.CallLog.CredentialsFile)...)
		if err != nil {
			return nil, fmt.Errorf("create storage client: %w", err)
		}
		c.storage = client
	}

	if cfg.CallLog.PubSubTopicID != "" {
		client, err := pubsub.NewClient(ctx, cfg.CallLog.PubSubProjectID, clientOptions(cfg.CallLog.CredentialsFile)...)
		if err != nil {
			if c.storage != nil {
				_ = c.storage.Close()
			}
			return nil, fmt.Errorf("create pubsub client: %w", err)
		}
		c.pubsubClient = client
		c.publisher = client.Publisher(cfg.CallLog.PubSubTopicID)
	}

	return c, nil
}

// Close releases the shared clients. Safe to call on a nil *Clients.
func (c *Clients) Close() {
	if c == nil {
		return
	}
	if c.publisher != nil {
		c.publisher.Stop()
	}
	if c.pubsubClient != nil {
		_ = c.pubsubClient.Close()
	}
	if c.storage != nil {
		_ = c.storage.Close()
	}
}

func (c *Clients) Publish(ctx context.Context, entry Entry) error {
	if c == nil || c.publisher == nil {
		return nil
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	result := c.publisher.Publish(ctx, &pubsub.Message{Data: data})
	_, err = result.Get(ctx)
	return err
}

// RecordingObjectName returns the recording's object name, placed directly
// in the bucket root (no per-backend subfolder) so it's <call_id>.ulaw for
// every backend.
func RecordingObjectName(callID string) string {
	return cleanObjectPart(callID) + ".ulaw"
}

func (r *Recorder) closeLocked() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}

func cleanObjectPart(value string) string {
	value = objectPartCleaner.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return "unknown"
	}
	return value
}

func clientOptions(credentialsFile string) []option.ClientOption {
	if credentialsFile == "" {
		return nil
	}
	return []option.ClientOption{option.WithCredentialsFile(credentialsFile)}
}
