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
	CallID    string              `json:"call_id"`
	ANI       string              `json:"ani"`
	DNIS      string              `json:"dnis"`
	StartedAt time.Time           `json:"started_at"`
	EndedAt   time.Time           `json:"ended_at"`
	Metadata  map[string][]string `json:"metadata,omitempty"`
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

func (r *Recorder) Upload(ctx context.Context, cfg *config.Config, callID string) (string, error) {
	if r == nil || cfg == nil || cfg.CallLog.RecordingBucket == "" {
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

	client, err := storage.NewClient(ctx, clientOptions(cfg.CES)...)
	if err != nil {
		return "", err
	}
	defer client.Close()

	objectName := RecordingObjectName(cfg.CES.AppID, callID)
	writer := client.Bucket(cfg.CallLog.RecordingBucket).Object(objectName).NewWriter(ctx)
	writer.ContentType = uploadContentType
	if _, err := io.Copy(writer, r.file); err != nil {
		_ = writer.Close()
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	return fmt.Sprintf("gs://%s/%s", cfg.CallLog.RecordingBucket, objectName), nil
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

func Publish(ctx context.Context, cfg *config.Config, entry Entry) error {
	if cfg == nil || cfg.CallLog.PubSubTopicID == "" {
		return nil
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	projectID := cfg.CallLog.PubSubProjectID
	if projectID == "" {
		projectID = cfg.CES.ProjectID
	}
	client, err := pubsub.NewClient(ctx, projectID, clientOptions(cfg.CES)...)
	if err != nil {
		return err
	}
	defer client.Close()

	publisher := client.Publisher(cfg.CallLog.PubSubTopicID)
	defer publisher.Stop()
	result := publisher.Publish(ctx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"call_id": entry.CallID,
		},
	})
	_, err = result.Get(ctx)
	return err
}

func RecordingObjectName(appID, callID string) string {
	appPart := cleanObjectPart(appID)
	callPart := cleanObjectPart(callID)
	return appPart + "/" + callPart + ".ulaw"
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

func clientOptions(conf config.CESConfig) []option.ClientOption {
	if conf.CredentialsFile == "" {
		return nil
	}
	return []option.ClientOption{option.WithCredentialsFile(conf.CredentialsFile)}
}
