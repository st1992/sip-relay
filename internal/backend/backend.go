package backend

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

var (
	ErrAgentEnded = errors.New("backend ended session")
	ErrTransfer   = errors.New("backend requested transfer")
	ErrGoAway     = errors.New("backend sent go-away")
)

type EventType int

const (
	EventAudio EventType = iota
	EventBotTranscript
	EventUserTranscript
	EventBargeIn
	EventTurnComplete
	EventEndSession
	EventTransfer
	EventGoAway
)

type Event struct {
	Type  EventType
	Audio []byte
	Text  string
}

type Stream interface {
	Input() chan<- []byte
	Events() <-chan Event
	Done() <-chan error
	Close() error
}

type Dialer interface {
	Dial(ctx context.Context, sessionID string, log *slog.Logger) (Stream, error)
	Name() string
	Metadata() map[string]string
	ConnectTimeout() time.Duration
}
