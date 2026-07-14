package call

import (
	"context"
	"testing"
	"time"

	"sip-relay/internal/backend"
)

func TestCloseBeforeStartClosesDone(t *testing.T) {
	c := New("call-1", Metadata{}, nil, nil, nil, nil)

	c.Close()

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed call")
	}
}

func TestStartAfterCloseDoesNotReopenCall(t *testing.T) {
	c := New("call-1", Metadata{}, nil, nil, nil, nil)
	c.Close()

	c.Start(context.Background())

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed call")
	}
}

func TestBackendEndReasonsHaveDistinctCallLogReasons(t *testing.T) {
	tests := []struct {
		reason EndReason
		want   string
	}{
		{EndReasonAgent, "AGENT_ENDED"},
		{EndReasonTransfer, "TRANSFERRED"},
		{EndReasonBackend, "BACKEND_ERROR"},
		{EndReasonUser, "USER_ENDED"},
	}
	for _, test := range tests {
		c := New("call-1", Metadata{}, nil, nil, nil, nil)
		c.setEndReason(test.reason)
		if got := c.hangupReason(); got != test.want {
			t.Errorf("hangupReason(%s) = %q, want %q", test.reason, got, test.want)
		}
	}
}

func TestReceiveBackendTreatsClosedEventsAsFailure(t *testing.T) {
	events := make(chan backend.Event)
	close(events)
	stream := &fakeBackendStream{events: events}
	c := New("call-1", Metadata{}, nil, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.receiveBackend(ctx, stream, nil, &mediaStats{})
	if err == nil || err.Error() != "backend events channel closed unexpectedly" {
		t.Fatalf("receiveBackend() error = %v", err)
	}
}

type fakeBackendStream struct {
	events <-chan backend.Event
}

func (s *fakeBackendStream) Input() chan<- []byte         { return nil }
func (s *fakeBackendStream) Events() <-chan backend.Event { return s.events }
func (s *fakeBackendStream) Done() <-chan error           { return nil }
func (s *fakeBackendStream) Close() error                 { return nil }
