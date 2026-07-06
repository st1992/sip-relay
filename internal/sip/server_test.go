package sip

import (
	"testing"

	sipmsg "github.com/livekit/sipgo/sip"
)

func TestHeadersCapturesAllSIPHeaders(t *testing.T) {
	req := sipmsg.NewRequest(sipmsg.INVITE, sipmsg.Uri{User: "relay", Host: "example.com"})
	req.AppendHeader(sipmsg.NewHeader("Via", "SIP/2.0/UDP first.example.com"))
	req.AppendHeader(sipmsg.NewHeader("Via", "SIP/2.0/UDP second.example.com"))
	req.AppendHeader(sipmsg.NewHeader("X-Custom", "custom-value"))

	got := headers(req)
	if len(got["Via"]) != 2 {
		t.Fatalf("Via headers = %v, want 2 values", got["Via"])
	}
	if got["Via"][0] != "SIP/2.0/UDP first.example.com" {
		t.Fatalf("first Via = %q", got["Via"][0])
	}
	if got["Via"][1] != "SIP/2.0/UDP second.example.com" {
		t.Fatalf("second Via = %q", got["Via"][1])
	}
	if got["X-Custom"][0] != "custom-value" {
		t.Fatalf("X-Custom = %v", got["X-Custom"])
	}
}
