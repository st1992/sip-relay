package sdp

import (
	"net/netip"
	"strings"
	"testing"
)

func TestAnswerOfferAcceptsPCMUOnly(t *testing.T) {
	offer := []byte("v=0\r\n" +
		"o=- 1 1 IN IP4 192.0.2.10\r\n" +
		"s=-\r\n" +
		"c=IN IP4 192.0.2.10\r\n" +
		"t=0 0\r\n" +
		"m=audio 4000 RTP/AVP 0 8\r\n" +
		"a=rtpmap:0 PCMU/8000\r\n" +
		"a=rtpmap:8 PCMA/8000\r\n")

	answer, err := AnswerOffer(offer, netip.MustParseAddr("198.51.100.20"), 12000)
	if err != nil {
		t.Fatal(err)
	}
	if answer.Offer.RemoteAddr.String() != "192.0.2.10" {
		t.Fatalf("remote addr = %s", answer.Offer.RemoteAddr)
	}
	if answer.Offer.RemotePort != 4000 {
		t.Fatalf("remote port = %d", answer.Offer.RemotePort)
	}
	if answer.Offer.PayloadType != 0 {
		t.Fatalf("payload type = %d", answer.Offer.PayloadType)
	}
	body := string(answer.Payload)
	for _, want := range []string{"m=audio 12000 RTP/AVP 0", "a=rtpmap:0 PCMU/8000", "a=ptime:20", "c=IN IP4 198.51.100.20"} {
		if !strings.Contains(body, want) {
			t.Fatalf("answer missing %q:\n%s", want, body)
		}
	}
}

func TestAnswerOfferRejectsNonPCMU(t *testing.T) {
	offer := []byte("v=0\r\n" +
		"o=- 1 1 IN IP4 192.0.2.10\r\n" +
		"s=-\r\n" +
		"c=IN IP4 192.0.2.10\r\n" +
		"t=0 0\r\n" +
		"m=audio 4000 RTP/AVP 8\r\n" +
		"a=rtpmap:8 PCMA/8000\r\n")

	if _, err := AnswerOffer(offer, netip.MustParseAddr("198.51.100.20"), 12000); err == nil {
		t.Fatal("expected non-PCMU offer to be rejected")
	}
}
