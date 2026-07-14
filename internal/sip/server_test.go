package sip

import (
	"errors"
	"testing"

	sipmsg "github.com/livekit/sipgo/sip"
)

func TestRespondTryingSendsProvisionalResponse(t *testing.T) {
	req := sipmsg.NewRequest(sipmsg.INVITE, sipmsg.Uri{User: "relay", Host: "example.com"})
	tx := &fakeServerTransaction{done: make(chan struct{})}

	respondTrying(req, tx)

	if len(tx.responses) != 1 {
		t.Fatalf("responses = %d, want 1", len(tx.responses))
	}
	resp := tx.responses[0]
	if resp.StatusCode != sipmsg.StatusTrying {
		t.Fatalf("status = %d, want %d", resp.StatusCode, sipmsg.StatusTrying)
	}
	if resp.Reason != "Trying" {
		t.Fatalf("reason = %q, want Trying", resp.Reason)
	}
	if tx.terminated {
		t.Fatal("100 Trying should not terminate the transaction")
	}
}

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

func TestServerBYEUsesServerSideDialogHeaders(t *testing.T) {
	invite := sipmsg.NewRequest(sipmsg.INVITE, sipmsg.Uri{User: "relay", Host: "example.com"})
	invite.SetSource("198.51.100.10:5060")
	invite.SetTransport("UDP")
	invite.AppendHeader(&sipmsg.FromHeader{
		Address: sipmsg.Uri{User: "caller", Host: "provider.example.com"},
		Params:  paramsWith("tag", "remote-tag"),
	})
	invite.AppendHeader(&sipmsg.ToHeader{
		Address: sipmsg.Uri{User: "relay", Host: "example.com"},
		Params:  sipmsg.NewParams(),
	})
	callID := sipmsg.CallIDHeader("call-1")
	invite.AppendHeader(&callID)
	invite.AppendHeader(&sipmsg.CSeqHeader{SeqNo: 7, MethodName: sipmsg.INVITE})
	invite.AppendHeader(&sipmsg.ContactHeader{
		Address: sipmsg.Uri{User: "caller", Host: "198.51.100.10", Port: 5060},
		Params:  sipmsg.NewParams(),
	})

	ok := sipmsg.NewResponseFromRequest(invite, sipmsg.StatusOK, "OK", nil)
	if to := ok.To(); to != nil {
		to.Params.Add("tag", "local-tag")
	}

	bye := newServerBYE(&dialog{invite: invite, ok: ok})
	if bye == nil {
		t.Fatal("missing BYE request")
	}
	if bye.Method != sipmsg.BYE {
		t.Fatalf("method = %s, want BYE", bye.Method)
	}
	if got := bye.Destination(); got != "198.51.100.10:5060" {
		t.Fatalf("destination = %q", got)
	}
	if got := bye.From().Params.GetOr("tag", ""); got != "local-tag" {
		t.Fatalf("from tag = %q, want local tag", got)
	}
	if got := bye.To().Params.GetOr("tag", ""); got != "remote-tag" {
		t.Fatalf("to tag = %q, want remote tag", got)
	}
	if got := bye.CSeq(); got == nil || got.MethodName != sipmsg.BYE || got.SeqNo != 8 {
		t.Fatalf("cseq = %#v, want 8 BYE", got)
	}
}

func paramsWith(key, value string) sipmsg.HeaderParams {
	params := sipmsg.NewParams()
	params.Add(key, value)
	return params
}

type fakeServerTransaction struct {
	responses  []*sipmsg.Response
	terminated bool
	done       chan struct{}
	err        error
}

func (tx *fakeServerTransaction) Respond(res *sipmsg.Response) error {
	tx.responses = append(tx.responses, res)
	return nil
}

func (tx *fakeServerTransaction) Terminate() {
	tx.terminated = true
}

func (tx *fakeServerTransaction) Done() <-chan struct{} {
	return tx.done
}

func (tx *fakeServerTransaction) Err() error {
	if tx.err != nil {
		return tx.err
	}
	return errors.New("fake transaction error")
}

func (tx *fakeServerTransaction) Acks() <-chan *sipmsg.Request {
	return nil
}

func (tx *fakeServerTransaction) Cancels() <-chan *sipmsg.Request {
	return nil
}
