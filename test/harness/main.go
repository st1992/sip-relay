package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/livekit/sipgo"
	"github.com/livekit/sipgo/sip"
	pionrtp "github.com/pion/rtp"
	pionsdp "github.com/pion/sdp/v3"
)

func main() {
	sipDest := flag.String("sip-dest", "127.0.0.1:5060", "SIP destination host:port")
	toUser := flag.String("to", "relay", "SIP recipient user")
	fromUser := flag.String("from", "harness", "SIP Contact user")
	advertisedIP := flag.String("advertised-ip", "127.0.0.1", "IP advertised in SDP and Contact")
	payloadHex := flag.String("payload", "fffefdfcfbfaf9f8", "PCMU payload bytes to send as hex")
	flag.Parse()

	payload, err := hex.DecodeString(*payloadHex)
	if err != nil {
		log.Fatal(err)
	}

	rtpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(*advertisedIP)})
	if err != nil {
		log.Fatal(err)
	}
	defer rtpConn.Close()

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("sip-relay-harness"))
	if err != nil {
		log.Fatal(err)
	}
	defer ua.Close()

	client, err := sipgo.NewClient(ua, sipgo.WithClientHostname(*advertisedIP))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	offer, err := createOffer(*advertisedIP, rtpConn.LocalAddr().(*net.UDPAddr).Port)
	if err != nil {
		log.Fatal(err)
	}

	host, _, err := net.SplitHostPort(*sipDest)
	if err != nil {
		log.Fatal(err)
	}
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: *toUser, Host: host})
	req.SetDestination(*sipDest)
	req.SetBody(offer)
	req.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
	req.AppendHeader(sip.NewHeader("Contact", fmt.Sprintf("<sip:%s@%s>", *fromUser, *sipDest)))

	tx, err := client.TransactionRequest(req)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Terminate()

	resp, err := waitFinal(tx)
	if err != nil {
		log.Fatal(err)
	}
	if resp.StatusCode != 200 {
		log.Fatalf("INVITE failed with status %d", resp.StatusCode)
	}
	if err := client.WriteRequest(sip.NewAckRequest(req, resp, nil)); err != nil {
		log.Fatal(err)
	}

	rtpHost, rtpPort, err := parseAnswer(resp.Body())
	if err != nil {
		log.Fatal(err)
	}
	dst, err := net.ResolveUDPAddr("udp", net.JoinHostPort(rtpHost, strconv.Itoa(rtpPort)))
	if err != nil {
		log.Fatal(err)
	}

	packet := pionrtp.Packet{
		Header:  pionrtp.Header{Version: 2, PayloadType: 0, SequenceNumber: 1, Timestamp: 160, SSRC: 42},
		Payload: payload,
	}
	raw, err := packet.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	if _, err := rtpConn.WriteToUDP(raw, dst); err != nil {
		log.Fatal(err)
	}
	log.Printf("sent PCMU payload: %s", hex.EncodeToString(payload))

	_ = rtpConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1500)
	n, _, err := rtpConn.ReadFromUDP(buf)
	if err != nil {
		log.Printf("no returned RTP payload within timeout: %v", err)
		return
	}
	var returned pionrtp.Packet
	if err := returned.Unmarshal(buf[:n]); err != nil {
		log.Fatal(err)
	}
	log.Printf("received PCMU payload: %s", hex.EncodeToString(returned.Payload))
}

func createOffer(ip string, port int) ([]byte, error) {
	desc := pionsdp.SessionDescription{
		Version: 0,
		Origin: pionsdp.Origin{
			Username:       "-",
			SessionID:      1,
			SessionVersion: 1,
			NetworkType:    "IN",
			AddressType:    "IP4",
			UnicastAddress: ip,
		},
		SessionName: "sip-relay-harness",
		ConnectionInformation: &pionsdp.ConnectionInformation{
			NetworkType: "IN",
			AddressType: "IP4",
			Address:     &pionsdp.Address{Address: ip},
		},
		TimeDescriptions: []pionsdp.TimeDescription{
			{Timing: pionsdp.Timing{StartTime: 0, StopTime: 0}},
		},
		MediaDescriptions: []*pionsdp.MediaDescription{
			{
				MediaName: pionsdp.MediaName{
					Media:   "audio",
					Port:    pionsdp.RangedPort{Value: port},
					Protos:  []string{"RTP", "AVP"},
					Formats: []string{"0"},
				},
				Attributes: []pionsdp.Attribute{{Key: "rtpmap", Value: "0 PCMU/8000"}},
			},
		},
	}
	return desc.Marshal()
}

func waitFinal(tx sip.ClientTransaction) (*sip.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tx.Done():
			return nil, fmt.Errorf("transaction ended before final response")
		case resp := <-tx.Responses():
			if resp.StatusCode >= 200 {
				return resp, nil
			}
		}
	}
}

func parseAnswer(body []byte) (string, int, error) {
	var desc pionsdp.SessionDescription
	if err := desc.Unmarshal(body); err != nil {
		return "", 0, err
	}
	if desc.ConnectionInformation == nil || desc.ConnectionInformation.Address == nil {
		return "", 0, fmt.Errorf("answer has no connection address")
	}
	if len(desc.MediaDescriptions) == 0 {
		return "", 0, fmt.Errorf("answer has no media")
	}
	return desc.ConnectionInformation.Address.Address, desc.MediaDescriptions[0].MediaName.Port.Value, nil
}
