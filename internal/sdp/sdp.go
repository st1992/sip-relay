package sdp

import (
	"errors"
	"fmt"
	"math/rand"
	"net/netip"
	"strconv"
	"strings"

	pionsdp "github.com/pion/sdp/v3"
)

const (
	CodecName  = "PCMU"
	SampleRate = 8000
)

type Offer struct {
	RemoteAddr  netip.Addr
	RemotePort  int
	PayloadType uint8
}

type Answer struct {
	Offer   Offer
	Payload []byte
}

func AnswerOffer(offerData []byte, advertisedIP netip.Addr, mediaPort int) (*Answer, error) {
	var desc pionsdp.SessionDescription
	if err := desc.Unmarshal(offerData); err != nil {
		return nil, err
	}

	offer, err := parseOffer(&desc)
	if err != nil {
		return nil, err
	}

	sessionID := uint64(rand.Int63())
	answer := pionsdp.SessionDescription{
		Version: 0,
		Origin: pionsdp.Origin{
			Username:       "-",
			SessionID:      sessionID,
			SessionVersion: sessionID,
			NetworkType:    "IN",
			AddressType:    addressType(advertisedIP),
			UnicastAddress: advertisedIP.String(),
		},
		SessionName: "sip-relay",
		ConnectionInformation: &pionsdp.ConnectionInformation{
			NetworkType: "IN",
			AddressType: addressType(advertisedIP),
			Address:     &pionsdp.Address{Address: advertisedIP.String()},
		},
		TimeDescriptions: []pionsdp.TimeDescription{
			{Timing: pionsdp.Timing{StartTime: 0, StopTime: 0}},
		},
		MediaDescriptions: []*pionsdp.MediaDescription{
			{
				MediaName: pionsdp.MediaName{
					Media:   "audio",
					Port:    pionsdp.RangedPort{Value: mediaPort},
					Protos:  []string{"RTP", "AVP"},
					Formats: []string{strconv.Itoa(int(offer.PayloadType))},
				},
				Attributes: []pionsdp.Attribute{
					{Key: "rtpmap", Value: fmt.Sprintf("%d PCMU/8000", offer.PayloadType)},
					{Key: "sendrecv"},
				},
			},
		},
	}

	payload, err := answer.Marshal()
	if err != nil {
		return nil, err
	}
	return &Answer{Offer: offer, Payload: payload}, nil
}

func parseOffer(desc *pionsdp.SessionDescription) (Offer, error) {
	if len(desc.MediaDescriptions) == 0 {
		return Offer{}, errors.New("SDP offer has no media descriptions")
	}

	var audio *pionsdp.MediaDescription
	for _, md := range desc.MediaDescriptions {
		if md.MediaName.Media == "audio" {
			audio = md
			break
		}
	}
	if audio == nil {
		return Offer{}, errors.New("SDP offer has no audio media")
	}
	if audio.MediaName.Port.Value <= 0 {
		return Offer{}, errors.New("SDP audio port is invalid")
	}

	payloadType, err := findPCMU(audio)
	if err != nil {
		return Offer{}, err
	}

	addr, err := connectionAddress(desc.ConnectionInformation, audio.ConnectionInformation)
	if err != nil {
		return Offer{}, err
	}
	return Offer{
		RemoteAddr:  addr,
		RemotePort:  audio.MediaName.Port.Value,
		PayloadType: payloadType,
	}, nil
}

func findPCMU(md *pionsdp.MediaDescription) (uint8, error) {
	allowed := make(map[string]struct{}, len(md.MediaName.Formats))
	for _, f := range md.MediaName.Formats {
		for _, field := range strings.Fields(f) {
			allowed[field] = struct{}{}
		}
	}

	if _, ok := allowed["0"]; ok {
		return 0, nil
	}

	for _, attr := range md.Attributes {
		if attr.Key != "rtpmap" {
			continue
		}
		fields := strings.Fields(attr.Value)
		if len(fields) < 2 {
			continue
		}
		if _, ok := allowed[fields[0]]; !ok {
			continue
		}
		codec := strings.ToUpper(fields[1])
		if codec == "PCMU/8000" || strings.HasPrefix(codec, "PCMU/8000/") {
			pt, err := strconv.ParseUint(fields[0], 10, 8)
			if err != nil {
				return 0, fmt.Errorf("invalid PCMU payload type %q: %w", fields[0], err)
			}
			return uint8(pt), nil
		}
	}
	return 0, errors.New("SDP offer does not include PCMU/8000")
}

func connectionAddress(session, media *pionsdp.ConnectionInformation) (netip.Addr, error) {
	conn := media
	if conn == nil {
		conn = session
	}
	if conn == nil || conn.Address == nil || conn.Address.Address == "" {
		return netip.Addr{}, errors.New("SDP offer has no connection address")
	}
	addr, err := netip.ParseAddr(conn.Address.Address)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("invalid SDP connection address: %w", err)
	}
	return addr, nil
}

func addressType(ip netip.Addr) string {
	if ip.Is6() {
		return "IP6"
	}
	return "IP4"
}
