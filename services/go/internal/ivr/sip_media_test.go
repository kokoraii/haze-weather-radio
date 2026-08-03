package ivr

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/gotranspile/g722"
	"github.com/pion/rtp"
)

func TestSIPInboundPCMUHandlesExtensionsPaddingLossAndDuplicates(t *testing.T) {
	t.Parallel()
	decoder := newSIPInboundDecoder(sipAudioCodecPCMU, sipPayloadPCMU, sipDefaultDTMFPayload)
	now := time.Unix(100, 0)
	first := marshalTestRTP(t, &rtp.Packet{Header: rtp.Header{
		Version: 2, PayloadType: sipPayloadPCMU, SequenceNumber: 10,
		Extension: true, ExtensionProfile: 0xBEDE, Extensions: []rtp.Extension{{}},
	}, Payload: make([]byte, sipPacketSamples)})
	frames, _, _, ok := decoder.Decode(first, now)
	if !ok || len(frames) != 1 || len(frames[0].Samples) != sipG722FrameSamples {
		t.Fatalf("unexpected PCMU decode: ok=%t frames=%d", ok, len(frames))
	}
	if frames, _, _, ok = decoder.Decode(first, now); ok || len(frames) != 0 {
		t.Fatal("duplicate RTP packet was not dropped")
	}
	third := marshalTestRTP(t, &rtp.Packet{Header: rtp.Header{
		Version: 2, Padding: true, PayloadType: sipPayloadPCMU, SequenceNumber: 12,
	}, Payload: make([]byte, sipPacketSamples), PaddingSize: 4})
	frames, _, _, ok = decoder.Decode(third, now.Add(40*time.Millisecond))
	if !ok || len(frames) != 2 {
		t.Fatalf("sequence gap did not insert one bounded silence frame: %d", len(frames))
	}
	if !pcmIsSilent(frames[0].Samples) {
		t.Fatal("gap frame was not silence")
	}
}

func TestSIPInboundG722AndRFC2833RemainIndependent(t *testing.T) {
	t.Parallel()
	decoder := newSIPInboundDecoder(sipAudioCodecG722, sipPayloadG722, sipDefaultDTMFPayload)
	encoded := make([]byte, sipPacketSamples)
	n := g722.NewEncoder(g722.Rate64000, 0).Encode(encoded, alternatingSamples(sipG722FrameSamples, 2000))
	audio := marshalTestRTP(t, &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: sipPayloadG722, SequenceNumber: 20}, Payload: encoded[:n]})
	frames, _, _, ok := decoder.Decode(audio, time.Now())
	if !ok || len(frames) != 1 || len(frames[0].Samples) == 0 {
		t.Fatalf("G.722 decode failed: frames=%d", len(frames))
	}

	dtmfPayload := []byte{5, 0x80, 0, 80}
	dtmf := marshalTestRTP(t, &rtp.Packet{Header: rtp.Header{
		Version: 2, Padding: true, Extension: true, ExtensionProfile: 0xBEDE, Extensions: []rtp.Extension{{}},
		PayloadType: sipDefaultDTMFPayload, SequenceNumber: 21, SSRC: 99, Timestamp: 123,
	}, Payload: dtmfPayload, PaddingSize: 4})
	_, digit, _, ok := decoder.Decode(dtmf, time.Now())
	if !ok || digit != "5" {
		t.Fatalf("RFC2833 decode = %q, ok=%t", digit, ok)
	}
}

func marshalTestRTP(t *testing.T, packet *rtp.Packet) []byte {
	t.Helper()
	raw, err := packet.Marshal()
	if err != nil {
		t.Fatalf("marshal RTP: %v", err)
	}
	return raw
}

func TestULawRoundTripPolarity(t *testing.T) {
	t.Parallel()
	for _, sample := range []int16{-12000, -1000, 0, 1000, 12000} {
		decoded := uLawToLinear(linearToULaw(sample))
		if sample < 0 && decoded >= 0 || sample > 0 && decoded <= 0 {
			t.Fatalf("round trip changed polarity: %d to %d", sample, decoded)
		}
	}
}

func testRFC2833Raw(payloadType byte, sequence uint16, event byte) []byte {
	raw := make([]byte, 16)
	raw[0] = 0x80
	raw[1] = payloadType
	binary.BigEndian.PutUint16(raw[2:4], sequence)
	raw[12] = event
	raw[13] = 0x80
	return raw
}
