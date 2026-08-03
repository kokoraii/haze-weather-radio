package ivr

import (
	"time"

	"github.com/gotranspile/g722"
	"github.com/pion/rtp"
)

type sipInboundDecoder struct {
	codec       sipAudioCodec
	audioPT     uint8
	dtmfPT      uint8
	g722        *g722.Decoder
	initialized bool
	lastSeq     uint16
}

func newSIPInboundDecoder(codec sipAudioCodec, audioPayload int, dtmfPayload int) *sipInboundDecoder {
	decoder := &sipInboundDecoder{codec: codec, audioPT: uint8(audioPayload), dtmfPT: uint8(dtmfPayload)}
	if codec == sipAudioCodecG722 {
		decoder.g722 = g722.NewDecoder(g722.Rate64000, 0)
	}
	return decoder
}

func (decoder *sipInboundDecoder) Decode(raw []byte, at time.Time) ([]pcmFrame, string, string, bool) {
	var packet rtp.Packet
	if packet.Unmarshal(raw) != nil {
		return nil, "", "", false
	}
	if packet.PayloadType == decoder.dtmfPT {
		digit, key := rtpDTMFDigit(raw, int(decoder.dtmfPT))
		return nil, digit, key, digit != ""
	}
	if packet.PayloadType != decoder.audioPT || len(packet.Payload) == 0 {
		return nil, "", "", false
	}
	missing := 0
	if decoder.initialized {
		delta := uint16(packet.SequenceNumber - decoder.lastSeq)
		if delta == 0 || delta > 0x8000 {
			return nil, "", "", false
		}
		if delta > 1 {
			missing = minInt(int(delta)-1, 3)
			if decoder.codec == sipAudioCodecG722 {
				decoder.g722 = g722.NewDecoder(g722.Rate64000, 0)
			}
		}
	}
	decoder.initialized = true
	decoder.lastSeq = packet.SequenceNumber
	frames := make([]pcmFrame, 0, missing+1)
	frameSamples := sipG722FrameSamples
	for index := 0; index < missing; index++ {
		frames = append(frames, pcmFrame{Samples: make([]int16, frameSamples), At: at.Add(-time.Duration(missing-index) * 20 * time.Millisecond)})
	}
	samples := decoder.decodePayload(packet.Payload)
	if len(samples) > 0 {
		frames = append(frames, pcmFrame{Samples: samples, At: at})
	}
	return frames, "", "", len(frames) > 0
}

func (decoder *sipInboundDecoder) decodePayload(payload []byte) []int16 {
	if decoder.codec == sipAudioCodecG722 {
		output := make([]int16, len(payload)*2+8)
		n := decoder.g722.Decode(output, payload)
		if n <= 0 {
			return nil
		}
		return output[:n]
	}
	output := make([]int16, len(payload)*2)
	for index, value := range payload {
		sample := uLawToLinear(value)
		output[index*2] = sample
		output[index*2+1] = sample
	}
	return output
}

func uLawToLinear(value byte) int16 {
	value = ^value
	sign := value & 0x80
	exponent := (value >> 4) & 0x07
	mantissa := value & 0x0f
	sample := ((int(mantissa) << 3) + 0x84) << exponent
	sample -= 0x84
	if sign != 0 {
		sample = -sample
	}
	return int16(sample)
}
