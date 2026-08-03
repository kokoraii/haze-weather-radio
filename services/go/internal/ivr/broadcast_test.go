package ivr

import (
	"context"
	"encoding/base64"
	"net"
	"testing"
	"time"

	"github.com/gotranspile/g722"
)

func TestDecodeBroadcastPCMEventAndEncodeFrames(t *testing.T) {
	pcm := make([]byte, 48000/50*2)
	event := map[string]any{
		"type":    "playout.pcm",
		"feed_id": "sk-0001",
		"data": map[string]any{
			"sample_rate": 48000,
			"channels":    1,
			"pcm":         base64.StdEncoding.EncodeToString(pcm),
		},
	}
	chunk, ok := decodeBroadcastPCMEvent(event)
	if !ok {
		t.Fatal("event was not decoded")
	}
	if chunk.FeedID != "sk-0001" || chunk.SampleRate != 48000 || chunk.Channels != 1 {
		t.Fatalf("chunk = %#v", chunk)
	}
	pcmuFrames := pcmChunkToPCMUFrames(chunk)
	if len(pcmuFrames) != 1 || len(pcmuFrames[0]) != sipPacketSamples {
		t.Fatalf("PCMU frames = %d len=%d", len(pcmuFrames), len(pcmuFrames[0]))
	}
	g722Frames := pcmChunkToG722Frames(g722.NewEncoder(g722.Rate64000, 0), chunk)
	if len(g722Frames) != 1 || len(g722Frames[0]) != sipPacketSamples {
		t.Fatalf("G.722 frames = %d len=%d", len(g722Frames), len(g722Frames[0]))
	}
}

func TestSIPDirectFeedUsesLiveBroadcastUntilCallerHangup(t *testing.T) {
	receiver, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub := newBroadcastHub()
	call := &sipCall{
		service:      &Service{broadcast: hub},
		directFeedID: "sk-0001",
		ctx:          ctx,
		cancel:       cancel,
		rtpConn:      sender,
		remoteRTP:    cloneUDPAddr(receiver.LocalAddr().(*net.UDPAddr)),
		audioCodec:   sipAudioCodecPCMU,
		audioPayload: sipPayloadPCMU,
		dtmfPayload:  sipDefaultDTMFPayload,
		digits:       make(chan string, 16),
		done:         make(chan struct{}),
		ssrc:         1,
	}
	finished := make(chan struct{})
	go func() {
		call.run()
		close(finished)
	}()

	hub.publish(broadcastPCMChunk{
		FeedID:     "sk-0001",
		SampleRate: sipPCMUSampleRate,
		Channels:   1,
		Data:       make([]byte, sipPacketSamples*2),
	})
	if err := receiver.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 1500)
	n, _, err := receiver.ReadFromUDP(packet)
	if err != nil {
		t.Fatalf("direct feed did not send RTP: %v", err)
	}
	if n != 12+sipPacketSamples || int(packet[1]&0x7f) != sipPayloadPCMU {
		t.Fatalf("unexpected direct feed RTP packet: bytes=%d payload=%d", n, packet[1]&0x7f)
	}

	call.pushDigit("#")
	select {
	case <-finished:
		t.Fatal("direct feed call ended on IVR pound input")
	case <-time.After(100 * time.Millisecond):
	}

	call.close()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("direct feed call did not stop after caller hangup")
	}
}
