package gen

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	bridgev1 "github.com/meowraii/haze-weather-radio/services/go/internal/gen/bridge/v1"
	mediav1 "github.com/meowraii/haze-weather-radio/services/go/internal/gen/media/v1"
	"google.golang.org/protobuf/proto"
)

// These tests pin the design choices in contracts/bridge/v1/bridge.proto. They
// do not exercise a transport. They prove the Envelope can carry every member
// of today's JSON-lines events, including numbers a double cannot hold, so the
// Phase 2 gRPC bridge can be a lossless drop-in.

var routingStrings = []string{"id", "type", "source", "timestamp", "subject", "target", "reply_to"}

func envelopeFromJSON(t *testing.T, line string) *bridgev1.Envelope {
	t.Helper()
	var members map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &members); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	take := func(name string) string {
		var value string
		raw, ok := members[name]
		if !ok || json.Unmarshal(raw, &value) != nil {
			return ""
		}
		delete(members, name)
		return value
	}
	envelope := &bridgev1.Envelope{
		Id: take("id"), Type: take("type"), Source: take("source"), Timestamp: take("timestamp"),
		Subject: take("subject"), Target: take("target"), ReplyTo: take("reply_to"),
	}
	if raw, ok := members["data"]; ok {
		envelope.DataJson = raw
		delete(members, "data")
	}
	if len(members) > 0 {
		extra, err := json.Marshal(members)
		if err != nil {
			t.Fatal(err)
		}
		envelope.ExtraJson = extra
	}
	return envelope
}

func envelopeToJSON(t *testing.T, envelope *bridgev1.Envelope) []byte {
	t.Helper()
	members := map[string]json.RawMessage{}
	if len(envelope.ExtraJson) > 0 {
		if err := json.Unmarshal(envelope.ExtraJson, &members); err != nil {
			t.Fatal(err)
		}
	}
	set := func(name, value string) {
		if value == "" {
			return
		}
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		members[name] = raw
	}
	values := []string{envelope.Id, envelope.Type, envelope.Source, envelope.Timestamp, envelope.Subject, envelope.Target, envelope.ReplyTo}
	for index, name := range routingStrings {
		set(name, values[index])
	}
	if len(envelope.DataJson) > 0 {
		members["data"] = envelope.DataJson
	}
	out, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func canonical(t *testing.T, raw []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return value
}

func TestEnvelopeCarriesEveryJSONLinesMember(t *testing.T) {
	cases := map[string]string{
		"go service event": `{"id":"e1","type":"tts.synthesized","source":"haze-tts","timestamp":"2026-10-25T12:00:00.123456789Z",` +
			`"subject":"job-1","target":"haze-playout","reply_to":"haze-playlist","data":{"job_id":"job-1","output_path":"a/b.wav"}}`,
		"cap alert with top-level delivery_id": `{"type":"cap.alert.received","source":"naads","subject":"urn:parent","delivery_id":"cap-abc",` +
			`"id":"cap-abc","timestamp":"2026-10-25T12:00:00+00:00","data":{"delivery_id":"cap-abc","alert":{"identifier":"x"}}}`,
		"playout pcm with top-level feed and queue": `{"type":"playout.pcm","source":"haze-playout","feed_id":"feed-a","queue_id":"q1",` +
			`"timestamp":"2026-10-25T12:00:00Z","data":{"sequence":9007199254740993,"pts_ns":18446744073709551615,"pcm":"AAAA"}}`,
		"no data member":           `{"type":"service.ready","source":"haze-web"}`,
		"non-string timestamp":     `{"type":"x","timestamp":1761393600,"data":{}}`,
		"nested arrays and nulls":  `{"type":"x","data":{"a":[1,2,{"b":null}],"c":1.50}}`,
		"unmodelled member shapes": `{"type":"x","header":"H","feed_ids":["a","b"],"data":{"x":1}}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			envelope := envelopeFromJSON(t, line)
			wire, err := proto.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			var decoded bridgev1.Envelope
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			got := canonical(t, envelopeToJSON(t, &decoded))
			want := canonical(t, []byte(line))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip changed the event\n got: %v\nwant: %v", got, want)
			}
		})
	}
}

func TestDataJSONKeepsSixtyFourBitIntegersExact(t *testing.T) {
	envelope := envelopeFromJSON(t, `{"type":"playout.pcm","data":{"sequence":9007199254740993}}`)
	if !bytes.Contains(envelope.DataJson, []byte("9007199254740993")) {
		t.Fatalf("data_json lost integer precision: %s", envelope.DataJson)
	}
}

func TestConnectRequestOneofSelectsExactlyOneFrame(t *testing.T) {
	request := &bridgev1.ConnectRequest{Frame: &bridgev1.ConnectRequest_Hello{Hello: &bridgev1.Hello{
		ClientId: "haze-test", ReceiveEvents: true, Subscriptions: []string{"cap.*"},
	}}}
	wire, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var decoded bridgev1.ConnectRequest
	if err := proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetHello().GetClientId() != "haze-test" || decoded.GetPublish() != nil || decoded.GetLog() != nil {
		t.Fatalf("decoded frame = %v", &decoded)
	}
}

func TestMediaKindNumbersAreHPCMKindPlusOne(t *testing.T) {
	// crates/haze-media-protocol: Silence 0, Routine 1, Alert 2, OperatorBreakIn 3.
	want := map[mediav1.MediaKind]int32{
		mediav1.MediaKind_MEDIA_KIND_SILENCE:           0 + 1,
		mediav1.MediaKind_MEDIA_KIND_ROUTINE:           1 + 1,
		mediav1.MediaKind_MEDIA_KIND_ALERT:             2 + 1,
		mediav1.MediaKind_MEDIA_KIND_OPERATOR_BREAK_IN: 3 + 1,
	}
	for kind, number := range want {
		if int32(kind) != number {
			t.Errorf("%s = %d, want %d", kind, int32(kind), number)
		}
	}
	if mediav1.MediaKind_MEDIA_KIND_UNSPECIFIED != 0 {
		t.Fatal("zero value must stay unspecified")
	}
}
