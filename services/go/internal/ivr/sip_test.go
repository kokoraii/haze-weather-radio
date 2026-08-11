package ivr

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func acceptTestSIPInvite(service *Service, ctx context.Context, request sipRequest, remote *net.UDPAddr, local net.Addr) (*sipCall, string) {
	return service.acceptSIPInvite(ctx, request, remote, nil, local)
}

func TestSIPListenBindingsSupportDomainBoundPort(t *testing.T) {
	var root rootConfig
	err := yaml.Unmarshal([]byte(`
services:
  go:
    ivr:
      sip:
        listen: "0.0.0.0:5060"
        listen_ports:
          - port: 5060
          - port: 5080
            domain: sip.teleweather.ca
`), &root)
	if err != nil {
		t.Fatal(err)
	}
	cfg := root.Services.Go.IVR
	normalizeIVRConfig(&cfg)

	bindings := cfg.SIP.listenBindings()
	if len(bindings) != 2 {
		t.Fatalf("bindings = %#v", bindings)
	}
	if bindings[0].Addr != "0.0.0.0:5060" || bindings[0].Domain != "" {
		t.Fatalf("first binding = %#v", bindings[0])
	}
	if bindings[1].Addr != "0.0.0.0:5080" || bindings[1].Domain != "sip.teleweather.ca" {
		t.Fatalf("second binding = %#v", bindings[1])
	}
}

func TestEquivalentSIPListenOverridePreservesAdditionalPorts(t *testing.T) {
	cfg := sipConfig{
		Listen: "0.0.0.0:5060",
		ListenPorts: sipListenPorts{
			{Port: 5060},
			{Port: 5080, Domain: "sip.teleweather.ca"},
		},
	}
	applySIPListenOverride(&cfg, "0.0.0.0:5060")
	if bindings := cfg.listenBindings(); len(bindings) != 2 {
		t.Fatalf("equivalent daemon override removed configured listeners: %#v", bindings)
	}

	applySIPListenOverride(&cfg, "127.0.0.1:5099")
	bindings := cfg.listenBindings()
	if len(bindings) != 1 || bindings[0].Addr != "127.0.0.1:5099" {
		t.Fatalf("explicit alternate override did not replace configured listeners: %#v", bindings)
	}
}

func TestSIPDomainAllowedMatchesRequestURI(t *testing.T) {
	service := &Service{}
	request := sampleSIPInviteRequest()
	request.URI = "sip:ivr@sip.teleweather.ca:5080"

	if !service.sipDomainAllowed(request, "sip.teleweather.ca") {
		t.Fatal("expected matching SIP domain to be allowed")
	}
	if service.sipDomainAllowed(request, "other.sip.rai.blue") {
		t.Fatal("expected mismatched SIP domain to be rejected")
	}
	if !service.sipDomainAllowed(request, "") {
		t.Fatal("expected blank listener domain to allow request")
	}
}

func TestNormalizeIVRExtensionsAddsEnabledCanadaLine(t *testing.T) {
	cfg := Config{}
	normalizeIVRConfig(&cfg)
	if len(cfg.Extensions) != 1 {
		t.Fatalf("extensions = %#v", cfg.Extensions)
	}
	line := cfg.Extensions[0]
	if line.Extension != "haze" || line.Province != "CA" || !line.enabled() {
		t.Fatalf("default line = %#v", line)
	}
}

func TestNormalizeIVRConfigBoundsSearchAndTwilioResources(t *testing.T) {
	enabled := true
	cfg := Config{
		MaxConcurrentCalls: 10,
		Search: searchConfig{
			Enabled: &enabled, MaxAttempts: 9, VADPrerollMS: 9000, VADTrailingMS: 9000,
			MinVoiceMS: 10, MaxVoiceMS: 500,
		},
		Twilio: twilioConfig{MaxMessageBytes: 1024 * 1024, MaxConcurrentStreams: 99},
	}
	normalizeIVRConfig(&cfg)
	if cfg.Search.MaxAttempts != 2 || cfg.Search.VADPrerollMS != 1000 || cfg.Search.VADTrailingMS != 2000 {
		t.Fatalf("search bounds = %#v", cfg.Search)
	}
	if cfg.Search.MinVoiceMS != 2000 || cfg.Search.MaxVoiceMS != 2000 {
		t.Fatalf("voice duration bounds = %d..%d", cfg.Search.MinVoiceMS, cfg.Search.MaxVoiceMS)
	}
	if cfg.Twilio.MaxMessageBytes != 256*1024 || cfg.Twilio.MaxConcurrentStreams != 10 {
		t.Fatalf("Twilio bounds = %#v", cfg.Twilio)
	}
}

func TestExtensionTelephoneServiceNameSupportsPlacementAndPronunciation(t *testing.T) {
	service := &Service{cfg: loadedConfig{}}
	service.cfg.Root.Operator.TelephoneName = map[string]any{"pronunciation": "tele weather"}
	before := extensionConfig{Name: map[string]any{"text": "Saskatchewan", "pronunciation": "sask at chew on"}, NamePosition: "before"}
	after := extensionConfig{Name: "Canada", NamePosition: "after"}
	if got := service.extensionTelephoneServiceName(before); got != "sask at chew on tele weather" {
		t.Fatalf("before greeting name = %q", got)
	}
	if got := service.extensionTelephoneServiceName(after); got != "tele weather Canada" {
		t.Fatalf("after greeting name = %q", got)
	}
}

func TestSIPInviteSelectsConfiguredProvinceLine(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		Extensions: []extensionConfig{{Extension: "600", Province: "SK"}},
		RTP:        rtpConfig{PortMin: 0, PortMax: 0},
	}, Prompts: defaultPromptConfig()}}
	request := sampleSIPInviteRequest()
	request.URI = "sip:600@sip.teleweather.ca"

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted province call, response: %s", response)
	}
	defer call.close()
	if call.line.Extension != "600" || call.line.directProvince() != "SK" {
		t.Fatalf("selected line = %#v", call.line)
	}
}

func TestSIPInviteAnswerUsesRegisteredContactIdentity(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		Extensions: []extensionConfig{{Extension: "haze", Province: "CA"}},
		RTP:        rtpConfig{PortMin: 0, PortMax: 0},
		SIP: sipConfig{Registration: sipRegistrationConfig{
			Username:    "tollfreef",
			ContactUser: "tollfreef",
		}},
	}, Prompts: defaultPromptConfig()}}
	request := sampleSIPInviteRequest()
	local := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060}
	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, local)
	if call == nil {
		t.Fatalf("expected accepted call, response: %s", response)
	}
	defer call.close()
	if !strings.Contains(response, "Contact: <sip:tollfreef@127.0.0.1:5060>") {
		t.Fatalf("SIP answer did not preserve the registered contact: %s", response)
	}
}

func TestSIPReplyPreservesAllViaHeaders(t *testing.T) {
	headers := sipHeaders([]string{
		"Via: SIP/2.0/UDP edge-1.example;branch=z9hG4bK-first",
		"Via: SIP/2.0/UDP edge-2.example;branch=z9hG4bK-second",
		"From: <sip:caller@example>;tag=caller",
		"To: <sip:haze@example>",
		"Call-ID: test-call",
		"CSeq: 1 INVITE",
	})
	response := sipReply("100 Trying", headers, "")
	first := strings.Index(response, "Via: SIP/2.0/UDP edge-1.example;branch=z9hG4bK-first\r\n")
	second := strings.Index(response, "Via: SIP/2.0/UDP edge-2.example;branch=z9hG4bK-second\r\n")
	if first < 0 || second < 0 || first >= second {
		t.Fatalf("SIP response did not preserve Via order: %s", response)
	}
}

func TestSIPReplyToAddsRequestedResponseRoutingParameters(t *testing.T) {
	headers := sipHeaders([]string{
		"Via: SIP/2.0/UDP provider.example:5060;rport;branch=z9hG4bK-one",
		"From: <sip:caller@example.net>;tag=origin",
		"To: <sip:tollfreef@example.net>",
		"Call-ID: call-routing",
		"CSeq: 1 INVITE",
	})
	reply := sipReplyWithBodyTo("200 OK", headers, "haze", "application/sdp", "v=0\r\n", "71.17.68.130", 5060, "tollfreef", "", &net.UDPAddr{IP: net.ParseIP("159.65.244.171"), Port: 5060})
	if !strings.Contains(reply, "Via: SIP/2.0/UDP provider.example:5060;rport=5060;branch=z9hG4bK-one;received=159.65.244.171\r\n") {
		t.Fatalf("response did not set RFC 3581 parameters: %q", reply)
	}
}

func TestSIPProvinceLineSurvivesProxyRequestURIRewrite(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{Extensions: []extensionConfig{
		{Extension: "haze", Province: "CA"},
		{Extension: "600", Province: "SK"},
	}}}}
	request := sampleSIPInviteRequest()
	request.URI = "sip:haze@sip.teleweather.ca"
	request.Headers["to"] = "<sip:600@sip.teleweather.ca>"

	line, matched := service.sipRequestLine(request)
	if !matched || line.Extension != "600" || line.directProvince() != "SK" {
		t.Fatalf("selected line = %#v, matched=%v", line, matched)
	}
}

func TestSIPCalledDIDSelectsLineWithoutReplacingCallerIdentity(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{Extensions: []extensionConfig{
		{Extension: "haze", Name: "Canada", Province: "CA"},
		{Extension: "600", Name: "Saskatchewan", Province: "SK"},
		{Extension: "900", DIDs: []string{"+18675550123"}, Name: "Yukon", Province: "YT"},
		{Extension: "national", DIDs: []string{"3065550199"}, Name: "Canada", Province: "CA"},
	}}}}
	tests := []struct {
		name          string
		requestURI    string
		to            string
		from          string
		wantExtension string
		wantAddress   string
	}{
		{name: "Saskatchewan area code", requestURI: "sip:+13065551234@sip.teleweather.ca", wantExtension: "600", wantAddress: "sip:+13065551234@sip.teleweather.ca"},
		{name: "DID survives proxy rewrite", requestURI: "sip:haze@sip.teleweather.ca", to: "<sip:13065551234@sip.teleweather.ca>", wantExtension: "600", wantAddress: "<sip:13065551234@sip.teleweather.ca>"},
		{name: "toll free defaults Canada", requestURI: "sip:+18005550123@sip.teleweather.ca", wantExtension: "haze", wantAddress: "sip:+18005550123@sip.teleweather.ca"},
		{name: "explicit DID handles shared area code", requestURI: "sip:+18675550123@sip.teleweather.ca", wantExtension: "900", wantAddress: "sip:+18675550123@sip.teleweather.ca"},
		{name: "explicit DID overrides area inference", requestURI: "sip:+13065550199@sip.teleweather.ca", wantExtension: "national", wantAddress: "sip:+13065550199@sip.teleweather.ca"},
		{name: "calling party is not used as called line", requestURI: "sip:haze@sip.teleweather.ca", from: `"Saskatchewan caller" <sip:+13065551234@carrier.example>;tag=test`, wantExtension: "haze", wantAddress: "sip:haze@sip.teleweather.ca"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := sampleSIPInviteRequest()
			request.URI = test.requestURI
			if test.to != "" {
				request.Headers["to"] = test.to
			}
			if test.from != "" {
				request.Headers["from"] = test.from
			}
			selection := service.sipRequestLineSelection(request)
			if selection.Line.Extension != test.wantExtension || selection.CalledAddress != test.wantAddress {
				t.Fatalf("selection = %#v, want extension %q and address %q", selection, test.wantExtension, test.wantAddress)
			}
		})
	}
}

func TestSIPAcceptedCallAdvertisesConnectedLineName(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		Extensions: []extensionConfig{
			{Extension: "haze", Name: map[string]any{"text": "Canada", "pronunciation": "can ah dah"}, Province: "CA"},
			{Extension: "600", Name: map[string]any{"text": "Saskatchewan", "pronunciation": "sask at chew on"}, Province: "SK"},
		},
		RTP: rtpConfig{PortMin: 0, PortMax: 0},
	}, Prompts: defaultPromptConfig()}}
	service.cfg.Root.Operator.TelephoneName = map[string]any{"text": "TeleWeather", "pronunciation": "tele weather"}
	request := sampleSIPInviteRequest()
	request.URI = "sip:+13065551234@sip.teleweather.ca"
	request.Headers["to"] = "<sip:+13065551234@sip.teleweather.ca>"

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted call, response: %s", response)
	}
	defer call.close()
	identity := `"Saskatchewan TeleWeather" <sip:+13065551234@sip.teleweather.ca>`
	if !strings.Contains(response, "P-Asserted-Identity: "+identity+"\r\n") ||
		!strings.Contains(response, "Remote-Party-ID: "+identity+";party=called;screen=yes;privacy=off\r\n") {
		t.Fatalf("connected line identity missing from response: %s", response)
	}
	if !strings.Contains(response, "From: <sip:caller@127.0.0.1>;tag=abc\r\n") {
		t.Fatalf("response replaced the inbound caller identity: %s", response)
	}
}

func TestSIPConnectedLineHeadersRejectInjection(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		address string
		want    string
	}{
		{name: "safe", line: "Saskatchewan TeleWeather", address: "sip:600@sip.teleweather.ca", want: `P-Asserted-Identity: "Saskatchewan TeleWeather" <sip:600@sip.teleweather.ca>`},
		{name: "escaped", line: `Canada "Weather"`, address: "sip:haze@sip.teleweather.ca", want: `P-Asserted-Identity: "Canada \"Weather\"" <sip:haze@sip.teleweather.ca>`},
		{name: "invalid URI", line: "Canada TeleWeather", address: "sip:haze@bad host", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			headers := sipConnectedLineHeaders(test.line, test.address)
			if test.want == "" {
				if headers != "" {
					t.Fatalf("headers = %q, want empty", headers)
				}
				return
			}
			if !strings.Contains(headers, test.want) {
				t.Fatalf("headers = %q, missing %q", headers, test.want)
			}
			if strings.Contains(headers, "\r\nX-Injected:") {
				t.Fatalf("headers contain an injected field: %q", headers)
			}
		})
	}
	if headers := sipConnectedLineHeaders("Canada\r\nX-Injected: yes", "sip:haze@sip.teleweather.ca"); strings.Contains(headers, "\r\nX-Injected:") {
		t.Fatalf("control characters injected a SIP header: %q", headers)
	}
}

func TestIVRExtensionDIDAndCallerIDValidation(t *testing.T) {
	tests := []struct {
		name    string
		lines   []extensionConfig
		wantErr string
	}{
		{name: "valid", lines: []extensionConfig{{Extension: "haze", Province: "CA", NamePosition: "before", DIDs: []string{"3065550123"}, CallerIDName: "Canada TeleWeather"}}},
		{name: "duplicate formatted DID", lines: []extensionConfig{
			{Extension: "haze", Province: "CA", NamePosition: "before", DIDs: []string{"306-555-0123"}},
			{Extension: "600", Province: "SK", NamePosition: "before", DIDs: []string{"+13065550123"}},
		}, wantErr: "configured for extensions"},
		{name: "invalid DID", lines: []extensionConfig{{Extension: "haze", Province: "CA", NamePosition: "before", DIDs: []string{"not-a-number"}}}, wantErr: "invalid DID"},
		{name: "header injection", lines: []extensionConfig{{Extension: "haze", Province: "CA", NamePosition: "before", CallerIDName: "Canada\r\nX-Injected: yes"}}, wantErr: "invalid caller_id_name"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateIVRExtensions(test.lines)
			if test.wantErr == "" && err != nil {
				t.Fatalf("validateIVRExtensions() error = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("validateIVRExtensions() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestSIPRequestDirectFeedMatchesConfiguredRequestURI(t *testing.T) {
	service := &Service{cfg: loadedConfig{Feeds: []feedXML{
		{ID: "sk-0001", EnabledRaw: "true"},
		{ID: "CFSP-CAP", EnabledRaw: "true"},
		{ID: "off-0001", EnabledRaw: "false"},
	}}}
	tests := []struct {
		name string
		uri  string
		to   string
		want sipDirectFeedTarget
	}{
		{name: "enabled", uri: "sip:sk-0001@172.16.1.31:5060", want: sipDirectFeedTarget{ID: "sk-0001", Matched: true, Enabled: true}},
		{name: "case insensitive canonical result", uri: "sip:cfsp-cap@172.16.1.31", want: sipDirectFeedTarget{ID: "CFSP-CAP", Matched: true, Enabled: true}},
		{name: "disabled", uri: "sip:off-0001@172.16.1.31", want: sipDirectFeedTarget{ID: "off-0001", Matched: true, Enabled: false}},
		{name: "unknown", uri: "sip:not-a-feed@172.16.1.31"},
		{name: "request URI only", uri: "sip:haze@172.16.1.31", to: "<sip:sk-0001@172.16.1.31>"},
		{name: "path syntax rejected", uri: "sip:..%2fsk-0001@172.16.1.31"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := sampleSIPInviteRequest()
			request.URI = test.uri
			if test.to != "" {
				request.Headers["to"] = test.to
			}
			if got := service.sipRequestDirectFeed(request); got != test.want {
				t.Fatalf("direct feed target = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestValidSIPDirectFeedIDRejectsUnsafeOrOversizedValues(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "sk-0001", want: true},
		{value: "CFSP-CAP", want: true},
		{value: "feed_name.2", want: true},
		{value: ""},
		{value: "../sk-0001"},
		{value: "sk-0001%0d%0a"},
		{value: "sk 0001"},
		{value: strings.Repeat("a", 129)},
	}
	for _, test := range tests {
		if got := validSIPDirectFeedID(test.value); got != test.want {
			t.Errorf("validSIPDirectFeedID(%q) = %v, want %v", test.value, got, test.want)
		}
	}
}

func TestSIPInviteDirectFeedUsesCanonicalFeedWithoutCallDeadline(t *testing.T) {
	service := &Service{cfg: loadedConfig{
		IVR: Config{
			Extensions:     []extensionConfig{{Extension: "haze", Province: "CA"}},
			MaxCallSeconds: 7,
			RTP:            rtpConfig{PortMin: 0, PortMax: 0},
		},
		Feeds: []feedXML{{ID: "CFSP-CAP", EnabledRaw: "true"}},
	}}
	service.broadcast = newBroadcastHub()
	service.broadcast.SetHTTPSource("http://127.0.0.1:8097")
	request := sampleSIPInviteRequest()
	request.URI = "sip:cfsp-cap@172.16.1.31:5060"
	request.Headers["to"] = "<sip:cfsp-cap@172.16.1.31:5060>"

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted direct feed call, response: %s", response)
	}
	defer call.close()
	if call.directFeedID != "CFSP-CAP" {
		t.Fatalf("direct feed ID = %q, want canonical feed ID", call.directFeedID)
	}
	if _, ok := call.ctx.Deadline(); ok {
		t.Fatal("direct feed call received an IVR menu deadline")
	}
}

func TestSIPInviteDirectFeedAvailability(t *testing.T) {
	tests := []struct {
		name       string
		enabledRaw string
		media      bool
		warning    string
	}{
		{name: "disabled feed", enabledRaw: "false", media: true, warning: "live feed is disabled"},
		{name: "media unavailable", enabledRaw: "true", warning: "live feed is unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &Service{cfg: loadedConfig{
				IVR:   Config{Extensions: []extensionConfig{{Extension: "haze", Province: "CA"}}},
				Feeds: []feedXML{{ID: "sk-0001", EnabledRaw: test.enabledRaw}},
			}}
			service.broadcast = newBroadcastHub()
			if test.media {
				service.broadcast.SetHTTPSource("http://127.0.0.1:8097")
			}
			request := sampleSIPInviteRequest()
			request.URI = "sip:sk-0001@172.16.1.31:5060"
			request.Headers["to"] = "<sip:sk-0001@172.16.1.31:5060>"

			call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
			if call != nil {
				call.close()
				t.Fatal("unavailable direct feed accepted a call")
			}
			if !strings.Contains(response, "480 Temporarily Unavailable") || !strings.Contains(response, test.warning) {
				t.Fatalf("direct feed response = %q", response)
			}
		})
	}
}

func TestSIPInviteExtensionTakesPrecedenceOverCollidingFeedID(t *testing.T) {
	service := &Service{cfg: loadedConfig{
		IVR: Config{
			Extensions:     []extensionConfig{{Extension: "600", Province: "SK"}},
			MaxCallSeconds: 7,
			RTP:            rtpConfig{PortMin: 0, PortMax: 0},
		},
		Feeds: []feedXML{{ID: "600", EnabledRaw: "true"}},
	}}
	service.broadcast = newBroadcastHub()
	service.broadcast.SetHTTPSource("http://127.0.0.1:8097")
	request := sampleSIPInviteRequest()
	request.URI = "sip:600@172.16.1.31:5060"

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted extension call, response: %s", response)
	}
	defer call.close()
	if call.directFeedID != "" || call.line.Extension != "600" {
		t.Fatalf("colliding route selected feed=%q line=%#v", call.directFeedID, call.line)
	}
	if _, ok := call.ctx.Deadline(); !ok {
		t.Fatal("ordinary IVR extension lost its configured call deadline")
	}
}

func TestSIPInviteRejectsDisabledProvinceLine(t *testing.T) {
	disabled := false
	service := &Service{cfg: loadedConfig{IVR: Config{
		Extensions: []extensionConfig{
			{Extension: "haze", Province: "CA"},
			{Extension: "800", Province: "BC", Enabled: &disabled},
		},
		RTP: rtpConfig{PortMin: 0, PortMax: 0},
	}, Prompts: defaultPromptConfig()}}
	request := sampleSIPInviteRequest()
	request.URI = "sip:800@sip.teleweather.ca"

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call != nil {
		call.close()
		t.Fatal("disabled extension accepted a call")
	}
	if !strings.Contains(response, "480 Temporarily Unavailable") {
		t.Fatalf("disabled extension response = %q", response)
	}
}

func TestSIPInviteUsesConfiguredG722AndTelephoneEvents(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{RTP: rtpConfig{PortMin: 0, PortMax: 0}, Cache: cacheConfig{PhoneCodec: "g722"}}, Prompts: defaultPromptConfig()}}
	request := sampleSIPInviteRequestWithFormats("9 0 101", "a=rtpmap:9 G722/8000", "a=rtpmap:0 PCMU/8000")

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted call, response: %s", response)
	}
	call.close()
	if !strings.Contains(response, "SIP/2.0 200 OK") {
		t.Fatalf("response was not 200 OK: %s", response)
	}
	if !strings.Contains(response, "Content-Type: application/sdp") || !strings.Contains(response, "m=audio ") {
		t.Fatalf("response did not include SDP: %s", response)
	}
	if !strings.Contains(response, "a=rtpmap:9 G722/8000") || !strings.Contains(response, "telephone-event/8000") {
		t.Fatalf("response did not negotiate G.722 and telephone events: %s", response)
	}
	if call.audioCodec != sipAudioCodecG722 || call.audioPayload != sipPayloadG722 {
		t.Fatalf("call negotiated codec=%v payload=%d", call.audioCodec, call.audioPayload)
	}
}

func TestSIPInvitePrefersConfiguredPCMUOverG722(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{RTP: rtpConfig{PortMin: 0, PortMax: 0}, Cache: cacheConfig{PhoneCodec: "pcmu"}}, Prompts: defaultPromptConfig()}}
	request := sampleSIPInviteRequestWithFormats("9 0 101", "a=rtpmap:9 G722/8000", "a=rtpmap:0 PCMU/8000")

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted call, response: %s", response)
	}
	call.close()
	if !strings.Contains(response, "a=rtpmap:0 PCMU/8000") {
		t.Fatalf("response did not select PCMU: %s", response)
	}
	if call.audioCodec != sipAudioCodecPCMU || call.audioPayload != sipPayloadPCMU {
		t.Fatalf("call negotiated codec=%v payload=%d", call.audioCodec, call.audioPayload)
	}
}

func TestSIPInviteFallsBackToPCMU(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{RTP: rtpConfig{PortMin: 0, PortMax: 0}}, Prompts: defaultPromptConfig()}}
	request := sampleSIPInviteRequest()

	call, response := acceptTestSIPInvite(service, context.Background(), request, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted call, response: %s", response)
	}
	call.close()
	if !strings.Contains(response, "a=rtpmap:0 PCMU/8000") || !strings.Contains(response, "telephone-event/8000") {
		t.Fatalf("response did not negotiate PCMU and telephone events: %s", response)
	}
}

func TestSIPInviteBindsRTPWildcardWhenPublicHostConfigured(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		SIP: sipConfig{PublicHost: "203.0.113.10"},
		RTP: rtpConfig{PortMin: 0, PortMax: 0},
	}, Prompts: defaultPromptConfig()}}

	call, response := acceptTestSIPInvite(service, context.Background(), sampleSIPInviteRequest(), &net.UDPAddr{IP: net.ParseIP("198.51.100.20"), Port: 5060}, &net.UDPAddr{IP: net.IPv4zero, Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted call, response: %s", response)
	}
	defer call.close()
	if !strings.Contains(response, "c=IN IP4 203.0.113.10") {
		t.Fatalf("response did not advertise public host: %s", response)
	}
	if addr, ok := call.rtpConn.LocalAddr().(*net.UDPAddr); !ok || !addr.IP.IsUnspecified() {
		t.Fatalf("RTP should bind locally, got %#v", call.rtpConn.LocalAddr())
	}
}

func TestSIPAdvertiseHostUsesLANForPrivateCallers(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		SIP: sipConfig{PublicHost: "203.0.113.10"},
	}}}
	got := service.sipAdvertiseHost(&net.UDPAddr{IP: net.ParseIP("172.16.1.33"), Port: 5060}, &net.UDPAddr{IP: net.ParseIP("172.16.1.30"), Port: 5060})
	if got != "172.16.1.30" {
		t.Fatalf("LAN caller should receive LAN advertise host, got %q", got)
	}
	got = service.sipAdvertiseHost(&net.UDPAddr{IP: net.ParseIP("208.100.60.166"), Port: 5060}, &net.UDPAddr{IP: net.ParseIP("172.16.1.30"), Port: 5060})
	if got != "203.0.113.10" {
		t.Fatalf("public caller should receive public advertise host, got %q", got)
	}
}

func TestSIPAdvertiseHostUsesCallerRouteForWildcardListener(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		SIP: sipConfig{PublicHost: "203.0.113.10"},
	}}}
	got := service.sipAdvertiseHost(
		&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060},
		&net.UDPAddr{IP: net.IPv4zero, Port: 5060},
	)
	if got != "127.0.0.1" {
		t.Fatalf("wildcard listener should advertise the route back to a local caller, got %q", got)
	}
}

func TestSIPRegisterUsesLocalContactWithPublicHostConfigured(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		SIP: sipConfig{PublicHost: "203.0.113.10"},
	}}}
	registrar := &sipRegistrar{
		service:   service,
		localAddr: &net.UDPAddr{IP: net.ParseIP("172.16.1.30"), Port: 5060},
	}

	request := registrar.buildRegister("sip:ca.voip.ms:5060", "ca.voip.ms", "529289_TeleWeather", "529289_TeleWeather", "call-id", "tag", 1, 300, "")

	if !strings.Contains(request, "Via: SIP/2.0/UDP 172.16.1.30:5060") {
		t.Fatalf("REGISTER should use local Via host, got:\n%s", request)
	}
	if !strings.Contains(request, "Contact: <sip:529289_TeleWeather@172.16.1.30:5060>") {
		t.Fatalf("REGISTER should use local Contact host, got:\n%s", request)
	}
	if strings.Contains(request, "203.0.113.10") {
		t.Fatalf("REGISTER leaked public SDP host:\n%s", request)
	}
}

func TestSIPRegisterCanUsePublicContactForProviderCompatibility(t *testing.T) {
	supportedPath := false
	service := &Service{cfg: loadedConfig{IVR: Config{
		SIP: sipConfig{
			PublicHost: "203.0.113.10",
			Registration: sipRegistrationConfig{
				ContactHost:   "public",
				ViaHost:       "public",
				UserAgent:     "MicroSIP/3.21.3",
				SupportedPath: &supportedPath,
			},
		},
	}}}
	registrar := &sipRegistrar{
		service:   service,
		localAddr: &net.UDPAddr{IP: net.ParseIP("172.16.1.30"), Port: 5060},
	}

	request := registrar.buildRegister("sip:vancouver1.voip.ms:5060", "vancouver1.voip.ms", "529289_TeleWeather", "529289_TeleWeather", "call-id", "tag", 1, 120, "")

	if !strings.Contains(request, "REGISTER sip:vancouver1.voip.ms:5060 SIP/2.0") {
		t.Fatalf("REGISTER URI was not preserved:\n%s", request)
	}
	if !strings.Contains(request, "Via: SIP/2.0/UDP 203.0.113.10:5060") {
		t.Fatalf("REGISTER should use public Via host, got:\n%s", request)
	}
	if !strings.Contains(request, "Contact: <sip:529289_TeleWeather@203.0.113.10:5060>") {
		t.Fatalf("REGISTER should use public Contact host, got:\n%s", request)
	}
	if strings.Contains(strings.ToLower(request), "supported: path") {
		t.Fatalf("REGISTER should be able to suppress Supported: path:\n%s", request)
	}
	if !strings.Contains(request, "User-Agent: MicroSIP/3.21.3") {
		t.Fatalf("REGISTER should use configured User-Agent:\n%s", request)
	}
}

func TestSIPRegisterURIAddsSIPScheme(t *testing.T) {
	if got := sipRegisterURI("vancouver1.voip.ms:5060"); got != "sip:vancouver1.voip.ms:5060" {
		t.Fatalf("uri = %q", got)
	}
	if got := sipRegisterURI("sip:vancouver1.voip.ms:5060"); got != "sip:vancouver1.voip.ms:5060" {
		t.Fatalf("uri = %q", got)
	}
}

func TestSIPInviteSetsCallDeadline(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{
		MaxCallSeconds: 7,
		RTP:            rtpConfig{PortMin: 0, PortMax: 0},
	}}}
	call, response := acceptTestSIPInvite(service, context.Background(), sampleSIPInviteRequest(), &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060})
	if call == nil {
		t.Fatalf("expected accepted call, response: %s", response)
	}
	defer call.close()
	deadline, ok := call.ctx.Deadline()
	if !ok {
		t.Fatalf("call context did not receive a deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > 8*time.Second {
		t.Fatalf("unexpected call deadline remaining: %s", remaining)
	}
}

func TestSIPAdvertiseHostIPHelpers(t *testing.T) {
	for _, value := range []string{"10.0.0.5", "172.16.1.20", "172.31.255.1", "192.168.1.50"} {
		if !isPrivateIPv4(net.ParseIP(value)) {
			t.Fatalf("%s should be private IPv4", value)
		}
	}
	for _, value := range []string{"127.0.0.1", "169.254.1.2", "172.32.0.1", "8.8.8.8", "::1"} {
		if isPrivateIPv4(net.ParseIP(value)) {
			t.Fatalf("%s should not be private IPv4", value)
		}
	}
	if usableIPv4(net.ParseIP("192.168.1.50")).String() != "192.168.1.50" {
		t.Fatal("usable IPv4 rejected LAN address")
	}
	if usableIPv4(net.ParseIP("::1")) != nil {
		t.Fatal("usable IPv4 accepted IPv6 loopback")
	}
}

func TestSDPOfferRejectsMissingSupportedCodec(t *testing.T) {
	_, err := parseSDPOffer(strings.Join([]string{
		"v=0",
		"o=caller 0 0 IN IP4 127.0.0.1",
		"s=call",
		"c=IN IP4 127.0.0.1",
		"t=0 0",
		"m=audio 40000 RTP/AVP 8 101",
		"a=rtpmap:8 PCMA/8000",
		"a=rtpmap:101 telephone-event/8000",
		"",
	}, "\r\n"), &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062})
	if err == nil || !strings.Contains(err.Error(), "G722") || !strings.Contains(err.Error(), "PCMU") {
		t.Fatalf("expected supported-codec rejection, got %v", err)
	}
}

func TestSDPOfferRejectsMalformedAudioLineWithoutPanic(t *testing.T) {
	_, err := parseSDPOffer(strings.Join([]string{
		"v=0",
		"o=caller 0 0 IN IP4 127.0.0.1",
		"s=call",
		"c=IN IP4 127.0.0.1",
		"t=0 0",
		"m=audio 40000",
		"",
	}, "\r\n"), &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5062})
	if err == nil {
		t.Fatalf("expected malformed SDP to be rejected")
	}
}

func TestSIPSourceAllowedSupportsIPAndCIDR(t *testing.T) {
	service := &Service{}
	if !service.sipSourceAllowed(&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 5060}) {
		t.Fatalf("empty allowlist should allow any source")
	}
	service.cfg.IVR.SIP.AllowedSources = []string{"192.0.2.10", "10.0.0.0/8"}
	if !service.sipSourceAllowed(&net.UDPAddr{IP: net.ParseIP("192.0.2.10"), Port: 5060}) {
		t.Fatalf("exact source IP was not allowed")
	}
	if !service.sipSourceAllowed(&net.UDPAddr{IP: net.ParseIP("10.42.0.5"), Port: 5060}) {
		t.Fatalf("CIDR source IP was not allowed")
	}
	if service.sipSourceAllowed(&net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 5060}) {
		t.Fatalf("source outside allowlist was allowed")
	}
}

func TestSIPTrustedSourceRefreshUsesRegistrationServerAndRetainsLastGoodSet(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{SIP: sipConfig{Registration: sipRegistrationConfig{
		Server: "sip:registrar.example.test:5060;transport=udp",
	}}}}}
	state := &sipServerState{
		trustedSourceLookup: func(_ context.Context, host string) ([]net.IPAddr, error) {
			if host != "registrar.example.test" {
				t.Fatalf("lookup host = %q, want registrar.example.test", host)
			}
			return []net.IPAddr{
				{IP: net.ParseIP("192.0.2.44")},
				{IP: net.ParseIP("2001:db8::44")},
				{},
			}, nil
		},
	}
	state.trustedSources.Store(map[string]struct{}{"192.0.2.1": {}})

	service.refreshSIPTrustedSources(context.Background(), state)
	trusted, ok := state.trustedSources.Load().(map[string]struct{})
	if !ok || len(trusted) != 2 {
		t.Fatalf("trusted sources = %#v", state.trustedSources.Load())
	}
	for _, source := range []string{"192.0.2.44", "2001:db8::44"} {
		if _, exists := trusted[source]; !exists {
			t.Fatalf("trusted sources missing %q: %#v", source, trusted)
		}
	}

	state.trustedSourceLookup = func(context.Context, string) ([]net.IPAddr, error) {
		return nil, fmt.Errorf("temporary DNS failure")
	}
	service.refreshSIPTrustedSources(context.Background(), state)
	trusted, ok = state.trustedSources.Load().(map[string]struct{})
	if !ok || len(trusted) != 2 {
		t.Fatalf("failed refresh replaced the last good source set: %#v", state.trustedSources.Load())
	}
}

func TestSIPTrustedSourceRefreshAcceptsLiteralServerWithoutDNS(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{SIP: sipConfig{Registration: sipRegistrationConfig{
		Server: "[2001:db8::55]:5060",
	}}}}}
	state := &sipServerState{
		trustedSourceLookup: func(context.Context, string) ([]net.IPAddr, error) {
			t.Fatal("literal registration server should not use DNS")
			return nil, nil
		},
	}
	state.trustedSources.Store(map[string]struct{}{})

	service.refreshSIPTrustedSources(context.Background(), state)
	trusted, ok := state.trustedSources.Load().(map[string]struct{})
	if !ok || len(trusted) != 1 {
		t.Fatalf("trusted sources = %#v", state.trustedSources.Load())
	}
	if _, exists := trusted["2001:db8::55"]; !exists {
		t.Fatalf("literal IPv6 source was not retained: %#v", trusted)
	}
}

func TestSIPSourceAllowedWithTrustedSources(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{SIP: sipConfig{Registration: sipRegistrationConfig{
		Enabled: true, RestrictSourcesToServer: true,
	}}}}}
	state := &sipServerState{}
	state.trustedSources.Store(map[string]struct{}{"192.0.2.44": {}})
	trustedRemote := &net.UDPAddr{IP: net.ParseIP("192.0.2.44"), Port: 5060}
	configuredRemote := &net.UDPAddr{IP: net.ParseIP("198.51.100.21"), Port: 5060}
	otherRemote := &net.UDPAddr{IP: net.ParseIP("203.0.113.25"), Port: 5060}

	if !service.sipSourceAllowedWithTrustedSources(trustedRemote, state) {
		t.Fatal("resolved registration-server source was rejected")
	}
	if service.sipSourceAllowedWithTrustedSources(otherRemote, state) {
		t.Fatal("source outside the trusted registration-server set was allowed")
	}

	service.cfg.IVR.SIP.AllowedSources = []string{"198.51.100.21"}
	if !service.sipSourceAllowedWithTrustedSources(configuredRemote, state) {
		t.Fatal("explicit configured source was not preserved")
	}
	if !service.sipSourceAllowedWithTrustedSources(trustedRemote, state) {
		t.Fatal("trusted registration-server source was not additive to configured sources")
	}

	state.trustedSources.Store(map[string]struct{}{})
	service.cfg.IVR.SIP.AllowedSources = nil
	if service.sipSourceAllowedWithTrustedSources(otherRemote, state) {
		t.Fatal("empty trusted source set did not fail closed")
	}
	service.cfg.IVR.SIP.Registration.RestrictSourcesToServer = false
	if !service.sipSourceAllowedWithTrustedSources(otherRemote, state) {
		t.Fatal("default source behavior changed when restriction is disabled")
	}
}

func TestRunSIPTrustedSourceRefreshStopsWithContext(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{SIP: sipConfig{Registration: sipRegistrationConfig{
		Server: "registrar.example.test",
	}}}}}
	refreshed := make(chan struct{}, 1)
	state := &sipServerState{
		trustedSourceRefreshEvery: 5 * time.Millisecond,
		trustedSourceLookup: func(context.Context, string) ([]net.IPAddr, error) {
			select {
			case refreshed <- struct{}{}:
			default:
			}
			return []net.IPAddr{{IP: net.ParseIP("192.0.2.55")}}, nil
		},
	}
	state.trustedSources.Store(map[string]struct{}{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		service.runSIPTrustedSourceRefresh(ctx, state)
		close(done)
	}()
	select {
	case <-refreshed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("trusted source refresh did not run")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("trusted source refresher did not stop after cancellation")
	}
}

func TestSIPCanAcceptCallHonorsConfiguredLimit(t *testing.T) {
	service := &Service{cfg: loadedConfig{IVR: Config{MaxConcurrentCalls: 2}}}
	if !service.sipCanAcceptCall(1) {
		t.Fatalf("call below limit should be accepted")
	}
	if service.sipCanAcceptCall(2) {
		t.Fatalf("call at limit should be rejected")
	}
}

func TestSIPDigestAuthChallengeAndVerification(t *testing.T) {
	t.Setenv("HAZE_IVR_TEST_PASSWORD", "secret")
	service := &Service{}
	service.cfg.IVR.SIP.Auth.Enabled = true
	service.cfg.IVR.SIP.Auth.Username = "haze"
	service.cfg.IVR.SIP.Auth.PasswordEnv = "HAZE_IVR_TEST_PASSWORD"
	request := sampleSIPInviteRequest()
	challenges := map[string]time.Time{}

	ok, status, extra := service.sipAuthorizeInvite(request, challenges)
	if ok || status != "401 Unauthorized" || !strings.Contains(extra, "WWW-Authenticate: Digest") {
		t.Fatalf("expected digest challenge, ok=%v status=%q extra=%q", ok, status, extra)
	}
	if len(challenges) != 1 {
		t.Fatalf("expected one nonce challenge, got %d", len(challenges))
	}
	nonce := ""
	for key := range challenges {
		nonce = key
	}
	request.Headers["authorization"] = digestAuthHeader(request, "haze", sipAuthRealm(), "secret", nonce)
	ok, status, extra = service.sipAuthorizeInvite(request, challenges)
	if !ok {
		t.Fatalf("expected digest auth to pass, status=%q extra=%q", status, extra)
	}
}

func TestSIPPromptPlaybackBargesInOnDigit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "long.pcmu")
	raw := make([]byte, sipPacketSamples*80)
	for index := range raw {
		raw[index] = 0xFF
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	call := &sipCall{
		ctx:    ctx,
		digits: make(chan string, 1),
	}
	go func() {
		time.Sleep(40 * time.Millisecond)
		call.pushDigit("2")
	}()

	startedAt := time.Now()
	digit, ok := call.playPCMUFile(path, digitInterruptAny)
	elapsed := time.Since(startedAt)
	if !ok || digit != "2" {
		t.Fatalf("digit=%q ok=%v", digit, ok)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("barge-in took too long: %s", elapsed)
	}
}

func TestSIPProductPlaybackOnlyBargesInOnPound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "long.pcmu")
	raw := make([]byte, sipPacketSamples*80)
	for index := range raw {
		raw[index] = 0xFF
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	call := &sipCall{
		ctx:    ctx,
		digits: make(chan string, 4),
	}
	go func() {
		time.Sleep(40 * time.Millisecond)
		call.pushDigit("2")
		time.Sleep(40 * time.Millisecond)
		call.pushDigit("#")
	}()

	startedAt := time.Now()
	digit, ok := call.playPCMUFile(path, digitInterruptPound)
	elapsed := time.Since(startedAt)
	if !ok || digit != "#" {
		t.Fatalf("digit=%q ok=%v", digit, ok)
	}
	if elapsed > 400*time.Millisecond {
		t.Fatalf("product pound interrupt took too long: %s", elapsed)
	}
}

func TestCollectDigitsUsesDigitThatInterruptedPrompt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call := &sipCall{
		ctx:    ctx,
		digits: make(chan string, 8),
		service: &Service{cfg: loadedConfig{IVR: Config{
			DigitTimeoutSeconds: 1,
		}}},
	}
	call.pushDigit("6")
	call.pushDigit("0")
	call.pushDigit("4")
	call.pushDigit("0")
	call.pushDigit("#")

	code, ok := call.collectDigits(time.Second, "0")
	if !ok || code != "06040" {
		t.Fatalf("code=%q ok=%v", code, ok)
	}
}

func TestCollectLocationInputAutoSubmitsValidCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := loadedConfig{
		IVR:   Config{DigitTimeoutSeconds: 2, DefaultLanguage: "en-CA"},
		Feeds: []feedXML{testFeedWithLanguages("sk-0001", "en-CA")},
	}
	call := &sipCall{
		ctx:    ctx,
		digits: make(chan string, 8),
		service: &Service{
			cfg:      cfg,
			resolver: resolverWithHelloWeather(cfg, locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"}),
		},
	}
	for _, digit := range []string{"0", "6", "0", "4", "0"} {
		call.pushDigit(digit)
	}

	startedAt := time.Now()
	code, geophysical, ok := call.collectLocationInput(2*time.Second, "")
	elapsed := time.Since(startedAt)
	if !ok || geophysical || code != "06040" {
		t.Fatalf("code=%q geophysical=%v ok=%v", code, geophysical, ok)
	}
	if elapsed < 550*time.Millisecond || elapsed > 1200*time.Millisecond {
		t.Fatalf("auto-submit delay = %s, want about 600ms", elapsed)
	}
}

func TestCollectLocationInputStarSelectsGeophysical(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call := &sipCall{
		ctx:     ctx,
		digits:  make(chan string, 1),
		service: &Service{cfg: loadedConfig{IVR: Config{DigitTimeoutSeconds: 1}}},
	}
	code, geophysical, ok := call.collectLocationInput(time.Second, "*")
	if !ok || !geophysical || code != "" {
		t.Fatalf("code=%q geophysical=%v ok=%v", code, geophysical, ok)
	}
}

func TestRTPDTMFDigitDecodesEndEvent(t *testing.T) {
	packet := make([]byte, 16)
	packet[0] = 0x80
	packet[1] = sipDefaultDTMFPayload
	binary.BigEndian.PutUint32(packet[4:8], 1234)
	packet[12] = 1
	packet[13] = 0x80
	packet[14] = 0
	packet[15] = 160

	digit, key := rtpDTMFDigit(packet, sipDefaultDTMFPayload)
	if digit != "1" || key == "" {
		t.Fatalf("digit=%q key=%q", digit, key)
	}
}

func TestRTPDTMFDigitDecodesInitialEventWithoutWaitingForEnd(t *testing.T) {
	packet := make([]byte, 16)
	packet[0] = 0x80
	packet[1] = sipDefaultDTMFPayload
	binary.BigEndian.PutUint32(packet[4:8], 5678)
	packet[12] = 2
	packet[13] = 0x00
	packet[14] = 0
	packet[15] = 80

	digit, key := rtpDTMFDigit(packet, sipDefaultDTMFPayload)
	if digit != "2" || key != "2:5678" {
		t.Fatalf("digit=%q key=%q", digit, key)
	}
}

func TestNonInterruptibleAudioPreservesQueuedDigit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alert.pcmu")
	if err := os.WriteFile(path, make([]byte, sipPacketSamples*2), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call := &sipCall{
		ctx:     ctx,
		digits:  make(chan string, 2),
		service: &Service{cfg: loadedConfig{IVR: Config{DigitTimeoutSeconds: 1}}},
	}
	call.digits <- "2"
	if digit, interrupted := call.playAudioFile(path, digitInterruptNone); interrupted || digit != "" {
		t.Fatalf("non-interruptible playback returned digit=%q interrupted=%v", digit, interrupted)
	}
	if digit, ok := call.waitDigit(50 * time.Millisecond); !ok || digit != "2" {
		t.Fatalf("queued digit after playback = %q, ok=%v", digit, ok)
	}
}

func TestAlertAudioCanBeInterruptedWithPound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alert.pcmu")
	if err := os.WriteFile(path, make([]byte, sipPacketSamples*2), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call := &sipCall{
		ctx:     ctx,
		digits:  make(chan string, 1),
		service: &Service{cfg: loadedConfig{IVR: Config{DigitTimeoutSeconds: 1}}},
	}
	call.digits <- "#"
	if digit, interrupted := call.playAudioFile(path, digitInterruptPound); !interrupted || digit != "#" {
		t.Fatalf("pound interrupt returned digit=%q interrupted=%v", digit, interrupted)
	}
}

func TestSIPCallerProvinceHint(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		enabled bool
		want    string
	}{
		{
			name: "asserted identity preferred",
			headers: map[string]string{
				"p-asserted-identity": "<sip:+13065551234@carrier.example>",
				"from":                "<sip:+14165551234@carrier.example>",
			},
			enabled: true,
			want:    "SK",
		},
		{name: "from fallback", headers: map[string]string{"from": "<tel:+14165551234>"}, enabled: true, want: "ON"},
		{name: "shared maritime code omitted", headers: map[string]string{"from": "<sip:+19025551234@carrier.example>"}, enabled: true},
		{name: "anonymous omitted", headers: map[string]string{"from": "<sip:anonymous@carrier.example>"}, enabled: true},
		{name: "feature disabled", headers: map[string]string{"from": "<sip:+13065551234@carrier.example>"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &Service{cfg: loadedConfig{IVR: Config{Search: searchConfig{CallerHintEnabled: test.enabled}}}}
			if got := service.sipCallerProvinceHint(sipRequest{Headers: test.headers}); got != test.want {
				t.Fatalf("sipCallerProvinceHint() = %q, want %q", got, test.want)
			}
		})
	}
}

func sampleSIPInviteRequest() sipRequest {
	return sampleSIPInviteRequestWithFormats("0 101", "a=rtpmap:0 PCMU/8000")
}

func sampleSIPInviteRequestWithFormats(formats string, rtpmapLines ...string) sipRequest {
	lines := []string{
		"INVITE sip:haze@127.0.0.1 SIP/2.0",
		"Via: SIP/2.0/UDP 127.0.0.1:5062;branch=z9hG4bK-test",
		"From: <sip:caller@127.0.0.1>;tag=abc",
		"To: <sip:haze@127.0.0.1>",
		"Call-ID: call-1",
		"CSeq: 1 INVITE",
		"Contact: <sip:caller@127.0.0.1:5062>",
		"Content-Type: application/sdp",
		"Content-Length: 161",
		"",
		"v=0",
		"o=caller 0 0 IN IP4 127.0.0.1",
		"s=call",
		"c=IN IP4 127.0.0.1",
		"t=0 0",
		"m=audio 40000 RTP/AVP " + formats,
	}
	lines = append(lines, rtpmapLines...)
	lines = append(lines,
		"a=rtpmap:101 telephone-event/8000",
		"",
	)
	return parseSIPRequest(strings.Join(lines, "\r\n"))
}

func digestAuthHeader(request sipRequest, username string, realm string, password string, nonce string) string {
	cnonce := "abcdef"
	nc := "00000001"
	qop := "auth"
	ha1 := sipMD5Hex(username + ":" + realm + ":" + password)
	ha2 := sipMD5Hex(request.Method + ":" + request.URI)
	response := sipMD5Hex(strings.Join([]string{ha1, nonce, nc, cnonce, qop, ha2}, ":"))
	return fmt.Sprintf(`Digest username="%s", realm="%s", nonce="%s", uri="%s", response="%s", algorithm=MD5, qop=%s, nc=%s, cnonce="%s"`, username, realm, nonce, request.URI, response, qop, nc, cnonce)
}
