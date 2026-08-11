package ivr

import (
	"context"
	"crypto/md5" // #nosec G501 -- SIP Digest authentication is defined with MD5.
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/pion/rtp"
)

const (
	sipPayloadPCMU                = 0
	sipPayloadG722                = 9
	sipDefaultDTMFPayload         = 101
	sipTelephoneEventClock        = 8000
	sipPCMUSampleRate             = 8000
	sipG722SampleRate             = 16000
	sipPacketSamples              = 160
	sipG722FrameSamples           = sipG722SampleRate / 50
	sipTrustedSourceLookupTimeout = 3 * time.Second
	sipTrustedSourceRefreshEvery  = 5 * time.Minute
)

type sipAudioCodec int

const (
	sipAudioCodecPCMU sipAudioCodec = iota
	sipAudioCodecG722
)

type digitInterruptMode int

const (
	digitInterruptNone digitInterruptMode = iota
	digitInterruptAny
	digitInterruptPound
)

type sipRequest struct {
	Method  string
	URI     string
	Headers map[string]string
	Body    string
}

type sipResponse struct {
	StatusCode int
	Reason     string
	Headers    map[string]string
	Body       string
}

type sipRegistrar struct {
	service   *Service
	conn      *net.UDPConn
	localAddr net.Addr
	responses chan sipResponse
}

type sipServerState struct {
	calls                     map[string]*sipCall
	inviteReplies             map[string]string
	challenges                map[string]time.Time
	trustedSources            atomic.Value // map[string]struct{}, refreshed from the registration server DNS name.
	trustedSourceLookup       func(context.Context, string) ([]net.IPAddr, error)
	trustedSourceRefreshEvery time.Duration
	callsMu                   sync.Mutex
	challengesMu              sync.Mutex
}

type sipMediaOffer struct {
	Host         string
	Port         int
	DTMFPayload  int
	AudioCodec   sipAudioCodec
	AudioPayload int
	PCMUPayload  int
	G722Payload  int
	HasPCMU      bool
	HasG722      bool
}

type sipCall struct {
	service        *Service
	line           extensionConfig
	directFeedID   string
	callerProvince string
	ctx            context.Context
	cancel         context.CancelFunc
	callID         string
	localTag       string
	sipConn        *net.UDPConn
	sipRemote      *net.UDPAddr
	sipHeaders     map[string]string
	sipRequestURI  string
	rtpConn        *net.UDPConn
	remoteRTP      *net.UDPAddr
	audioCodec     sipAudioCodec
	audioPayload   int
	dtmfPayload    int
	digits         chan string
	audioFrames    chan pcmFrame
	captureActive  atomic.Bool
	done           chan struct{}
	sendMu         sync.Mutex
	byeOnce        sync.Once
	seq            uint16
	timestamp      uint32
	ssrc           uint32
	sentRTPLog     bool
	recvRTPLog     bool
}

type sipDirectFeedTarget struct {
	ID      string
	Matched bool
	Enabled bool
}

type sipLineSelection struct {
	Line          extensionConfig
	Matched       bool
	CalledAddress string
}

func (s *Service) runSIP(ctx context.Context) error {
	bindings := s.cfg.IVR.SIP.listenBindings()
	if len(bindings) == 0 {
		return fmt.Errorf("no SIP listen bindings configured")
	}
	state := &sipServerState{calls: map[string]*sipCall{}, inviteReplies: map[string]string{}, challenges: map[string]time.Time{}}
	state.trustedSources.Store(map[string]struct{}{})
	if s.cfg.IVR.SIP.Registration.Enabled && s.cfg.IVR.SIP.Registration.RestrictSourcesToServer {
		s.refreshSIPTrustedSources(ctx, state)
		go s.runSIPTrustedSourceRefresh(ctx, state)
	}
	errCh := make(chan error, len(bindings))
	conns := make([]*net.UDPConn, 0, len(bindings))
	for index, binding := range bindings {
		addr, err := net.ResolveUDPAddr("udp", binding.Addr)
		if err != nil {
			return err
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			for _, opened := range conns {
				_ = opened.Close()
			}
			return err
		}
		conns = append(conns, conn)
		domainSuffix := ""
		if binding.Domain != "" {
			domainSuffix = " domain=" + binding.Domain
		}
		log.Printf("IVR SIP listening on %s%s", conn.LocalAddr(), domainSuffix)
		var registrar *sipRegistrar
		if index == 0 && s.cfg.IVR.SIP.Registration.Enabled {
			registrar = newSIPRegistrar(s, conn, conn.LocalAddr())
			s.sipDebugf("registrar enabled listen=%s server=%s domain=%s username=%s contact_user=%s public_host=%s",
				conn.LocalAddr(),
				s.cfg.IVR.SIP.Registration.Server,
				s.cfg.IVR.SIP.Registration.Domain,
				s.cfg.IVR.SIP.Registration.Username,
				s.cfg.IVR.SIP.Registration.ContactUser,
				s.cfg.IVR.SIP.PublicHost,
			)
			go registrar.run(ctx)
		}
		go func(listener sipListenBinding, udpConn *net.UDPConn, sipRegistrar *sipRegistrar) {
			errCh <- s.runSIPListener(ctx, listener, udpConn, state, sipRegistrar)
		}(binding, conn, registrar)
	}
	go func() {
		<-ctx.Done()
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		for _, conn := range conns {
			_ = conn.Close()
		}
		return err
	}
}

// refreshSIPTrustedSources resolves the configured registration server and
// atomically replaces the dynamic source set only after a complete, nonempty
// result. A failed lookup intentionally retains the previous set, and an
// initially empty set remains fail-closed when source restriction is enabled.
func (s *Service) refreshSIPTrustedSources(parent context.Context, state *sipServerState) {
	if s == nil || state == nil {
		return
	}
	host := strings.Trim(strings.TrimSpace(sipHostOnly(s.cfg.IVR.SIP.Registration.Server)), "[]")
	if host == "" {
		log.Printf("IVR SIP trusted source refresh skipped: registration server is not configured")
		return
	}

	trusted := make(map[string]struct{})
	if ip := net.ParseIP(host); ip != nil {
		if key := sipTrustedSourceKey(ip); key != "" {
			trusted[key] = struct{}{}
		}
	} else {
		if parent == nil {
			parent = context.Background()
		}
		lookupContext, cancel := context.WithTimeout(parent, sipTrustedSourceLookupTimeout)
		defer cancel()
		lookup := state.trustedSourceLookup
		if lookup == nil {
			lookup = net.DefaultResolver.LookupIPAddr
		}
		addresses, err := lookup(lookupContext, host)
		if err != nil {
			log.Printf("IVR SIP trusted source refresh failed for registration server %q: %v", host, err)
			return
		}
		for _, address := range addresses {
			if key := sipTrustedSourceKey(address.IP); key != "" {
				trusted[key] = struct{}{}
			}
		}
	}
	if len(trusted) == 0 {
		log.Printf("IVR SIP trusted source refresh returned no usable addresses for registration server %q", host)
		return
	}
	state.trustedSources.Store(trusted)
	s.sipDebugf("trusted SIP source refresh server=%s addresses=%d", host, len(trusted))
}

// runSIPTrustedSourceRefresh keeps the registration-server source set current
// without blocking SIP listeners. The caller's context owns its lifetime.
func (s *Service) runSIPTrustedSourceRefresh(ctx context.Context, state *sipServerState) {
	if s == nil || state == nil || ctx == nil {
		return
	}
	interval := state.trustedSourceRefreshEvery
	if interval <= 0 {
		interval = sipTrustedSourceRefreshEvery
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshSIPTrustedSources(ctx, state)
		}
	}
}

func sipTrustedSourceKey(ip net.IP) string {
	if ip == nil || ip.IsUnspecified() {
		return ""
	}
	return ip.String()
}

func (s *Service) runSIPListener(ctx context.Context, binding sipListenBinding, conn *net.UDPConn, state *sipServerState, registrar *sipRegistrar) error {
	defer conn.Close()
	buffer := make([]byte, 16384)
	for {
		n, remote, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		s.metrics.SIPMessages.Add(1)
		if response, ok := parseSIPResponse(string(buffer[:n])); ok {
			if registrar != nil {
				registrar.handleResponse(response)
			}
			continue
		}
		request := parseSIPRequest(string(buffer[:n]))
		if request.Method == "" {
			continue
		}
		callID := sipHeader(request.Headers, "call-id")
		if !s.sipSourceAllowedWithTrustedSources(remote, state) {
			_, _ = conn.WriteToUDP([]byte(sipReply("403 Forbidden", request.Headers, "Warning: 399 haze \"SIP source is not allowed\"\r\n")), remote)
			continue
		}
		if !s.sipDomainAllowed(request, binding.Domain) {
			_, _ = conn.WriteToUDP([]byte(sipReply("404 Not Found", request.Headers, "Warning: 399 haze \"SIP domain is not served here\"\r\n")), remote)
			continue
		}
		switch request.Method {
		case "OPTIONS":
			_, _ = conn.WriteToUDP([]byte(sipReply("200 OK", request.Headers, "Allow: INVITE, ACK, BYE, CANCEL, OPTIONS, INFO\r\nAccept: application/sdp\r\n")), remote)
		case "INVITE":
			if callID == "" {
				_, _ = conn.WriteToUDP([]byte(sipReply("400 Bad Request", request.Headers, "Warning: 399 haze \"missing Call-ID\"\r\n")), remote)
				continue
			}
			state.challengesMu.Lock()
			ok, status, extra := s.sipAuthorizeInvite(request, state.challenges)
			state.challengesMu.Unlock()
			if !ok {
				_, _ = conn.WriteToUDP([]byte(sipReply(status, request.Headers, extra)), remote)
				continue
			}
			if _, err := conn.WriteToUDP([]byte(sipReply("100 Trying", request.Headers, "")), remote); err != nil {
				log.Printf("IVR SIP failed to acknowledge INVITE %s from %s: %v", callID, remote, err)
			}
			state.callsMu.Lock()
			if reply := state.inviteReplies[callID]; reply != "" {
				state.callsMu.Unlock()
				if _, err := conn.WriteToUDP([]byte(reply), remote); err != nil {
					log.Printf("IVR SIP failed to resend INVITE response for %s to %s: %v", callID, remote, err)
				}
				continue
			}
			activeCalls := len(state.calls)
			if !s.sipCanAcceptCall(activeCalls) {
				state.callsMu.Unlock()
				_, _ = conn.WriteToUDP([]byte(sipReply("486 Busy Here", request.Headers, "Warning: 399 haze \"too many active IVR calls\"\r\n")), remote)
				continue
			}
			state.callsMu.Unlock()
			call, response := s.acceptSIPInvite(ctx, request, remote, conn, conn.LocalAddr())
			if _, err := conn.WriteToUDP([]byte(response), remote); err != nil {
				log.Printf("IVR SIP failed to respond to INVITE %s from %s: %v", callID, remote, err)
			}
			if call == nil {
				continue
			}
			state.callsMu.Lock()
			state.calls[callID] = call
			state.inviteReplies[callID] = response
			state.callsMu.Unlock()
			go func(id string, c *sipCall) {
				c.run()
				state.callsMu.Lock()
				if state.calls[id] == c {
					delete(state.calls, id)
					delete(state.inviteReplies, id)
				}
				state.callsMu.Unlock()
			}(callID, call)
		case "ACK":
			s.sipDebugf("received ACK call=%s from=%s", callID, remote)
		case "BYE", "CANCEL":
			state.callsMu.Lock()
			call := state.calls[callID]
			delete(state.calls, callID)
			delete(state.inviteReplies, callID)
			state.callsMu.Unlock()
			if call != nil {
				call.close()
			}
			_, _ = conn.WriteToUDP([]byte(sipReply("200 OK", request.Headers, "")), remote)
		case "INFO":
			if digit := sipInfoDigit(request.Body); digit != "" {
				state.callsMu.Lock()
				call := state.calls[callID]
				state.callsMu.Unlock()
				if call != nil {
					call.pushDigit(digit)
				}
			}
			_, _ = conn.WriteToUDP([]byte(sipReply("200 OK", request.Headers, "")), remote)
		default:
			_, _ = conn.WriteToUDP([]byte(sipReply("501 Not Implemented", request.Headers, "")), remote)
		}
	}
}

func (s *Service) acceptSIPInvite(ctx context.Context, request sipRequest, remote *net.UDPAddr, conn *net.UDPConn, local net.Addr) (*sipCall, string) {
	selection := s.sipRequestLineSelection(request)
	line, matched := selection.Line, selection.Matched
	directFeed := s.sipRequestDirectFeed(request)
	if _, extensionMatched := s.cfg.IVR.extensionLine(sipURIUser(request.URI)); extensionMatched {
		directFeed = sipDirectFeedTarget{}
	}
	if directFeed.Matched && !directFeed.Enabled {
		return nil, sipReply("480 Temporarily Unavailable", request.Headers, "Warning: 399 haze \"live feed is disabled\"\r\n")
	}
	if directFeed.Matched && !s.broadcastAvailable(directFeed.ID) {
		return nil, sipReply("480 Temporarily Unavailable", request.Headers, "Warning: 399 haze \"live feed is unavailable\"\r\n")
	}
	if !directFeed.Matched && !line.enabled() {
		status := "404 Not Found"
		if matched {
			status = "480 Temporarily Unavailable"
		}
		return nil, sipReply(status, request.Headers, "Warning: 399 haze \"IVR line is disabled\"\r\n")
	}
	offer, err := parseSDPOffer(request.Body, remote)
	if err != nil {
		log.Printf("IVR SIP rejected INVITE from %s: %v", remote, err)
		return nil, sipReply("488 Not Acceptable Here", request.Headers, "Warning: 399 haze \""+escapeSIPWarning(err.Error())+"\"\r\n")
	}
	localHost := s.sipAdvertiseHost(remote, local)
	rtpBindHost := s.sipRTPBindHost(local)
	rtpConn, rtpPort, err := listenRTPPort(rtpBindHost, s.cfg.IVR.RTP.PortMin, s.cfg.IVR.RTP.PortMax)
	if err != nil {
		s.sipDebugf("reject INVITE from=%s reason=no RTP ports: %v", remote, err)
		return nil, sipReply("503 Service Unavailable", request.Headers, "Warning: 399 haze \"no RTP ports available\"\r\n")
	}
	var callCtx context.Context
	var cancel context.CancelFunc
	if directFeed.Matched || s.cfg.IVR.MaxCallSeconds <= 0 {
		callCtx, cancel = context.WithCancel(ctx)
	} else {
		callCtx, cancel = context.WithTimeout(ctx, time.Duration(s.cfg.IVR.MaxCallSeconds)*time.Second)
	}
	audioCodec, audioPayload := s.selectSIPAudioCodec(offer)
	call := &sipCall{
		service:        s,
		line:           line,
		directFeedID:   directFeed.ID,
		callerProvince: s.sipCallerProvinceHint(request),
		ctx:            callCtx,
		cancel:         cancel,
		callID:         sipHeader(request.Headers, "call-id"),
		localTag:       randomHex(6),
		sipConn:        conn,
		sipRemote:      cloneUDPAddr(remote),
		sipHeaders:     cloneSIPHeaders(request.Headers),
		sipRequestURI:  firstNonBlank(sipContactURI(sipHeader(request.Headers, "contact")), request.URI),
		rtpConn:        rtpConn,
		remoteRTP:      &net.UDPAddr{IP: net.ParseIP(offer.Host), Port: offer.Port},
		audioCodec:     audioCodec,
		audioPayload:   audioPayload,
		dtmfPayload:    offer.DTMFPayload,
		digits:         make(chan string, 16),
		audioFrames:    make(chan pcmFrame, 64),
		done:           make(chan struct{}),
		seq:            uint16(time.Now().UnixNano()),
		timestamp:      uint32(time.Now().UnixNano()),
		ssrc:           randomUint32(),
	}
	if call.dtmfPayload <= 0 {
		call.dtmfPayload = sipDefaultDTMFPayload
	}
	if call.audioPayload < 0 {
		call.audioPayload = int(call.audioCodec.defaultPayload())
	}
	sdp := sipAnswerSDP(localHost, rtpPort, call.audioCodec, call.audioPayload, call.dtmfPayload)
	log.Printf("IVR SIP accepted call %s from %s remote_rtp=%s:%d advertised_rtp=%s:%d offered_pcmu=%v/%d offered_g722=%v/%d selected=%s/%d dtmf=%d",
		call.callID, remote, offer.Host, offer.Port, localHost, rtpPort, offer.HasPCMU, offer.PCMUPayload, offer.HasG722, offer.G722Payload, call.audioCodec.name(), call.audioPayload, call.dtmfPayload)
	s.sipDebugf("accepted call=%s from=%s remote_rtp=%s:%d bind_rtp=%s advertised_rtp=%s:%d selected=%s/%d offered_pcmu=%v/%d offered_g722=%v/%d dtmf=%d sdp=%q",
		call.callID, remote, offer.Host, offer.Port, rtpConn.LocalAddr(), localHost, rtpPort, call.audioCodec.name(), call.audioPayload, offer.HasPCMU, offer.PCMUPayload, offer.HasG722, offer.G722Payload, call.dtmfPayload, sdp)
	connectedIdentity := ""
	if !directFeed.Matched {
		connectedIdentity = sipConnectedLineHeaders(s.extensionCallerIDName(line), selection.CalledAddress)
	}
	return call, sipReplyWithBodyTo("200 OK", request.Headers, call.localTag, "application/sdp", sdp, localHost, sipAddrPort(local), s.sipContactUser(), connectedIdentity, remote)
}

func (s *Service) sipContactUser() string {
	if s == nil {
		return "haze"
	}
	registration := s.cfg.IVR.SIP.Registration
	return firstNonBlank(registration.ContactUser, registration.FromUser, registration.Username, "haze")
}

func (s *Service) sipCallerProvinceHint(request sipRequest) string {
	if s == nil || !s.cfg.IVR.Search.CallerHintEnabled {
		return ""
	}
	for _, header := range []string{"p-asserted-identity", "remote-party-id", "from"} {
		caller := normalizeNANPCaller(sipURIUser(sipHeader(request.Headers, header)))
		if caller == "" {
			continue
		}
		return callerProvinceHint(caller)
	}
	return ""
}

func newSIPRegistrar(service *Service, conn *net.UDPConn, localAddr net.Addr) *sipRegistrar {
	return &sipRegistrar{
		service:   service,
		conn:      conn,
		localAddr: localAddr,
		responses: make(chan sipResponse, 8),
	}
}

func (r *sipRegistrar) run(ctx context.Context) {
	if r == nil || r.service == nil || r.conn == nil {
		return
	}
	cfg := r.service.cfg.IVR.SIP.Registration
	remote, err := net.ResolveUDPAddr("udp", sipServerAddr(cfg.Server))
	if err != nil {
		log.Printf("IVR SIP registration disabled: resolve %q failed: %v", cfg.Server, err)
		r.service.sipDebugf("registrar resolve failed server=%q error=%v", cfg.Server, err)
		return
	}
	password := ""
	if env := strings.TrimSpace(cfg.PasswordEnv); env != "" {
		password = os.Getenv(env)
	}
	if password == "" {
		log.Printf("IVR SIP registration disabled: password env %q is not set", cfg.PasswordEnv)
		r.service.sipDebugf("registrar password missing env=%s", cfg.PasswordEnv)
		return
	}
	r.service.sipDebugf("registrar starting remote=%s username=%s env=%s expires=%d retry_seconds=%d",
		remote, cfg.Username, cfg.PasswordEnv, cfg.Expires, cfg.RetrySeconds)
	retry := time.Duration(maxInt(5, cfg.RetrySeconds)) * time.Second
	for ctx.Err() == nil {
		expires, err := r.registerOnce(ctx, remote, password)
		if err != nil {
			log.Printf("IVR SIP registration failed: %v", err)
			r.service.sipDebugf("registrar failed remote=%s error=%v retry=%s", remote, err, retry)
			if !sleepContext(ctx, retry) {
				return
			}
			continue
		}
		refresh := time.Duration(maxInt(30, expires-30)) * time.Second
		log.Printf("IVR SIP registered to %s for %ds; refreshing in %s", remote, expires, refresh)
		r.service.sipDebugf("registrar registered remote=%s expires=%d refresh=%s", remote, expires, refresh)
		if !sleepContext(ctx, refresh) {
			return
		}
	}
}

func (r *sipRegistrar) registerOnce(ctx context.Context, remote *net.UDPAddr, password string) (int, error) {
	cfg := r.service.cfg.IVR.SIP.Registration
	domain := firstNonBlank(cfg.Domain, sipHostOnly(cfg.Server))
	registerURI := sipRegisterURI(firstNonBlank(cfg.RegisterURI, sipRegisterTarget(cfg.Server, domain)))
	username := strings.TrimSpace(cfg.Username)
	if username == "" {
		return 0, fmt.Errorf("username is not configured")
	}
	authUsername := firstNonBlank(cfg.AuthUsername, username)
	fromUser := firstNonBlank(cfg.FromUser, username)
	contactUser := firstNonBlank(cfg.ContactUser, fromUser)
	expires := maxInt(60, cfg.Expires)
	callID := randomHex(12)
	fromTag := randomHex(6)
	cseq := 1
	request := r.buildRegister(registerURI, domain, fromUser, contactUser, callID, fromTag, cseq, expires, "")
	r.service.sipDebugf("registrar sending REGISTER remote=%s call_id=%s cseq=%d auth=false domain=%s from_user=%s contact_user=%s uri=%s",
		remote, callID, cseq, domain, fromUser, contactUser, registerURI)
	response, err := r.sendRegister(ctx, remote, request, callID)
	if err != nil {
		return 0, err
	}
	r.service.sipDebugf("registrar response call_id=%s status=%d reason=%q", callID, response.StatusCode, response.Reason)
	if response.StatusCode == 200 {
		return sipResponseExpires(response, expires), nil
	}
	if response.StatusCode != 401 && response.StatusCode != 407 {
		return 0, fmt.Errorf("provider returned %d %s", response.StatusCode, response.Reason)
	}
	challenge := firstNonBlank(sipHeader(response.Headers, "www-authenticate"), sipHeader(response.Headers, "proxy-authenticate"))
	r.service.sipDebugf("registrar challenge call_id=%s %s auth_username=%s password_len=%d",
		callID, sipDigestChallengeSummary(challenge), authUsername, len(password))
	authorization, err := sipRegisterAuthorization(challenge, authUsername, password, "REGISTER", registerURI)
	if err != nil {
		return 0, err
	}
	headerName := "Authorization"
	if response.StatusCode == 407 {
		headerName = "Proxy-Authorization"
	}
	cseq++
	request = r.buildRegister(registerURI, domain, fromUser, contactUser, callID, fromTag, cseq, expires, fmt.Sprintf("%s: %s\r\n", headerName, authorization))
	r.service.sipDebugf("registrar sending REGISTER remote=%s call_id=%s cseq=%d auth=true header=%s", remote, callID, cseq, headerName)
	response, err = r.sendRegister(ctx, remote, request, callID)
	if err != nil {
		return 0, err
	}
	r.service.sipDebugf("registrar auth response call_id=%s status=%d reason=%q", callID, response.StatusCode, response.Reason)
	if response.StatusCode != 200 {
		r.service.sipDebugf("registrar auth failed call_id=%s %s", callID, sipResponseHeaderSummary(response))
		return 0, fmt.Errorf("provider returned %d %s after auth", response.StatusCode, response.Reason)
	}
	return sipResponseExpires(response, expires), nil
}

func (r *sipRegistrar) buildRegister(registerURI string, domain string, fromUser string, contactUser string, callID string, fromTag string, cseq int, expires int, authHeader string) string {
	cfg := r.service.cfg.IVR.SIP.Registration
	viaHost := r.registrationHost(cfg.ViaHost, r.localAddr)
	contactHost := r.registrationHost(cfg.ContactHost, r.localAddr)
	contactPort := sipAddrPort(r.localAddr)
	contact := fmt.Sprintf("<sip:%s@%s:%d>", contactUser, contactHost, contactPort)
	branch := "z9hG4bK-" + randomHex(8)
	userAgent := firstNonBlank(cfg.UserAgent, "Haze Weather Radio IVR")
	supportedPath := cfg.SupportedPath == nil || *cfg.SupportedPath
	var builder strings.Builder
	builder.WriteString("REGISTER " + registerURI + " SIP/2.0\r\n")
	builder.WriteString(fmt.Sprintf("Via: SIP/2.0/UDP %s:%d;branch=%s;rport\r\n", viaHost, contactPort, branch))
	builder.WriteString(fmt.Sprintf("Max-Forwards: 70\r\n"))
	builder.WriteString(fmt.Sprintf("From: <sip:%s@%s>;tag=%s\r\n", fromUser, domain, fromTag))
	builder.WriteString(fmt.Sprintf("To: <sip:%s@%s>\r\n", fromUser, domain))
	builder.WriteString("Call-ID: " + callID + "\r\n")
	builder.WriteString(fmt.Sprintf("CSeq: %d REGISTER\r\n", cseq))
	builder.WriteString("Contact: " + contact + "\r\n")
	builder.WriteString(fmt.Sprintf("Expires: %d\r\n", expires))
	if supportedPath {
		builder.WriteString("Supported: path\r\n")
	}
	builder.WriteString("User-Agent: " + userAgent + "\r\n")
	builder.WriteString(authHeader)
	builder.WriteString("Content-Length: 0\r\n\r\n")
	return builder.String()
}

func (r *sipRegistrar) registrationHost(configured string, local net.Addr) string {
	value := strings.TrimSpace(configured)
	switch strings.ToLower(value) {
	case "", "local":
		return r.service.sipLocalAdvertiseHost(nil, local)
	case "public", "public_host":
		if host := strings.TrimSpace(r.service.cfg.IVR.SIP.PublicHost); host != "" {
			return sipHostNameOnly(host)
		}
		return r.service.sipLocalAdvertiseHost(nil, local)
	default:
		return sipHostNameOnly(value)
	}
}

func (r *sipRegistrar) sendRegister(ctx context.Context, remote *net.UDPAddr, request string, callID string) (sipResponse, error) {
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case response := <-r.responses:
			if sipHeader(response.Headers, "call-id") == callID {
				continue
			}
		default:
			goto drained
		}
	}
drained:
	if _, err := r.conn.WriteToUDP([]byte(request), remote); err != nil {
		return sipResponse{}, err
	}
	r.service.sipDebugf("registrar sent REGISTER bytes=%d remote=%s call_id=%s", len(request), remote, callID)
	for {
		select {
		case <-ctx.Done():
			return sipResponse{}, ctx.Err()
		case <-deadline.C:
			return sipResponse{}, fmt.Errorf("REGISTER timed out")
		case response := <-r.responses:
			if sipHeader(response.Headers, "call-id") != callID {
				continue
			}
			return response, nil
		}
	}
}

func (r *sipRegistrar) handleResponse(response sipResponse) {
	select {
	case r.responses <- response:
	default:
	}
}

func (s *Service) selectSIPAudioCodec(offer sipMediaOffer) (sipAudioCodec, int) {
	preferred := strings.ToLower(strings.TrimSpace(s.cfg.IVR.Cache.PhoneCodec))
	switch preferred {
	case "g722", "g.722":
		if offer.HasG722 {
			return sipAudioCodecG722, fallbackInt(offer.G722Payload, sipPayloadG722)
		}
		if offer.HasPCMU {
			return sipAudioCodecPCMU, fallbackInt(offer.PCMUPayload, sipPayloadPCMU)
		}
	default:
		if offer.HasPCMU {
			return sipAudioCodecPCMU, fallbackInt(offer.PCMUPayload, sipPayloadPCMU)
		}
		if offer.HasG722 {
			return sipAudioCodecG722, fallbackInt(offer.G722Payload, sipPayloadG722)
		}
	}
	return offer.AudioCodec, offer.AudioPayload
}

func (s *Service) sipDebugf(format string, args ...any) {
	if s == nil {
		return
	}
	dir := filepath.Join(s.cfg.BaseDir, "runtime", "ivr")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	line := time.Now().Format(time.RFC3339Nano) + " " + fmt.Sprintf(format, args...) + "\n"
	file, err := os.OpenFile(filepath.Join(dir, "sip-debug.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.WriteString(line)
}

func (s *Service) sipAdvertiseHost(remote *net.UDPAddr, local net.Addr) string {
	if remote != nil && sipRemoteIsLocal(remote.IP) {
		return s.sipLocalAdvertiseHost(remote, local)
	}
	if host := strings.TrimSpace(s.cfg.IVR.SIP.PublicHost); host != "" {
		return host
	}
	return s.sipLocalAdvertiseHost(remote, local)
}

func (s *Service) sipLocalAdvertiseHost(remote *net.UDPAddr, local net.Addr) string {
	if udp, ok := local.(*net.UDPAddr); ok && udp.IP != nil && !udp.IP.IsUnspecified() {
		if ip := usableIPv4(udp.IP); ip != nil {
			return ip.String()
		}
	}
	if remote != nil {
		port := remote.Port
		if port <= 0 {
			port = 9
		}
		if conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: remote.IP, Port: port}); err == nil {
			defer conn.Close()
			if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && addr.IP != nil && !addr.IP.IsUnspecified() {
				if ip := addr.IP.To4(); ip != nil {
					return ip.String()
				}
			}
		}
	}
	if host := firstConnectedInterfaceIPv4(); host != "" {
		return host
	}
	if udp, ok := local.(*net.UDPAddr); ok && udp.IP != nil && !udp.IP.IsUnspecified() {
		return udp.IP.String()
	}
	return "127.0.0.1"
}

func sipRemoteIsLocal(ip net.IP) bool {
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || isPrivateIPv4(ip)
}

func (s *Service) sipRTPBindHost(local net.Addr) string {
	if udp, ok := local.(*net.UDPAddr); ok && udp.IP != nil && !udp.IP.IsUnspecified() {
		if ip := usableIPv4(udp.IP); ip != nil {
			return ip.String()
		}
	}
	return ""
}

func firstConnectedInterfaceIPv4() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	fallback := ""
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip := interfaceAddrIP(addr)
			if ip == nil {
				continue
			}
			if isPrivateIPv4(ip) {
				return ip.String()
			}
			if fallback == "" && ip.IsGlobalUnicast() && !ip.IsLinkLocalUnicast() {
				fallback = ip.String()
			}
		}
	}
	return fallback
}

func interfaceAddrIP(addr net.Addr) net.IP {
	switch typed := addr.(type) {
	case *net.IPNet:
		return usableIPv4(typed.IP)
	case *net.IPAddr:
		return usableIPv4(typed.IP)
	default:
		return nil
	}
}

func usableIPv4(ip net.IP) net.IP {
	if ip == nil {
		return nil
	}
	ip = ip.To4()
	if ip == nil || ip.IsUnspecified() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return nil
	}
	return ip
}

func isPrivateIPv4(ip net.IP) bool {
	ip = usableIPv4(ip)
	if ip == nil {
		return false
	}
	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}

func (c *sipCall) run() {
	defer c.close()
	go c.readRTP()
	if c.directFeedID != "" {
		for c.ctx.Err() == nil && c.playLiveBroadcast(c.directFeedID) {
		}
		return
	}
	c.menuLoop()
}

func (c *sipCall) close() {
	select {
	case <-c.done:
		return
	default:
		close(c.done)
		c.cancel()
		_ = c.rtpConn.Close()
	}
}

func (c *sipCall) timeoutHangup() {
	c.playPrompt("error", "timeout", nil)
	c.hangup()
}

func (c *sipCall) hangup() {
	c.sendBYE()
	c.close()
}

func (c *sipCall) sendBYE() {
	if c == nil || c.sipConn == nil || c.sipRemote == nil {
		return
	}
	c.byeOnce.Do(func() {
		requestURI := strings.TrimSpace(c.sipRequestURI)
		if requestURI == "" {
			requestURI = "sip:unknown"
		}
		localHost, localPort := sipAddrHostPort(c.sipConn.LocalAddr())
		if localHost == "" {
			localHost = "0.0.0.0"
		}
		from := sipHeader(c.sipHeaders, "to")
		if from == "" {
			from = "<sip:haze@" + localHost + ">"
		}
		if !strings.Contains(strings.ToLower(from), "tag=") {
			from += ";tag=" + c.localTag
		}
		to := sipHeader(c.sipHeaders, "from")
		if to == "" {
			to = "<sip:caller@" + c.sipRemote.IP.String() + ">"
		}
		userAgent := firstNonBlank(c.service.cfg.IVR.SIP.Registration.UserAgent, "Haze Weather Radio IVR")
		var builder strings.Builder
		builder.WriteString("BYE " + requestURI + " SIP/2.0\r\n")
		builder.WriteString(fmt.Sprintf("Via: SIP/2.0/UDP %s:%d;branch=z9hG4bK-%s;rport\r\n", localHost, localPort, randomHex(8)))
		builder.WriteString("Max-Forwards: 70\r\n")
		builder.WriteString("From: " + from + "\r\n")
		builder.WriteString("To: " + to + "\r\n")
		builder.WriteString("Call-ID: " + c.callID + "\r\n")
		builder.WriteString("CSeq: 2 BYE\r\n")
		builder.WriteString("User-Agent: " + userAgent + "\r\n")
		builder.WriteString("Content-Length: 0\r\n\r\n")
		_, _ = c.sipConn.WriteToUDP([]byte(builder.String()), c.sipRemote)
	})
}

func (c *sipCall) menuLoop() {
	language := c.service.cfg.IVR.DefaultLanguage
	directProvince := c.line.directProvince()
	c.playPrompt("entry", "greeting", c.service.extensionGreetingValues(c.line))
	for c.ctx.Err() == nil {
		entry, _ := c.service.cfg.Prompts.Menu("entry")
		lineKey := c.service.menuMainLine("entry", "")
		if configuredLanguage, single := c.service.singleConfiguredLanguage(); single {
			language = configuredLanguage
			var location ResolvedLocation
			var ok bool
			if directProvince != "" {
				location, ok = c.collectLocationNumber(language, directProvince)
			} else {
				location, ok = c.collectLocationWithPrompt(language, "entry", lineKey, c.service.promptValues(nil), entry.Timeout)
			}
			if !ok {
				return
			}
			c.locationMenu(location)
			continue
		}
		digit, ok := c.promptAndWaitDigit("entry", lineKey, c.service.promptValues(nil), entry.Timeout)
		if !ok {
			c.timeoutHangup()
			return
		}
		option, ok := c.service.cfg.Prompts.Option("entry", digit)
		if !ok {
			c.playPrompt("error", "invalid_code", nil)
			continue
		}
		switch option.Action {
		case "language":
			if !c.service.languageConfigured(option.Language) {
				c.playPrompt("error", "invalid_code", nil)
				continue
			}
			language = fallbackText(option.Language, language)
			var location ResolvedLocation
			if directProvince != "" {
				location, ok = c.collectLocationNumber(language, directProvince)
			} else {
				location, ok = c.collectLocation(language)
			}
			if !ok {
				return
			}
			c.locationMenu(location)
		case "product":
			location, err := c.service.defaultFeedLocation()
			if err != nil {
				c.playPrompt("weather_product", "unavailable", nil)
				return
			}
			if option.Language != "" {
				location.Language = option.Language
			}
			c.playMappedProduct(location, splitCSV(option.Packages), true)
		case "broadcast":
			location, err := c.service.defaultFeedLocation()
			if err != nil || !c.service.broadcastAvailable(location.FeedID) {
				c.playPrompt("error", "invalid_code", nil)
				continue
			}
			location.Language = language
			c.playPrompt("broadcast_menu", "main", nil)
			if !c.playLiveBroadcast(location.FeedID) {
				packages := c.service.broadcastPackages(option)
				c.playProduct(c.service.mapProductLocation(location, packages, nil), packages)
			}
		case "operator":
			c.playPrompt("operator", "main", nil)
			return
		default:
			c.playPrompt("error", "invalid_code", nil)
		}
	}
}

func (c *sipCall) collectLocation(language string) (ResolvedLocation, bool) {
	return c.collectLocationWithPrompt(language, "location_code", "main", nil, 0)
}

func (c *sipCall) collectLocationWithPrompt(language string, menuID string, lineKey string, values map[string]string, timeout time.Duration) (ResolvedLocation, bool) {
	menu, _ := c.service.cfg.Prompts.Menu("location_code")
	if timeout <= 0 {
		timeout = menu.Timeout
	}
	attempts := maxInt(1, menu.Retries+1)
	for attempt := 0; attempt < attempts && c.ctx.Err() == nil; attempt++ {
		firstDigit, interrupted := c.playPromptForDigit(menuID, lineKey, values)
		var code string
		var geophysical bool
		var ok bool
		if interrupted {
			code, geophysical, ok = c.collectLocationInput(timeout, firstDigit)
		} else {
			code, geophysical, ok = c.collectLocationInput(timeout, "")
		}
		if !ok {
			c.timeoutHangup()
			return ResolvedLocation{}, false
		}
		if geophysical {
			location, err := c.service.defaultFeedLocation()
			if err != nil {
				c.playPrompt("weather_product", "unavailable", nil)
				return ResolvedLocation{}, false
			}
			location.Language = language
			_ = c.playProduct(location, []string{"geophysical_alert"})
			continue
		}
		location, err := c.service.resolveLocation(code)
		if err == nil {
			location.Language = language
			return location, true
		}
		if isProvinceDigit(code) {
			location, ok := c.collectLocationNumber(language, code)
			if ok {
				return location, true
			}
			return ResolvedLocation{}, false
		}
		c.playPrompt("error", "invalid_code", nil)
	}
	return ResolvedLocation{}, false
}

func (c *sipCall) collectLocationNumber(language string, province string) (ResolvedLocation, bool) {
	menu, _ := c.service.cfg.Prompts.Menu("location_number")
	for attempt := 0; attempt < maxInt(1, menu.Retries+1) && c.ctx.Err() == nil; attempt++ {
		firstDigit, interrupted := c.playPromptForDigit("location_number", "main", map[string]string{"province": provinceDigitDisplayName(province)})
		initial := ""
		if interrupted {
			initial = firstDigit
		}
		number, search, ok := c.collectLocationNumberInput(menu.Timeout, province, initial)
		if !ok {
			c.timeoutHangup()
			return ResolvedLocation{}, false
		}
		if search {
			if location, found := c.searchLocation(language, province); found {
				return location, true
			}
			continue
		}
		code, codeOK := c.service.resolver.helloWeatherCodeForProvinceNumber(province, number)
		if !codeOK {
			c.playPrompt("error", "invalid_code", nil)
			continue
		}
		location, err := c.service.resolveLocation(code)
		if err != nil {
			c.playPrompt("error", "invalid_code", nil)
			continue
		}
		location.Language = language
		return location, true
	}
	return ResolvedLocation{}, false
}

func (c *sipCall) locationMenu(location ResolvedLocation) {
	menu, _ := c.service.cfg.Prompts.Menu("location_menu")
	for c.ctx.Err() == nil {
		alerts := c.service.activeIVRAlerts(c.ctx, location)
		var digit string
		var ok bool
		if len(alerts) > 0 {
			digit, ok = c.promptTextAndWaitDigit("location_menu_"+firstNonBlank(location.FeedID, location.Code, "default"), c.service.locationMenuAlertText(location, len(alerts)), menu.Timeout)
		} else {
			lineKey := c.service.locationMenuMainLine(location)
			digit, ok = c.promptAndWaitDigit("location_menu", lineKey, c.service.promptValues(map[string]string{"location": spokenLocationName(location), "feed_id": location.FeedID}), menu.Timeout)
		}
		if !ok {
			c.timeoutHangup()
			return
		}
		if digit == "#" {
			return
		}
		if digit == "*" {
			if len(alerts) > 0 {
				c.alertMenu(location)
				continue
			}
			c.playPrompt("error", "invalid_code", nil)
			continue
		}
		option, ok := c.service.cfg.Prompts.Option("location_menu", digit)
		if !ok {
			c.playPrompt("error", "invalid_code", nil)
			continue
		}
		switch option.Action {
		case "product":
			c.playMappedProduct(location, splitCSV(option.Packages), true)
		case "menu":
			c.configuredMenu(location, option.Next)
		case "broadcast":
			if !c.service.locationBroadcastAvailable(location) {
				c.playPrompt("error", "invalid_code", nil)
				continue
			}
			c.playPrompt("broadcast_menu", "main", nil)
			if !c.playLiveBroadcast(location.FeedID) {
				packages := c.service.broadcastPackages(option)
				_ = c.playProduct(c.service.mapProductLocation(location, packages, nil), packages)
			}
		case "operator":
			c.playPrompt("operator", "main", nil)
			return
		default:
			c.playPrompt("error", "invalid_code", nil)
		}
	}
}

func (c *sipCall) alertMenu(location ResolvedLocation) {
	menu, _ := c.service.cfg.Prompts.Menu("location_menu")
	for c.ctx.Err() == nil {
		alerts := c.service.activeIVRAlerts(c.ctx, location)
		if len(alerts) == 0 {
			c.playTextPrompt("alert_unavailable", fmt.Sprintf("%s has no active alerts in effect.", spokenLocationName(location)))
			return
		}
		digit, ok := c.promptTextAndWaitDigit("alert_menu_"+firstNonBlank(location.FeedID, location.Code, "default"), c.service.alertMenuText(location, alerts), menu.Timeout)
		if !ok {
			c.timeoutHangup()
			return
		}
		if digit == "#" {
			return
		}
		alert, ok := ivrAlertByDigit(alerts, digit)
		if !ok {
			c.playPrompt("error", "invalid_code", nil)
			continue
		}
		if interrupt, ok := c.playTextPromptAudioWithInterrupt(
			"alert_readout_"+firstNonBlank(alert.ID, digit),
			c.service.alertReadoutText(location, alert),
			digitInterruptPound,
		); ok && interrupt == "#" {
			return
		}
	}
}

func (c *sipCall) configuredMenu(location ResolvedLocation, menuID string) {
	menuID = strings.ToLower(strings.TrimSpace(menuID))
	menu, ok := c.service.cfg.Prompts.Menu(menuID)
	if !ok {
		c.playPrompt("error", "invalid_code", nil)
		return
	}
	for c.ctx.Err() == nil {
		digit, ok := c.promptAndWaitDigit(menuID, "main", map[string]string{"location": spokenLocationName(location), "feed_id": location.FeedID}, menu.Timeout)
		if !ok {
			c.timeoutHangup()
			return
		}
		if digit == "#" {
			return
		}
		option, ok := c.service.cfg.Prompts.Option(menuID, digit)
		if !ok {
			c.playPrompt("error", "invalid_code", nil)
			continue
		}
		switch option.Action {
		case "product":
			c.playMappedProduct(location, splitCSV(option.Packages), true)
		case "menu":
			c.configuredMenu(location, option.Next)
		case "broadcast":
			if !c.service.locationBroadcastAvailable(location) {
				c.playPrompt("error", "invalid_code", nil)
				continue
			}
			c.playPrompt("broadcast_menu", "main", nil)
			if !c.playLiveBroadcast(location.FeedID) {
				packages := c.service.broadcastPackages(option)
				_ = c.playProduct(c.service.mapProductLocation(location, packages, nil), packages)
			}
		case "operator":
			c.playPrompt("operator", "main", nil)
			return
		default:
			c.playPrompt("error", "invalid_code", nil)
		}
	}
}

func (c *sipCall) playPrompt(menuID string, lineKey string, values map[string]string) {
	_, _ = c.playPromptAudio(menuID, lineKey, values, false)
}

func (c *sipCall) playTextPrompt(lineKey string, text string) {
	_, _ = c.playTextPromptAudio(lineKey, text, false)
}

func (c *sipCall) promptAndWaitDigit(menuID string, lineKey string, values map[string]string, timeout time.Duration) (string, bool) {
	if digit, ok := c.playPromptForDigit(menuID, lineKey, values); ok {
		return digit, true
	}
	return c.waitDigit(timeout)
}

func (c *sipCall) promptTextAndWaitDigit(lineKey string, text string, timeout time.Duration) (string, bool) {
	if digit, ok := c.playTextPromptAudio(lineKey, text, true); ok {
		return digit, true
	}
	return c.waitDigit(timeout)
}

func (c *sipCall) playPromptForDigit(menuID string, lineKey string, values map[string]string) (string, bool) {
	return c.playPromptAudio(menuID, lineKey, values, true)
}

func (c *sipCall) playPromptAudio(menuID string, lineKey string, values map[string]string, interruptible bool) (string, bool) {
	promptValues := c.service.promptValues(values)
	audio, ok := c.service.staticPromptAudio(menuID, lineKey, promptValues)
	if !ok {
		var err error
		audio, err = c.service.cache.GetPromptWithPolicy(c.ctx, menuID, lineKey, promptValues, c.service.staticPromptPolicy(), false)
		if err != nil {
			log.Printf("IVR SIP prompt %s/%s failed: %v", menuID, lineKey, err)
			return "", false
		}
	}
	if interruptible {
		return c.playAudioFile(c.cachedAudioPath(audio), digitInterruptAny)
	}
	return c.playAudioFile(c.cachedAudioPath(audio), digitInterruptNone)
}

func (c *sipCall) playTextPromptAudio(lineKey string, text string, interruptible bool) (string, bool) {
	interruptMode := digitInterruptNone
	if interruptible {
		interruptMode = digitInterruptAny
	}
	return c.playTextPromptAudioWithInterrupt(lineKey, text, interruptMode)
}

func (c *sipCall) playTextPromptAudioWithInterrupt(lineKey string, text string, interruptMode digitInterruptMode) (string, bool) {
	audio, err := c.service.textPromptAudio(c.ctx, lineKey, text)
	if err != nil {
		log.Printf("IVR SIP dynamic prompt %s failed: %v", lineKey, err)
		return "", false
	}
	return c.playAudioFile(c.cachedAudioPath(audio), interruptMode)
}

func (c *sipCall) playProduct(location ResolvedLocation, packages []string) bool {
	packages = normalizePackages(packages, c.service.cfg.IVR.DefaultPackages)
	if _, ok := c.service.cache.Fresh(location, packages); !ok {
		c.playPrompt("", "one_moment", nil)
	}
	type result struct {
		product CachedProduct
		err     error
	}
	done := make(chan result, 1)
	go func() {
		product, err := c.service.productForLocation(c.ctx, location, packages, false)
		done <- result{product: product, err: err}
	}()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	silence := c.audioCodec.silenceFrame()
	for {
		select {
		case <-c.ctx.Done():
			return false
		case result := <-done:
			if result.err != nil {
				log.Printf("IVR SIP product unavailable for %s packages=%s: %v", firstNonBlank(location.Code, location.FeedID), strings.Join(packages, ","), result.err)
				c.playPrompt("weather_product", "unavailable", nil)
				return false
			}
			digit, ok := c.playAudioFile(c.cachedProductPath(result.product), digitInterruptPound)
			return ok && digit == "#"
		case <-ticker.C:
			if digit, ok := c.pendingInterruptDigit(digitInterruptPound); ok && digit == "#" {
				return true
			}
			c.sendRTP(silence)
		}
	}
}

func (c *sipCall) playPCMUFile(path string, interruptMode digitInterruptMode) (string, bool) {
	return c.playAudioFile(path, interruptMode)
}

func (c *sipCall) playAudioFile(path string, interruptMode digitInterruptMode) (string, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		log.Printf("IVR SIP audio read failed: %v", err)
		return "", false
	}
	if len(raw) == 0 {
		return "", false
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var digitEvents <-chan string
	if interruptMode != digitInterruptNone {
		digitEvents = c.digits
	}
	for offset := 0; offset < len(raw) && c.ctx.Err() == nil; offset += sipPacketSamples {
		if digit, ok := c.pendingInterruptDigit(interruptMode); ok {
			return digit, true
		}
		end := offset + sipPacketSamples
		frame := c.audioCodec.silenceFrame()
		if end > len(raw) {
			end = len(raw)
		}
		copy(frame, raw[offset:end])
		c.sendRTP(frame)
		select {
		case <-c.ctx.Done():
			return "", false
		case digit := <-digitEvents:
			if interruptMode.matches(digit) {
				return digit, true
			}
		case <-ticker.C:
		}
	}
	return "", false
}

func (c *sipCall) cachedAudioPath(audio CachedAudio) string {
	if c.audioCodec == sipAudioCodecG722 && strings.TrimSpace(audio.G722Path) != "" {
		return audio.G722Path
	}
	return audio.PCMUPath
}

func (c *sipCall) cachedProductPath(product CachedProduct) string {
	if c.audioCodec == sipAudioCodecG722 && strings.TrimSpace(product.G722Path) != "" {
		return product.G722Path
	}
	return product.PCMUPath
}

func (c *sipCall) sendRTP(payload []byte) {
	if c.rtpConn == nil || len(payload) == 0 {
		return
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.remoteRTP == nil {
		return
	}
	packet := make([]byte, 12+len(payload))
	packet[0] = 0x80
	packet[1] = byte(c.audioPayload & 0x7F)
	binary.BigEndian.PutUint16(packet[2:4], c.seq)
	binary.BigEndian.PutUint32(packet[4:8], c.timestamp)
	binary.BigEndian.PutUint32(packet[8:12], c.ssrc)
	copy(packet[12:], payload)
	_, _ = c.rtpConn.WriteToUDP(packet, c.remoteRTP)
	if !c.sentRTPLog {
		c.sentRTPLog = true
		c.service.sipDebugf("sent first RTP call=%s local=%s remote=%s payload=%d codec=%s bytes=%d", c.callID, c.rtpConn.LocalAddr(), c.remoteRTP, c.audioPayload, c.audioCodec.name(), len(payload))
	}
	c.seq++
	c.timestamp += c.audioCodec.timestampStep(payload)
}

func (codec sipAudioCodec) defaultPayload() byte {
	if codec == sipAudioCodecG722 {
		return sipPayloadG722
	}
	return sipPayloadPCMU
}

func (codec sipAudioCodec) rtpmap(payload int) string {
	if codec == sipAudioCodecG722 {
		return fmt.Sprintf("a=rtpmap:%d G722/8000", payload)
	}
	return fmt.Sprintf("a=rtpmap:%d PCMU/8000", payload)
}

func (codec sipAudioCodec) name() string {
	if codec == sipAudioCodecG722 {
		return "G722"
	}
	return "PCMU"
}

func (codec sipAudioCodec) silenceFrame() []byte {
	if codec == sipAudioCodecG722 {
		return g722SilenceFrame()
	}
	frame := make([]byte, sipPacketSamples)
	for i := range frame {
		frame[i] = 0xff
	}
	return frame
}

func fallbackInt(value int, fallback int) int {
	if value >= 0 {
		return value
	}
	return fallback
}

func (codec sipAudioCodec) timestampStep(_ []byte) uint32 {
	return sipPacketSamples
}

func (c *sipCall) readRTP() {
	buffer := make([]byte, 1500)
	var lastEvent string
	var decoder *sipInboundDecoder
	wasCapturing := false
	for {
		n, remote, err := c.rtpConn.ReadFromUDP(buffer)
		if err != nil {
			return
		}
		c.learnSymmetricRTP(remote)
		if !c.recvRTPLog {
			c.recvRTPLog = true
			c.service.sipDebugf("received first RTP call=%s local=%s remote=%s bytes=%d", c.callID, c.rtpConn.LocalAddr(), remote, n)
		}
		digit, key := rtpDTMFDigit(buffer[:n], c.dtmfPayload)
		if digit != "" && key != lastEvent {
			lastEvent = key
			c.pushDigit(digit)
		}
		capturing := c.captureActive.Load()
		if !capturing {
			wasCapturing = false
			continue
		}
		if !wasCapturing || decoder == nil {
			decoder = newSIPInboundDecoder(c.audioCodec, c.audioPayload, c.dtmfPayload)
			wasCapturing = true
		}
		frames, _, _, ok := decoder.Decode(buffer[:n], time.Now())
		if !ok {
			continue
		}
		for _, frame := range frames {
			c.pushPCMFrame(frame)
		}
	}
}

func (c *sipCall) beginVoiceCapture() <-chan pcmFrame {
	if c == nil {
		return nil
	}
	for {
		select {
		case <-c.audioFrames:
		default:
			c.captureActive.Store(true)
			return c.audioFrames
		}
	}
}

func (c *sipCall) endVoiceCapture() {
	if c != nil {
		c.captureActive.Store(false)
	}
}

func (c *sipCall) pushPCMFrame(frame pcmFrame) {
	if c == nil || !c.captureActive.Load() || len(frame.Samples) == 0 {
		return
	}
	select {
	case c.audioFrames <- frame:
		return
	default:
	}
	select {
	case <-c.audioFrames:
	default:
	}
	select {
	case c.audioFrames <- frame:
	default:
	}
}

func (c *sipCall) learnSymmetricRTP(remote *net.UDPAddr) {
	if remote == nil || remote.IP == nil || remote.Port <= 0 {
		return
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if sameUDPAddr(c.remoteRTP, remote) {
		return
	}
	previous := "<nil>"
	if c.remoteRTP != nil {
		previous = c.remoteRTP.String()
	}
	c.remoteRTP = &net.UDPAddr{IP: append(net.IP(nil), remote.IP...), Port: remote.Port, Zone: remote.Zone}
	c.service.sipDebugf("symmetric RTP learned call=%s previous_remote=%s learned_remote=%s", c.callID, previous, c.remoteRTP)
}

func sameUDPAddr(left *net.UDPAddr, right *net.UDPAddr) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Port == right.Port && left.Zone == right.Zone && left.IP.Equal(right.IP)
}

func (c *sipCall) pushDigit(digit string) {
	select {
	case c.digits <- digit:
	default:
	}
}

func (c *sipCall) pendingInterruptDigit(interruptMode digitInterruptMode) (string, bool) {
	if interruptMode == digitInterruptNone {
		return "", false
	}
	select {
	case digit := <-c.digits:
		return digit, interruptMode.matches(digit)
	default:
		return "", false
	}
}

func (mode digitInterruptMode) matches(digit string) bool {
	switch mode {
	case digitInterruptAny:
		return digit != ""
	case digitInterruptPound:
		return digit == "#"
	default:
		return false
	}
}

func (c *sipCall) waitDigit(timeout time.Duration) (string, bool) {
	if timeout <= 0 {
		timeout = time.Duration(c.service.cfg.IVR.DigitTimeoutSeconds) * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return "", false
		case digit := <-c.digits:
			if digit != "" {
				return digit, true
			}
		case <-timer.C:
			return "", false
		}
	}
}

func (c *sipCall) collectDigits(timeout time.Duration, initial string) (string, bool) {
	if timeout <= 0 {
		timeout = time.Duration(c.service.cfg.IVR.DigitTimeoutSeconds) * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var builder strings.Builder
	if done, ok := appendCollectedDigit(&builder, initial); done {
		return builder.String(), ok
	}
	for {
		select {
		case <-c.ctx.Done():
			return "", false
		case digit := <-c.digits:
			done, ok := appendCollectedDigit(&builder, digit)
			if builder.Len() >= 12 {
				return builder.String(), true
			}
			if done {
				return builder.String(), ok
			}
		case <-timer.C:
			return builder.String(), builder.Len() > 0
		}
	}
}

func (c *sipCall) collectLocationInput(timeout time.Duration, initial string) (string, bool, bool) {
	if timeout <= 0 {
		timeout = time.Duration(c.service.cfg.IVR.DigitTimeoutSeconds) * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var submitTimer *time.Timer
	var submit <-chan time.Time
	stopSubmitTimer := func() {
		if submitTimer == nil {
			return
		}
		if !submitTimer.Stop() {
			select {
			case <-submitTimer.C:
			default:
			}
		}
		submitTimer = nil
		submit = nil
	}
	defer stopSubmitTimer()
	var builder strings.Builder
	processDigit := func(digit string) (done bool, geophysical bool, ok bool) {
		switch digit {
		case "":
			return false, false, false
		case "*":
			if builder.Len() == 0 {
				return true, true, true
			}
			builder.Reset()
			stopSubmitTimer()
			return false, false, false
		case "#":
			return true, false, builder.Len() > 0
		default:
			if len(digit) == 1 && digit[0] >= '0' && digit[0] <= '9' {
				builder.WriteString(digit)
				if builder.Len() >= 12 {
					return true, false, true
				}
			}
		}
		if c.locationCodeCurrentlyValid(builder.String()) {
			stopSubmitTimer()
			submitTimer = time.NewTimer(600 * time.Millisecond)
			submit = submitTimer.C
		} else {
			stopSubmitTimer()
		}
		return false, false, false
	}
	if done, geophysical, ok := processDigit(initial); done {
		return builder.String(), geophysical, ok
	}
	for {
		select {
		case <-c.ctx.Done():
			return "", false, false
		case digit := <-c.digits:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
			if done, geophysical, ok := processDigit(digit); done {
				return builder.String(), geophysical, ok
			}
		case <-submit:
			return builder.String(), false, builder.Len() > 0
		case <-timer.C:
			return builder.String(), false, builder.Len() > 0
		}
	}
}

func (c *sipCall) collectLocationNumberInput(timeout time.Duration, province string, initial string) (string, bool, bool) {
	if timeout <= 0 {
		timeout = time.Duration(c.service.cfg.IVR.DigitTimeoutSeconds) * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var submitTimer *time.Timer
	var submit <-chan time.Time
	stopSubmitTimer := func() {
		if submitTimer == nil {
			return
		}
		if !submitTimer.Stop() {
			select {
			case <-submitTimer.C:
			default:
			}
		}
		submitTimer = nil
		submit = nil
	}
	defer stopSubmitTimer()
	var builder strings.Builder
	processDigit := func(digit string) (done bool, search bool, ok bool) {
		switch digit {
		case "":
			return false, false, false
		case "*":
			return true, true, true
		case "#":
			return true, false, builder.Len() > 0
		default:
			if len(digit) == 1 && digit[0] >= '0' && digit[0] <= '9' && builder.Len() < 5 {
				builder.WriteString(digit)
			}
		}
		if code, ok := c.service.resolver.helloWeatherCodeForProvinceNumber(province, builder.String()); ok && c.locationCodeCurrentlyValid(code) {
			stopSubmitTimer()
			submitTimer = time.NewTimer(600 * time.Millisecond)
			submit = submitTimer.C
		} else {
			stopSubmitTimer()
		}
		if builder.Len() >= 5 {
			return true, false, true
		}
		return false, false, false
	}
	if done, search, ok := processDigit(initial); done {
		return builder.String(), search, ok
	}
	for {
		select {
		case <-c.ctx.Done():
			return "", false, false
		case digit := <-c.digits:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
			if done, search, ok := processDigit(digit); done {
				return builder.String(), search, ok
			}
		case <-submit:
			return builder.String(), false, builder.Len() > 0
		case <-timer.C:
			return builder.String(), false, builder.Len() > 0
		}
	}
}

func (c *sipCall) locationCodeCurrentlyValid(code string) bool {
	if strings.TrimSpace(code) == "" || c == nil || c.service == nil {
		return false
	}
	_, err := c.service.resolveLocation(code)
	return err == nil
}

func appendCollectedDigit(builder *strings.Builder, digit string) (bool, bool) {
	if digit == "" {
		return false, false
	}
	if digit == "#" {
		return true, builder.Len() > 0
	}
	if digit == "*" {
		builder.Reset()
		return false, false
	}
	if len(digit) == 1 && digit[0] >= '0' && digit[0] <= '9' {
		builder.WriteString(digit)
	}
	return false, false
}

func parseSIPResponse(raw string) (sipResponse, bool) {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	head, body, _ := strings.Cut(normalized, "\n\n")
	lines := strings.Split(head, "\n")
	if len(lines) == 0 {
		return sipResponse{}, false
	}
	statusLine := strings.TrimSpace(lines[0])
	if !strings.HasPrefix(statusLine, "SIP/2.0 ") {
		return sipResponse{}, false
	}
	fields := strings.Fields(statusLine)
	if len(fields) < 2 {
		return sipResponse{}, false
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return sipResponse{}, false
	}
	reason := ""
	if len(statusLine) > len("SIP/2.0 ")+3 {
		reason = strings.TrimSpace(statusLine[len("SIP/2.0 ")+3:])
	}
	return sipResponse{
		StatusCode: code,
		Reason:     reason,
		Headers:    sipHeaders(lines[1:]),
		Body:       body,
	}, true
}

func parseSIPRequest(raw string) sipRequest {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	head, body, _ := strings.Cut(normalized, "\n\n")
	lines := strings.Split(head, "\n")
	if len(lines) == 0 {
		return sipRequest{}
	}
	fields := strings.Fields(strings.TrimSpace(lines[0]))
	if len(fields) < 1 {
		return sipRequest{}
	}
	headers := sipHeaders(lines[1:])
	method := strings.ToUpper(fields[0])
	uri := ""
	if len(fields) > 1 {
		uri = fields[1]
	}
	return sipRequest{Method: method, URI: uri, Headers: headers, Body: body}
}

func sipServerAddr(server string) string {
	server = strings.TrimSpace(server)
	if server == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(server), "sip:") {
		server = strings.TrimSpace(server[4:])
	}
	server = strings.Trim(server, "<>")
	if strings.Contains(server, "@") {
		_, server, _ = strings.Cut(server, "@")
	}
	if strings.Contains(server, ";") {
		server, _, _ = strings.Cut(server, ";")
	}
	if _, _, err := net.SplitHostPort(server); err == nil {
		return server
	}
	return net.JoinHostPort(server, "5060")
}

func sipHostOnly(server string) string {
	addr := sipServerAddr(server)
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return strings.TrimSpace(server)
	}
	return host
}

func sipRegisterTarget(server string, domain string) string {
	server = strings.TrimSpace(server)
	if strings.HasPrefix(strings.ToLower(server), "sip:") {
		server = strings.TrimSpace(server[4:])
	}
	server = strings.Trim(server, "<>")
	if strings.Contains(server, "@") {
		_, server, _ = strings.Cut(server, "@")
	}
	if strings.Contains(server, ";") {
		server, _, _ = strings.Cut(server, ";")
	}
	if server != "" {
		return server
	}
	return strings.TrimSpace(domain)
}

func sipRegisterURI(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(value), "sip:") {
		return value
	}
	return "sip:" + value
}

func sipHostNameOnly(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "<>")
	if strings.HasPrefix(strings.ToLower(value), "sip:") {
		value = strings.TrimSpace(value[4:])
	}
	if strings.Contains(value, "@") {
		_, value, _ = strings.Cut(value, "@")
	}
	if strings.Contains(value, ";") {
		value, _, _ = strings.Cut(value, ";")
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		return host
	}
	if colon := strings.LastIndex(value, ":"); colon > 0 {
		if _, err := strconv.Atoi(value[colon+1:]); err == nil {
			return value[:colon]
		}
	}
	return value
}

func sipAddrPort(addr net.Addr) int {
	if udp, ok := addr.(*net.UDPAddr); ok && udp.Port > 0 {
		return udp.Port
	}
	return 5060
}

func sipAddrHostPort(addr net.Addr) (string, int) {
	if udp, ok := addr.(*net.UDPAddr); ok {
		host := ""
		if udp.IP != nil {
			host = udp.IP.String()
		}
		return host, sipAddrPort(addr)
	}
	if addr == nil {
		return "", 5060
	}
	host, portText, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String(), 5060
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 {
		port = 5060
	}
	return host, port
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	return &net.UDPAddr{
		IP:   append(net.IP(nil), addr.IP...),
		Port: addr.Port,
		Zone: addr.Zone,
	}
}

func cloneSIPHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for key, value := range headers {
		out[key] = value
	}
	return out
}

func sipContactURI(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if start := strings.Index(value, "<"); start >= 0 {
		if end := strings.Index(value[start+1:], ">"); end >= 0 {
			return strings.TrimSpace(value[start+1 : start+1+end])
		}
	}
	if semi := strings.Index(value, ";"); semi >= 0 {
		value = value[:semi]
	}
	return strings.Trim(value, "<> ")
}

func (s *Service) sipRequestLine(request sipRequest) (extensionConfig, bool) {
	selection := s.sipRequestLineSelection(request)
	return selection.Line, selection.Matched
}

func (s *Service) sipRequestLineSelection(request sipRequest) sipLineSelection {
	candidates := []string{
		request.URI,
		sipHeader(request.Headers, "to"),
		sipHeader(request.Headers, "p-called-party-id"),
		sipHeader(request.Headers, "x-haze-extension"),
		sipHeader(request.Headers, "diversion"),
	}
	var canadaLine extensionConfig
	canadaMatched := false
	canadaAddress := ""
	calledNumberAddress := ""
	for _, candidate := range candidates {
		user := sipURIUser(candidate)
		if user == "" {
			continue
		}
		if calledNumberAddress == "" && normalizeCalledNumber(user) != "" {
			calledNumberAddress = candidate
		}
		if line, matched := s.cfg.IVR.extensionLineByDID(user); matched {
			return sipLineSelection{Line: line, Matched: true, CalledAddress: candidate}
		}
		if line, matched := s.cfg.IVR.extensionLineByExtension(user); matched {
			if line.directProvince() != "" {
				return sipLineSelection{Line: line, Matched: true, CalledAddress: candidate}
			}
			if !canadaMatched {
				canadaLine = line
				canadaMatched = true
				canadaAddress = candidate
			}
		}
	}
	for _, candidate := range candidates {
		user := sipURIUser(candidate)
		if province := callerProvinceHint(normalizeCalledNumber(user)); province != "" {
			if line, matched := s.cfg.IVR.provinceLine(province); matched {
				return sipLineSelection{Line: line, Matched: true, CalledAddress: candidate}
			}
		}
	}
	if canadaMatched {
		if calledNumberAddress != "" {
			canadaAddress = calledNumberAddress
		}
		return sipLineSelection{Line: canadaLine, Matched: true, CalledAddress: canadaAddress}
	}
	line, matched := s.cfg.IVR.extensionLine("")
	calledAddress := calledNumberAddress
	if calledAddress == "" {
		calledAddress = request.URI
	}
	return sipLineSelection{Line: line, Matched: matched, CalledAddress: calledAddress}
}

func (s *Service) sipRequestDirectFeed(request sipRequest) sipDirectFeedTarget {
	candidate := sipURIUser(request.URI)
	if !validSIPDirectFeedID(candidate) {
		return sipDirectFeedTarget{}
	}
	for _, feed := range s.cfg.Feeds {
		feedID := strings.TrimSpace(feed.ID)
		if !strings.EqualFold(feedID, candidate) {
			continue
		}
		return sipDirectFeedTarget{
			ID:      feedID,
			Matched: true,
			Enabled: xmlBool(feed.EnabledRaw, true),
		}
	}
	return sipDirectFeedTarget{}
}

func validSIPDirectFeedID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func sipURIUser(value string) string {
	value = strings.TrimSpace(sipContactURI(value))
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	for _, scheme := range []string{"sip:", "sips:", "tel:"} {
		if strings.HasPrefix(lower, scheme) {
			value = value[len(scheme):]
			break
		}
	}
	if separator := strings.IndexAny(value, "@;?"); separator >= 0 {
		value = value[:separator]
	}
	return normalizeExtensionID(value)
}

func sipResponseExpires(response sipResponse, fallback int) int {
	if raw := sipHeader(response.Headers, "expires"); raw != "" {
		if value, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && value > 0 {
			return value
		}
	}
	return fallback
}

func sipRegisterAuthorization(challenge string, username string, password string, method string, uri string) (string, error) {
	params := parseDigestHeader(challenge)
	if len(params) == 0 {
		return "", fmt.Errorf("provider did not return a digest challenge")
	}
	realm := params["realm"]
	nonce := params["nonce"]
	if realm == "" || nonce == "" {
		return "", fmt.Errorf("provider digest challenge is missing realm or nonce")
	}
	qop := "auth"
	if offered := params["qop"]; offered != "" && !digestQOPIncludesAuth(offered) {
		qop = ""
	}
	cnonce := randomHex(8)
	nc := "00000001"
	ha1 := sipMD5Hex(username + ":" + realm + ":" + password)
	ha2 := sipMD5Hex(method + ":" + uri)
	response := ""
	if qop == "auth" {
		response = sipMD5Hex(strings.Join([]string{ha1, nonce, nc, cnonce, qop, ha2}, ":"))
	} else {
		response = sipMD5Hex(strings.Join([]string{ha1, nonce, ha2}, ":"))
	}
	parts := []string{
		fmt.Sprintf(`username="%s"`, escapeDigestValue(username)),
		fmt.Sprintf(`realm="%s"`, escapeDigestValue(realm)),
		fmt.Sprintf(`nonce="%s"`, escapeDigestValue(nonce)),
		fmt.Sprintf(`uri="%s"`, escapeDigestValue(uri)),
		fmt.Sprintf(`response="%s"`, response),
	}
	if algorithm := params["algorithm"]; algorithm != "" {
		parts = append(parts, fmt.Sprintf(`algorithm=%s`, algorithm))
	} else {
		parts = append(parts, "algorithm=MD5")
	}
	if opaque := params["opaque"]; opaque != "" {
		parts = append(parts, fmt.Sprintf(`opaque="%s"`, escapeDigestValue(opaque)))
	}
	if qop == "auth" {
		parts = append(parts, `qop=auth`, "nc="+nc, fmt.Sprintf(`cnonce="%s"`, cnonce))
	}
	return "Digest " + strings.Join(parts, ", "), nil
}

func sipDigestChallengeSummary(challenge string) string {
	params := parseDigestHeader(challenge)
	if len(params) == 0 {
		return "challenge=unparsed"
	}
	return fmt.Sprintf("realm=%q qop=%q algorithm=%q opaque=%t stale=%q",
		params["realm"],
		params["qop"],
		params["algorithm"],
		params["opaque"] != "",
		params["stale"],
	)
}

func sipResponseHeaderSummary(response sipResponse) string {
	parts := []string{}
	for _, key := range []string{"server", "warning", "reason", "x-reason", "www-authenticate", "proxy-authenticate"} {
		if value := sipHeader(response.Headers, key); value != "" {
			parts = append(parts, key+"="+strconv.Quote(value))
		}
	}
	if len(parts) == 0 {
		return "headers=none"
	}
	return strings.Join(parts, " ")
}

func digestQOPIncludesAuth(value string) bool {
	for _, part := range strings.Split(value, ",") {
		if strings.EqualFold(strings.Trim(strings.TrimSpace(part), `"`), "auth") {
			return true
		}
	}
	return false
}

func escapeDigestValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func parseSDPOffer(body string, remote *net.UDPAddr) (sipMediaOffer, error) {
	offer := sipMediaOffer{DTMFPayload: sipDefaultDTMFPayload, AudioPayload: -1, PCMUPayload: -1, G722Payload: -1}
	audioFormats := map[int]bool{}
	rtpmapCodecs := map[int]string{}
	if remote != nil {
		offer.Host = remote.IP.String()
	}
	for _, line := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "c=IN IP4 ") {
			offer.Host = strings.TrimSpace(strings.TrimPrefix(line, "c=IN IP4 "))
			continue
		}
		if strings.HasPrefix(line, "m=audio ") {
			fields := strings.Fields(strings.TrimPrefix(line, "m=audio "))
			if len(fields) > 0 {
				port, _ := strconv.Atoi(fields[0])
				offer.Port = port
			}
			if len(fields) > 2 {
				for _, format := range fields[2:] {
					payload, err := strconv.Atoi(format)
					if err != nil {
						continue
					}
					audioFormats[payload] = true
					switch payload {
					case sipPayloadPCMU:
						offer.HasPCMU = true
						offer.PCMUPayload = payload
					case sipPayloadG722:
						offer.HasG722 = true
						offer.G722Payload = payload
					}
				}
			}
			continue
		}
		lowerLine := strings.ToLower(line)
		if strings.HasPrefix(lowerLine, "a=rtpmap:") {
			payloadText := strings.TrimPrefix(lowerLine, "a=rtpmap:")
			fields := strings.Fields(payloadText)
			if len(fields) < 2 {
				continue
			}
			number, err := strconv.Atoi(fields[0])
			if err != nil {
				continue
			}
			codec := strings.ToLower(fields[1])
			rtpmapCodecs[number] = codec
			if strings.HasPrefix(codec, "telephone-event/8000") {
				if number > 0 {
					offer.DTMFPayload = number
				}
				continue
			}
			if !audioFormats[number] {
				continue
			}
			if strings.HasPrefix(codec, "pcmu/8000") {
				offer.HasPCMU = true
				offer.PCMUPayload = number
			} else if strings.HasPrefix(codec, "g722/8000") || strings.HasPrefix(codec, "g722/16000") {
				offer.HasG722 = true
				offer.G722Payload = number
			}
		}
	}
	for payload, codec := range rtpmapCodecs {
		if !audioFormats[payload] {
			continue
		}
		if strings.HasPrefix(codec, "g722/8000") || strings.HasPrefix(codec, "g722/16000") {
			offer.HasG722 = true
			offer.G722Payload = payload
			if offer.AudioPayload < 0 || payload == sipPayloadG722 {
				offer.AudioPayload = payload
			}
		} else if strings.HasPrefix(codec, "pcmu/8000") {
			offer.HasPCMU = true
			offer.PCMUPayload = payload
			if offer.AudioPayload < 0 || payload == sipPayloadPCMU {
				offer.AudioPayload = payload
			}
		}
	}
	if audioFormats[sipPayloadG722] {
		offer.HasG722 = true
		offer.G722Payload = sipPayloadG722
	}
	if offer.HasG722 {
		offer.AudioCodec = sipAudioCodecG722
		if offer.AudioPayload < 0 {
			offer.AudioPayload = fallbackInt(offer.G722Payload, sipPayloadG722)
		}
	} else if audioFormats[sipPayloadPCMU] || offer.HasPCMU {
		offer.HasPCMU = true
		if audioFormats[sipPayloadPCMU] {
			offer.PCMUPayload = sipPayloadPCMU
		}
		offer.AudioCodec = sipAudioCodecPCMU
		if offer.AudioPayload < 0 {
			offer.AudioPayload = fallbackInt(offer.PCMUPayload, sipPayloadPCMU)
		}
	}
	if net.ParseIP(offer.Host) == nil {
		return sipMediaOffer{}, fmt.Errorf("missing remote RTP host")
	}
	if offer.Port <= 0 || offer.Port > math.MaxUint16 {
		return sipMediaOffer{}, fmt.Errorf("missing remote RTP port")
	}
	if !offer.HasG722 && !offer.HasPCMU {
		return sipMediaOffer{}, fmt.Errorf("G722/8000 or PCMU/8000 was not offered")
	}
	return offer, nil
}

func (s *Service) sipSourceAllowed(remote *net.UDPAddr) bool {
	if len(s.cfg.IVR.SIP.AllowedSources) == 0 {
		return true
	}
	if remote == nil || remote.IP == nil {
		return false
	}
	for _, source := range s.cfg.IVR.SIP.AllowedSources {
		source = strings.TrimSpace(source)
		if source == "" {
			continue
		}
		if _, network, err := net.ParseCIDR(source); err == nil {
			if network.Contains(remote.IP) {
				return true
			}
			continue
		}
		if ip := net.ParseIP(source); ip != nil && ip.Equal(remote.IP) {
			return true
		}
	}
	return false
}

// sipSourceAllowedWithTrustedSources preserves explicit configured source
// entries and, when registration-source restriction is enabled, adds currently
// resolved registration-server addresses. With no explicit configured entry,
// an unavailable or empty dynamic set is fail-closed for that opt-in
// restriction.
func (s *Service) sipSourceAllowedWithTrustedSources(remote *net.UDPAddr, state *sipServerState) bool {
	if s == nil {
		return false
	}
	registration := s.cfg.IVR.SIP.Registration
	if !registration.Enabled || !registration.RestrictSourcesToServer {
		return s.sipSourceAllowed(remote)
	}
	if remote == nil {
		return false
	}
	if len(s.cfg.IVR.SIP.AllowedSources) > 0 && s.sipSourceAllowed(remote) {
		return true
	}
	key := sipTrustedSourceKey(remote.IP)
	if key == "" || state == nil {
		return false
	}
	loaded := state.trustedSources.Load()
	trusted, ok := loaded.(map[string]struct{})
	if !ok || len(trusted) == 0 {
		return false
	}
	_, allowed := trusted[key]
	return allowed
}

func (s *Service) sipDomainAllowed(request sipRequest, domain string) bool {
	domain = normalizeSIPDomain(domain)
	if domain == "" {
		return true
	}
	for _, value := range []string{request.URI, sipHeader(request.Headers, "to")} {
		host := normalizeSIPDomain(sipHostNameOnly(sipContactURI(value)))
		if host == domain {
			return true
		}
	}
	return false
}

func (s *Service) sipCanAcceptCall(activeCalls int) bool {
	maxCalls := s.cfg.IVR.MaxConcurrentCalls
	return maxCalls <= 0 || activeCalls < maxCalls
}

func (s *Service) sipAuthorizeInvite(request sipRequest, challenges map[string]time.Time) (bool, string, string) {
	if !s.cfg.IVR.SIP.Auth.Enabled {
		return true, "", ""
	}
	passwordEnv := strings.TrimSpace(s.cfg.IVR.SIP.Auth.PasswordEnv)
	password := ""
	if passwordEnv != "" {
		password = os.Getenv(passwordEnv)
	}
	if password == "" {
		return false, "503 Service Unavailable", "Warning: 399 haze \"SIP auth password is not configured\"\r\n"
	}
	pruneSIPChallenges(challenges, time.Now())
	if verifySIPDigest(request, sipAuthUsername(s.cfg.IVR.SIP.Auth.Username), sipAuthRealm(), password, challenges) {
		return true, "", ""
	}
	nonce := randomHex(16)
	challenges[nonce] = time.Now().Add(5 * time.Minute)
	extra := fmt.Sprintf("WWW-Authenticate: Digest realm=%q, nonce=%q, algorithm=MD5, qop=\"auth\"\r\n", sipAuthRealm(), nonce)
	return false, "401 Unauthorized", extra
}

func sipAuthUsername(configured string) string {
	return fallbackText(configured, "haze")
}

func sipAuthRealm() string {
	return serviceID
}

func pruneSIPChallenges(challenges map[string]time.Time, now time.Time) {
	for nonce, expiresAt := range challenges {
		if now.After(expiresAt) {
			delete(challenges, nonce)
		}
	}
}

func verifySIPDigest(request sipRequest, username string, realm string, password string, challenges map[string]time.Time) bool {
	params := parseDigestHeader(sipHeader(request.Headers, "authorization"))
	if len(params) == 0 {
		return false
	}
	nonce := params["nonce"]
	expiresAt, nonceOK := challenges[nonce]
	if !nonceOK || time.Now().After(expiresAt) {
		return false
	}
	if params["username"] != username || params["realm"] != realm {
		return false
	}
	uri := params["uri"]
	if uri == "" || request.URI == "" || uri != request.URI {
		return false
	}
	response := params["response"]
	if response == "" {
		return false
	}
	ha1 := sipMD5Hex(username + ":" + realm + ":" + password)
	ha2 := sipMD5Hex(request.Method + ":" + uri)
	expected := ""
	if qop := params["qop"]; qop != "" {
		nc := params["nc"]
		cnonce := params["cnonce"]
		if nc == "" || cnonce == "" || qop != "auth" {
			return false
		}
		expected = sipMD5Hex(strings.Join([]string{ha1, nonce, nc, cnonce, qop, ha2}, ":"))
	} else {
		expected = sipMD5Hex(strings.Join([]string{ha1, nonce, ha2}, ":"))
	}
	return subtle.ConstantTimeCompare([]byte(strings.ToLower(response)), []byte(expected)) == 1
}

func parseDigestHeader(header string) map[string]string {
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(strings.ToLower(header), "digest ") {
		return nil
	}
	header = strings.TrimSpace(header[len("Digest "):])
	out := map[string]string{}
	for len(header) > 0 {
		header = strings.TrimLeft(header, " \t,")
		keyEnd := strings.Index(header, "=")
		if keyEnd < 0 {
			break
		}
		key := strings.ToLower(strings.TrimSpace(header[:keyEnd]))
		header = strings.TrimLeft(header[keyEnd+1:], " \t")
		value := ""
		if strings.HasPrefix(header, `"`) {
			header = header[1:]
			var builder strings.Builder
			escaped := false
			for index, char := range header {
				if escaped {
					builder.WriteRune(char)
					escaped = false
					continue
				}
				if char == '\\' {
					escaped = true
					continue
				}
				if char == '"' {
					value = builder.String()
					header = header[index+1:]
					break
				}
				builder.WriteRune(char)
			}
			if value == "" && !strings.HasPrefix(header, ",") {
				value = builder.String()
				header = ""
			}
		} else {
			valueEnd := strings.Index(header, ",")
			if valueEnd < 0 {
				value = strings.TrimSpace(header)
				header = ""
			} else {
				value = strings.TrimSpace(header[:valueEnd])
				header = header[valueEnd:]
			}
		}
		if key != "" {
			out[key] = value
		}
	}
	return out
}

func sipMD5Hex(value string) string {
	sum := md5.Sum([]byte(value)) // #nosec G401 -- SIP Digest authentication is defined with MD5.
	return hex.EncodeToString(sum[:])
}

func listenRTPPort(host string, minPort int, maxPort int) (*net.UDPConn, int, error) {
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		ip = net.IPv4zero
	}
	if minPort <= 0 || maxPort < minPort {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip, Port: 0})
		if err != nil {
			return nil, 0, err
		}
		return conn, conn.LocalAddr().(*net.UDPAddr).Port, nil
	}
	for port := minPort; port <= maxPort; port++ {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip, Port: port})
		if err == nil {
			return conn, port, nil
		}
	}
	return nil, 0, fmt.Errorf("no free RTP port in %d-%d", minPort, maxPort)
}

func sipAnswerSDP(host string, port int, audioCodec sipAudioCodec, audioPayload int, dtmfPayload int) string {
	if audioPayload < 0 {
		audioPayload = int(audioCodec.defaultPayload())
	}
	return strings.Join([]string{
		"v=0",
		"o=haze 0 0 IN IP4 " + host,
		"s=Haze IVR",
		"c=IN IP4 " + host,
		"t=0 0",
		fmt.Sprintf("m=audio %d RTP/AVP %d %d", port, audioPayload, dtmfPayload),
		audioCodec.rtpmap(audioPayload),
		fmt.Sprintf("a=rtpmap:%d telephone-event/8000", dtmfPayload),
		fmt.Sprintf("a=fmtp:%d 0-15", dtmfPayload),
		"a=sendrecv",
		"",
	}, "\r\n")
}

func sipHeaders(lines []string) map[string]string {
	headers := map[string]string{}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			break
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if key == "via" && headers[key] != "" {
			headers[key] += "\n" + value
			continue
		}
		headers[key] = value
	}
	return headers
}

func sipHeader(headers map[string]string, key string) string {
	return headers[strings.ToLower(strings.TrimSpace(key))]
}

func sipReply(status string, headers map[string]string, extra string) string {
	return sipBuildReply(status, headers, "", "", "", "", 0, "", extra)
}

func sipReplyWithBody(status string, headers map[string]string, toTag string, contentType string, body string, contactHost string, contactPort int, contactUser string, extra string) string {
	return sipBuildReply(status, headers, toTag, contentType, body, contactHost, contactPort, contactUser, extra)
}

// sipReplyWithBodyTo applies the RFC 3581 response parameters requested by the
// topmost SIP proxy before constructing a final response. This keeps replies
// routable when the provider reached us through a UDP source port that differs
// from the Via sent-by address.
func sipReplyWithBodyTo(status string, headers map[string]string, toTag string, contentType string, body string, contactHost string, contactPort int, contactUser string, extra string, remote *net.UDPAddr) string {
	return sipBuildReply(status, sipResponseHeaders(headers, remote), toTag, contentType, body, contactHost, contactPort, contactUser, extra)
}

func sipResponseHeaders(headers map[string]string, remote *net.UDPAddr) map[string]string {
	result := cloneSIPHeaders(headers)
	if remote == nil || remote.IP == nil || remote.Port <= 0 {
		return result
	}
	vias := strings.Split(sipHeader(result, "via"), "\n")
	if len(vias) == 0 || strings.TrimSpace(vias[0]) == "" {
		return result
	}
	vias[0] = sipResponseVia(vias[0], remote)
	result["via"] = strings.Join(vias, "\n")
	return result
}

func sipResponseVia(via string, remote *net.UDPAddr) string {
	via = strings.TrimSpace(via)
	if via == "" || remote == nil || remote.IP == nil || remote.Port <= 0 {
		return via
	}
	remoteIP := remote.IP.String()
	if !sipViaHasParameter(via, "received") {
		via += ";received=" + remoteIP
	}
	if start, end, found := sipViaParameterBounds(via, "rport"); found {
		valueStart := start + len(";rport")
		if valueStart == end {
			via = via[:valueStart] + "=" + strconv.Itoa(remote.Port) + via[end:]
		}
	}
	return via
}

func sipViaHasParameter(via string, parameter string) bool {
	_, _, found := sipViaParameterBounds(via, parameter)
	return found
}

func sipViaParameterBounds(via string, parameter string) (int, int, bool) {
	lower := strings.ToLower(via)
	needle := ";" + strings.ToLower(strings.TrimSpace(parameter))
	for offset := 0; ; {
		index := strings.Index(lower[offset:], needle)
		if index < 0 {
			return 0, 0, false
		}
		start := offset + index
		end := start + len(needle)
		if end == len(via) || via[end] == '=' || via[end] == ';' {
			for end < len(via) && via[end] != ';' {
				end++
			}
			return start, end, true
		}
		offset = end
	}
}

func sipBuildReply(status string, headers map[string]string, toTag string, contentType string, body string, contactHost string, contactPort int, contactUser string, extra string) string {
	var builder strings.Builder
	builder.WriteString("SIP/2.0 " + status + "\r\n")
	for _, key := range []string{"via", "from", "to", "call-id", "cseq"} {
		if value := sipHeader(headers, key); value != "" {
			name := map[string]string{"call-id": "Call-ID", "cseq": "CSeq"}[key]
			if name == "" {
				name = strings.ToUpper(key[:1]) + key[1:]
			}
			if key == "via" {
				for _, via := range strings.Split(value, "\n") {
					if via = strings.TrimSpace(via); via != "" {
						builder.WriteString("Via: " + via + "\r\n")
					}
				}
				continue
			}
			if key == "to" && toTag != "" && !strings.Contains(strings.ToLower(value), "tag=") {
				value += ";tag=" + toTag
			}
			builder.WriteString(name + ": " + value + "\r\n")
		}
	}
	if contactHost != "" {
		contactUser = firstNonBlank(contactUser, "haze")
		if contactPort > 0 {
			builder.WriteString(fmt.Sprintf("Contact: <sip:%s@%s:%d>\r\n", contactUser, contactHost, contactPort))
		} else {
			builder.WriteString(fmt.Sprintf("Contact: <sip:%s@%s>\r\n", contactUser, contactHost))
		}
	}
	builder.WriteString("Allow: INVITE, ACK, BYE, CANCEL, OPTIONS, INFO\r\n")
	builder.WriteString(extra)
	if contentType != "" && body != "" {
		builder.WriteString("Content-Type: " + contentType + "\r\n")
		builder.WriteString("Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n")
		builder.WriteString(body)
		return builder.String()
	}
	builder.WriteString("Content-Length: 0\r\n\r\n")
	return builder.String()
}

func sipInfoDigit(body string) string {
	for _, line := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(key), "Signal") {
			return normalizeDTMFDigit(strings.TrimSpace(value))
		}
	}
	return ""
}

func rtpDTMFDigit(packet []byte, payloadType int) (string, string) {
	var decoded rtp.Packet
	if decoded.Unmarshal(packet) != nil || int(decoded.PayloadType) != payloadType || len(decoded.Payload) < 4 {
		return "", ""
	}
	event := int(decoded.Payload[0])
	digit := normalizeDTMFDigit(strconv.Itoa(event))
	if digit == "" {
		return "", ""
	}
	return digit, fmt.Sprintf("%d:%d", event, decoded.Timestamp)
}

func normalizeDTMFDigit(value string) string {
	value = strings.TrimSpace(value)
	switch value {
	case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9":
		return value
	case "10", "*":
		return "*"
	case "11", "#":
		return "#"
	default:
		return ""
	}
}

func randomHex(bytesCount int) string {
	buf := make([]byte, bytesCount)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", buf)
}

func randomUint32() uint32 {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint32(buf[:])
}

func escapeSIPWarning(value string) string {
	value = strings.ReplaceAll(value, `"`, `'`)
	return strings.ReplaceAll(value, "\r\n", " ")
}

func sipConnectedLineHeaders(name string, address string) string {
	name = sanitizeSIPDisplayName(name)
	uri := canonicalSIPIdentityURI(address)
	if name == "" || uri == "" {
		return ""
	}
	identity := `"` + escapeSIPQuotedString(name) + `" <` + uri + `>`
	return "P-Asserted-Identity: " + identity + "\r\n" +
		"Remote-Party-ID: " + identity + ";party=called;screen=yes;privacy=off\r\n"
}

func sanitizeSIPDisplayName(value string) string {
	var builder strings.Builder
	spacePending := false
	runes := 0
	for _, character := range strings.TrimSpace(value) {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			spacePending = builder.Len() > 0
			continue
		}
		if runes >= 80 {
			break
		}
		if spacePending {
			builder.WriteByte(' ')
			spacePending = false
		}
		builder.WriteRune(character)
		runes++
	}
	return builder.String()
}

func escapeSIPQuotedString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func canonicalSIPIdentityURI(address string) string {
	contact := sipContactURI(address)
	user := sipURIUser(contact)
	host := normalizeSIPDomain(sipHostNameOnly(contact))
	if !validSIPIdentityUser(user) || !validSIPIdentityHost(host) {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil && strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return "sip:" + user + "@" + host
}

func validSIPIdentityUser(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '+' || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validSIPIdentityHost(value string) bool {
	if value == "" || len(value) > 253 {
		return false
	}
	if net.ParseIP(value) != nil {
		return true
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character >= 'a' && character <= 'z' ||
				character >= '0' && character <= '9' || character == '-' {
				continue
			}
			return false
		}
	}
	return true
}
