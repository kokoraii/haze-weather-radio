package webgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const mediaServiceWebRTCOfferTimeout = 1500 * time.Millisecond

type mediaServiceWebRTCAnswerResult struct {
	Answer     map[string]any
	StatusCode int
	Detail     string
	Terminal   bool
}

func (r mediaServiceWebRTCAnswerResult) OK() bool {
	return strings.TrimSpace(stringValue(r.Answer, "sdp")) != ""
}

func clientIPForMediaRequest(request *http.Request) string {
	if request == nil {
		return ""
	}
	for _, raw := range []string{
		request.Header.Get("CF-Connecting-IP"),
		request.Header.Get("True-Client-IP"),
		strings.TrimSpace(strings.Split(request.Header.Get("X-Forwarded-For"), ",")[0]),
		request.Header.Get("X-Real-IP"),
		request.RemoteAddr,
	} {
		ip := strings.TrimSpace(raw)
		if host, _, err := net.SplitHostPort(ip); err == nil {
			ip = host
		}
		ip = strings.Trim(ip, "[]")
		parsed := net.ParseIP(ip)
		if parsed == nil || parsed.IsUnspecified() || parsed.IsLoopback() {
			continue
		}
		return parsed.String()
	}
	return ""
}

func (s *Server) mediaServiceWebRTCAnswer(ctx context.Context, payload map[string]any) (map[string]any, bool) {
	result := s.mediaServiceWebRTCAnswerResult(ctx, payload)
	return result.Answer, result.OK()
}

func (s *Server) mediaServiceWebRTCAnswerResult(ctx context.Context, payload map[string]any) mediaServiceWebRTCAnswerResult {
	return mediaServiceWebRTCAnswerResultWith(ctx, mediaServiceControl(s.config), payload)
}

// mediaServiceWebRTCAnswerResultWith asks the control client for an answer. A nil
// client means the Rust media service is disabled, which is an empty result.
func mediaServiceWebRTCAnswerResultWith(ctx context.Context, control mediaControl, payload map[string]any) mediaServiceWebRTCAnswerResult {
	if control == nil {
		return mediaServiceWebRTCAnswerResult{}
	}
	return control.WebRTCOffer(ctx, payload)
}

func mediaServiceWebRTCAnswerFromBase(ctx context.Context, baseURL string, payload map[string]any) (map[string]any, bool) {
	result := mediaServiceWebRTCAnswerResultFromBase(ctx, baseURL, payload)
	return result.Answer, result.OK()
}

func mediaServiceWebRTCAnswerResultFromBase(ctx context.Context, baseURL string, payload map[string]any) mediaServiceWebRTCAnswerResult {
	if baseURL == "" {
		return mediaServiceWebRTCAnswerResult{}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return mediaServiceWebRTCAnswerResult{StatusCode: http.StatusInternalServerError, Detail: err.Error(), Terminal: true}
	}
	offerCtx, cancel := context.WithTimeout(ctx, mediaServiceWebRTCOfferTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(offerCtx, http.MethodPost, baseURL+"/api/v1/webrtc/offer", bytes.NewReader(raw))
	if err != nil {
		return mediaServiceWebRTCAnswerResult{StatusCode: http.StatusInternalServerError, Detail: err.Error(), Terminal: true}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Printf("haze-media WebRTC unavailable: %v", err)
		return mediaServiceWebRTCAnswerResult{}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotImplemented || response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusServiceUnavailable {
		return mediaServiceWebRTCAnswerResult{StatusCode: response.StatusCode}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		log.Printf("haze-media WebRTC returned %s", response.Status)
		return mediaServiceWebRTCAnswerResult{
			StatusCode: response.StatusCode,
			Detail:     mediaServiceWebRTCErrorDetail(response),
			Terminal:   true,
		}
	}
	var answer map[string]any
	if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
		log.Printf("haze-media WebRTC answer was invalid: %v", err)
		return mediaServiceWebRTCAnswerResult{StatusCode: response.StatusCode}
	}
	if strings.TrimSpace(stringValue(answer, "sdp")) == "" {
		return mediaServiceWebRTCAnswerResult{StatusCode: response.StatusCode}
	}
	return mediaServiceWebRTCAnswerResult{Answer: answer, StatusCode: response.StatusCode}
}

func mediaServiceWebRTCErrorDetail(response *http.Response) string {
	detail := response.Status
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return detail
	}
	body := strings.TrimSpace(string(raw))
	if body == "" {
		return detail
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err == nil {
		detail = firstNonBlank(stringValue(payload, "error"), stringValue(payload, "detail"), detail)
	} else {
		detail = body
	}
	return detail
}

func mediaServiceWebRTCAvailable(ctx context.Context, baseURL string) (bool, bool) {
	health, ok := mediaServiceHealthFromBaseURL(ctx, baseURL)
	if !ok {
		return false, false
	}
	capabilities, _ := health["capabilities"].(map[string]any)
	value, ok := capabilities["webrtc"].(bool)
	return value, ok
}

func mediaServiceHealth(ctx context.Context, config Config) (map[string]any, bool) {
	control := mediaServiceControl(config)
	if control == nil {
		return nil, false
	}
	return control.Health(ctx)
}

func mediaServiceHealthFromBaseURL(ctx context.Context, baseURL string) (map[string]any, bool) {
	checkCtx, cancel := context.WithTimeout(ctx, mediaHealthTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(checkCtx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/health", nil)
	if err != nil {
		return nil, false
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, false
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, false
	}
	var health map[string]any
	if err := json.NewDecoder(response.Body).Decode(&health); err != nil {
		return nil, false
	}
	return health, true
}
