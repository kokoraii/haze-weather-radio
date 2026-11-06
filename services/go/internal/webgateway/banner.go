package webgateway

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Banner alert state now comes from haze-netalert over /api/v1/netalert. The
// banner page still listens to its feed over WebRTC, which stays here with the
// rest of the media plane.

func (s *Server) bannerWebRTCOffer(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	legacyMediaAvailable := s.media != nil && s.media.Available()
	if !legacyMediaAvailable && mediaServiceBaseURL(s.config) == "" {
		http.Error(writer, "media bridge is not available", http.StatusServiceUnavailable)
		return
	}
	var payload struct {
		FeedID         string `json:"feed_id"`
		SDP            string `json:"sdp"`
		DisableG722    bool   `json:"disable_g722"`
		RequireOpus    bool   `json:"require_opus"`
		Codec          string `json:"codec"`
		PreferredCodec string `json:"preferred_codec"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		http.Error(writer, "invalid JSON", http.StatusBadRequest)
		return
	}
	feedID := strings.TrimSpace(payload.FeedID)
	if feedID == "" {
		feedID = strings.TrimSpace(request.URL.Query().Get("feed"))
	}
	if feedID == "" {
		http.Error(writer, "feed_id is required", http.StatusBadRequest)
		return
	}
	if !s.feedWebRTCEnabled(feedID) {
		http.Error(writer, "feed WebRTC output is not enabled", http.StatusForbidden)
		return
	}
	mediaResult := s.mediaServiceWebRTCAnswerResult(request.Context(), map[string]any{
		"feed_id":         feedID,
		"sdp":             payload.SDP,
		"disable_g722":    payload.DisableG722,
		"require_opus":    payload.RequireOpus,
		"preferred_codec": firstNonBlank(payload.Codec, payload.PreferredCodec),
		"client_ip":       clientIPForMediaRequest(request),
		"remote_addr":     request.RemoteAddr,
	})
	if mediaResult.OK() {
		answer := mediaResult.Answer
		answer["feed_id"] = feedID
		if _, ok := answer["sdp_type"]; !ok {
			answer["sdp_type"] = "answer"
		}
		writeJSON(writer, answer)
		return
	}
	if mediaResult.Terminal {
		status := mediaResult.StatusCode
		if status < 400 {
			status = http.StatusBadGateway
		}
		http.Error(writer, firstNonBlank(mediaResult.Detail, "haze-media WebRTC offer was rejected"), status)
		return
	}
	if !legacyMediaAvailable {
		if mediaServiceBaseURL(s.config) != "" {
			http.Error(writer, "haze-media WebRTC service is unavailable and legacy media bridge is not available", http.StatusServiceUnavailable)
			return
		}
		http.Error(writer, "media bridge is not available", http.StatusServiceUnavailable)
		return
	}
	release, ok := s.acquireFeedListener(writer, request, feedID)
	if !ok {
		return
	}
	answer, err := s.media.AnswerWithOptions(request.Context(), feedID, payload.SDP, WebRTCAnswerOptions{
		DisableG722:    payload.DisableG722,
		RequireOpus:    payload.RequireOpus,
		PreferredCodec: firstNonBlank(payload.Codec, payload.PreferredCodec),
		OnClose:        release,
	})
	if err != nil {
		release()
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(writer, map[string]any{
		"feed_id":      feedID,
		"sdp":          answer.SDP,
		"sdp_type":     "answer",
		"media_recent": answer.MediaRecent,
		"codec":        answer.Codec.String(),
		"payload_type": answer.PayloadType,
	})
}

func (s *Server) feedWebRTCEnabled(feedID string) bool {
	return feedAudioOutputEnabled(s.configPath, feedID)
}

func feedAudioOutputEnabled(configPath string, feedID string) bool {
	feedID = strings.TrimSpace(feedID)
	if feedID == "" {
		return false
	}
	feeds, err := loadBasicFeedSummaries(configPath)
	if err != nil {
		return false
	}
	for _, feed := range feeds {
		if stringValue(feed, "id") != feedID {
			continue
		}
		enabled, _ := feed["enabled"].(bool)
		webrtc, _ := feed["webrtc_enabled"].(bool)
		return enabled && webrtc
	}
	return false
}
