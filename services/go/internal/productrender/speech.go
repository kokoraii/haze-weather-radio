package productrender

import (
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
)

// Alert speech requests let feed-less consumers such as haze-netalert network
// clients voice a CAP alert with the same phrasing playout uses, without
// keeping a second copy of the alert text rules.
//
// Request data: request_id, alert (a cap.alert.received alert object with
// raw_xml), language (optional, picks the info block), timezone (optional).
// Replies: alert.speech.completed {request_id, text, language} or
// alert.speech.failed {request_id, error}.
const (
	alertSpeechRequestEvent   = "alert.speech.request"
	alertSpeechCompletedEvent = "alert.speech.completed"
	alertSpeechFailedEvent    = "alert.speech.failed"
)

func (s *Service) handleAlertSpeech(event map[string]any) {
	data := mapAt(event, "data")
	requestID := firstText(data, event, "request_id", "subject")
	if requestID == "" {
		return
	}
	alert, ok := capAlertFromEvent(event)
	if !ok {
		s.publishAlertSpeechFailed(requestID, "a CAP alert with raw XML is required")
		return
	}
	info := chooseAlertInfo(alert, firstText(nil, data, "language"))
	if info == nil {
		s.publishAlertSpeechFailed(requestID, "CAP alert has no info block")
		return
	}
	text := alerttext.BuildCAPAlertText(alerttext.CAPMessageRequest{
		Alert:     alert,
		Info:      *info,
		AreaText:  alerttext.JoinParts(alerttext.AreaNames(*info)),
		Timezone:  fallbackText(firstText(nil, data, "timezone"), "Local"),
		Now:       time.Now().UTC(),
		EventName: alerttext.AlertSubject(*info),
	})
	_ = s.bridge.Publish(map[string]any{
		"type":    alertSpeechCompletedEvent,
		"source":  serviceID,
		"subject": requestID,
		"data": map[string]any{
			"request_id": requestID,
			"text":       text,
			"language":   info.Language,
		},
	})
}

func (s *Service) publishAlertSpeechFailed(requestID string, detail string) {
	_ = s.bridge.Publish(map[string]any{
		"type":    alertSpeechFailedEvent,
		"source":  serviceID,
		"subject": requestID,
		"data": map[string]any{
			"request_id": requestID,
			"error":      detail,
		},
	})
}
