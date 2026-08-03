package webgateway

import (
	"fmt"
	"strings"
	"time"
)

func (s *wsSession) auditOrigination(identity Identity, command string, event string, payload map[string]any, result map[string]any) error {
	if s == nil || s.auth == nil || s.auth.accounts == nil || s.auth.accounts.audit == nil || !identity.Account.LoggingEnabled {
		return nil
	}
	details := map[string]any{
		"command":         command,
		"event_type":      strings.ToUpper(firstNonBlank(stringValue(payload, "same_event"), stringValue(payload, "event"), stringValue(payload, "event_code"))),
		"originator":      strings.ToUpper(firstNonBlank(stringValue(payload, "originator"), stringValue(payload, "same_originator"))),
		"sender_id":       firstNonBlank(stringValue(payload, "sender_id"), stringValue(payload, "same_callsign")),
		"originator_name": firstNonBlank(stringValue(payload, "originator_name"), stringValue(payload, "same_originator_name")),
		"locations":       payload["locations"],
		"feed_id":         payload["feed_id"],
		"feed_ids":        payload["feed_ids"],
		"alert_id":        payload["alert_id"],
	}
	if result != nil {
		for _, key := range []string{"header", "queue_id", "alert_id", "feed_id", "feeds_aired", "feed_ids", "scheduled", "schedule_at", "event_type", "originator", "sender_id"} {
			if value, ok := result[key]; ok {
				details[key] = value
			}
		}
		if value, ok := result["error"]; ok {
			details["error"] = value
		}
	}
	if err := s.auth.accounts.audit.Append("alerts", AuditEvent{
		Timestamp: time.Now().UTC(), Event: event,
		ActorID: identity.Account.ID, ActorUsername: identity.Account.Username,
		SessionID: identity.Session.ID, IP: identity.Session.IP, UserAgent: identity.Session.UserAgent,
		Severity: auditSeverity(event), Details: details,
	}); err != nil {
		return fmt.Errorf("alert audit integrity failure: %w", err)
	}
	return nil
}

func auditSeverity(event string) string {
	if strings.Contains(strings.ToUpper(event), "FAILED") {
		return "error"
	}
	return "info"
}

func auditedWebpanelMutation(command string, payload map[string]any) bool {
	if accountCommandName(command) || isOriginationExecution(command, payload) {
		return false
	}
	switch command {
	case "daemon.settings.save", "daemon.service.control", "dictionary.save", "tts.save",
		"feeds.save", "feeds.control", "bulletins.save", "bulletins.import", "bulletins.upload_audio",
		"cgen.scenes.save", "cgen.scenes.delete", "cgen.save", "cgen.action",
		"playlist.control", "playlist.insert", "alerts.archive.action", "automations.put", "same.templates.put",
		"operator_breakin.upload_preroll", "operator_breakin.generate_tone", "operator_breakin.start",
		"operator_breakin.chunk", "operator_breakin.finish", "operator_breakin.url", "operator_breakin.cancel":
		return true
	default:
		return false
	}
}

func (s *wsSession) auditGenericWebpanelMutation(identity Identity, command string, phase string, payload map[string]any) error {
	details := map[string]any{"command": command, "payload": sanitizedAuditMap(payload, 0)}
	if action := strings.TrimSpace(stringValue(payload, "action")); action != "" {
		details["action"] = action
	}
	return s.auditWebpanel(identity, "WEBPANEL_MUTATION_"+phase, identity.Account, details)
}

func sanitizedAuditMap(source map[string]any, depth int) map[string]any {
	if source == nil || depth > 4 {
		return map[string]any{}
	}
	out := make(map[string]any, len(source))
	for key, value := range source {
		out[key] = sanitizedAuditValue(key, value, depth+1)
	}
	return out
}

func sanitizedAuditValue(key string, value any, depth int) any {
	lowerKey := strings.ToLower(strings.TrimSpace(key))
	for _, marker := range []string{"password", "secret", "token", "authorization", "cookie", "credential", "private_key", "api_key", "apikey", "dsn"} {
		if strings.Contains(lowerKey, marker) {
			return "[REDACTED]"
		}
	}
	if strings.Contains(lowerKey, "audio_base64") || strings.Contains(lowerKey, "audio_bytes") || strings.Contains(lowerKey, "cap_xml") {
		return "[OMITTED BINARY OR CAP CONTENT]"
	}
	if depth > 4 {
		return "[MAX DEPTH]"
	}
	switch typed := value.(type) {
	case map[string]any:
		return sanitizedAuditMap(typed, depth)
	case []any:
		limit := len(typed)
		if limit > 50 {
			limit = 50
		}
		out := make([]any, 0, limit)
		for index := 0; index < limit; index++ {
			out = append(out, sanitizedAuditValue(key, typed[index], depth+1))
		}
		return out
	case []string:
		if len(typed) > 50 {
			return append([]string{}, typed[:50]...)
		}
		return append([]string{}, typed...)
	case string:
		if len(typed) > 512 {
			return typed[:512] + " [TRUNCATED]"
		}
		return typed
	default:
		return value
	}
}
