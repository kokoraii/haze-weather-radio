package ivr

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"strings"
)

func (s *Service) writeTwilioSearchConnect(writer http.ResponseWriter, request *http.Request, language string, region string) {
	if s == nil || s.twilio == nil || s.searchIndex == nil {
		s.writeUnavailableTwiML(writer, request)
		return
	}
	accountSID := strings.TrimSpace(request.FormValue("AccountSid"))
	callSID := strings.TrimSpace(request.FormValue("CallSid"))
	language = fallbackText(language, s.cfg.IVR.DefaultLanguage)
	region = searchRegionFromSelector(region)
	caller := ""
	if s.cfg.IVR.Search.CallerHintEnabled {
		caller = request.FormValue("From")
	}
	session, err := s.twilio.newPending(accountSID, callSID, language, region, caller)
	if err != nil {
		http.Error(writer, "unable to create Twilio search session", http.StatusBadRequest)
		return
	}
	streamURL := s.twilio.publicWSURL("/ivr/v1/twilio/media")
	recoveryURL := s.twilio.publicHTTPURL("/ivr/v1/twilio/recover/"+session.Token, "")
	body := `<Connect action="` + html.EscapeString(recoveryURL) + `" method="POST"><Stream url="` + html.EscapeString(streamURL) + `" name="haze-location-search"><Parameter name="nonce" value="` + html.EscapeString(session.Token) + `"/></Stream></Connect>`
	writeTwiML(writer, body)
}

func (s *Service) handleTwilioRecovery(writer http.ResponseWriter, request *http.Request) {
	if s == nil || s.twilio == nil {
		http.Error(writer, "Twilio IVR is unavailable", http.StatusServiceUnavailable)
		return
	}
	token := strings.TrimPrefix(request.URL.Path, "/ivr/v1/twilio/recover/")
	session, ok := s.twilio.recoverySession(token, request.FormValue("AccountSid"), request.FormValue("CallSid"))
	if !ok {
		http.Error(writer, "invalid or expired Twilio search session", http.StatusForbidden)
		return
	}
	params := map[string]string{
		"state":    "location_number",
		"lang":     session.Language,
		"province": session.Region,
	}
	target := twimlURL(request, "/ivr/v1/twiml", params)
	writeTwiML(writer, twimlRedirect(target))
}

func (s *Service) handleTwilioResult(writer http.ResponseWriter, request *http.Request) {
	if s == nil || s.twilio == nil {
		http.Error(writer, "Twilio IVR is unavailable", http.StatusServiceUnavailable)
		return
	}
	token := strings.TrimPrefix(request.URL.Path, "/ivr/v1/twilio/result/")
	result, ok := s.twilio.consumeResult(token, request.FormValue("AccountSid"), request.FormValue("CallSid"))
	if !ok {
		http.Error(writer, "invalid or expired Twilio location result", http.StatusForbidden)
		return
	}
	mediaToken, err := s.twilio.createMediaGrant(result)
	if err != nil {
		http.Error(writer, "unable to authorize Twilio media", http.StatusServiceUnavailable)
		return
	}
	request = s.externalTwilioRequest(request, mediaToken)
	s.writeLocationMenu(writer, request, result.Location)
}

func (s *Service) externalTwilioRequest(request *http.Request, mediaToken string) *http.Request {
	base := s.twilio.baseURL
	clone := request.Clone(context.WithValue(request.Context(), twilioRequestContextKey{}, twilioRequestContext{
		MediaToken: mediaToken,
		BaseURL:    strings.TrimRight(base.String(), "/"),
	}))
	clone.URL.Scheme = base.Scheme
	clone.URL.Host = base.Host
	clone.Host = base.Host
	clone.Header = clone.Header.Clone()
	clone.Header.Del("Forwarded")
	clone.Header.Del("X-Forwarded-Host")
	clone.Header.Del("X-Forwarded-Proto")
	clone.Header.Del("X-Forwarded-Port")
	return clone
}

func (s *Service) twilioLocationRequest(request *http.Request, location ResolvedLocation) *http.Request {
	if s == nil || s.twilio == nil || request == nil {
		return request
	}
	requestContext, ok := request.Context().Value(twilioRequestContextKey{}).(twilioRequestContext)
	if !ok || requestContext.MediaToken == "" {
		return request
	}
	grant, ok := s.twilio.mediaGrant(requestContext.MediaToken)
	if !ok {
		return request
	}
	if strings.EqualFold(grant.Location.Source, location.Source) && strings.EqualFold(grant.Location.Code, location.Code) {
		return request
	}
	token, err := s.twilio.createBoundMediaGrant(grant.AccountSID, grant.CallSID, location)
	if err != nil {
		return request
	}
	return s.externalTwilioRequest(request, token)
}

func (s *Service) twilioResultURL(token string) string {
	if s == nil || s.twilio == nil {
		return ""
	}
	return s.twilio.publicHTTPURL(fmt.Sprintf("/ivr/v1/twilio/result/%s", token), "")
}
