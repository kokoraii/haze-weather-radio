package webgateway

import (
	"net/http"
	"strings"
)

const (
	archiveCAPXMLMaxIDLength     = 512
	archiveCAPXMLMaxFeedIDLength = 128
)

func (s *Server) alertsArchiveCAPXML(writer http.ResponseWriter, request *http.Request) {
	s.serveArchiveCAPXML(writer, request, true)
}

func (s *Server) publicAlertsArchiveCAPXML(writer http.ResponseWriter, request *http.Request) {
	if publicAlertsArchiveAccess(s.config) != "public" {
		http.NotFound(writer, request)
		return
	}
	s.serveArchiveCAPXML(writer, request, false)
}

func (s *Server) serveArchiveCAPXML(writer http.ResponseWriter, request *http.Request, requireAuth bool) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	if requireAuth {
		identity, ok := s.requireRequestIdentity(writer, request)
		if !ok {
			return
		}
		if s.auth.AccountMode() && !identity.Account.IsAdmin && !identity.Account.CanViewLogs {
			err := &AuthError{Code: "logs_forbidden", Detail: "This account is not allowed to view alert logs.", HTTPStatus: http.StatusForbidden}
			status, response := commandErrorResponse(err)
			response["type"] = "auth_error"
			writeJSONStatus(writer, status, response)
			return
		}
	}
	id := strings.TrimSpace(request.URL.Query().Get("id"))
	feedID := strings.TrimSpace(request.URL.Query().Get("feed_id"))
	if id == "" {
		http.Error(writer, "alert id is required", http.StatusBadRequest)
		return
	}
	if len(id) > archiveCAPXMLMaxIDLength || len(feedID) > archiveCAPXMLMaxFeedIDLength {
		http.Error(writer, "alert lookup is too long", http.StatusBadRequest)
		return
	}
	if feedID != "" && !validPublicAudioFeedID(feedID) {
		http.Error(writer, "feed_id is invalid", http.StatusBadRequest)
		return
	}
	record, ok := findArchiveAlert(s.configPath, id, feedID)
	if !ok {
		http.NotFound(writer, request)
		return
	}
	rawXML := strings.TrimSpace(firstNonBlank(record.RawXML, record.Alert.RawXML))
	if rawXML == "" {
		http.Error(writer, "CAP XML is not available for this alert", http.StatusNotFound)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/cap+xml; charset=utf-8")
	writer.Header().Set("Content-Disposition", `inline; filename="cap-`+safeID(id)+`.xml"`)
	if request.Method == http.MethodHead {
		return
	}
	_, _ = writer.Write([]byte(rawXML))
}
