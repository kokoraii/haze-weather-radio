package capmodel

// Alert is a normalized CAP alert payload consumed from Haze CAP events.
type Alert struct {
	Identifier  string      `json:"identifier"`
	Sender      string      `json:"sender,omitempty"`
	Sent        string      `json:"sent,omitempty"`
	Status      string      `json:"status,omitempty"`
	MessageType string      `json:"message_type,omitempty"`
	Scope       string      `json:"scope,omitempty"`
	Note        string      `json:"note,omitempty"`
	Code        []string    `json:"code,omitempty"`
	References  string      `json:"references,omitempty"`
	Incidents   string      `json:"incidents,omitempty"`
	Infos       []AlertInfo `json:"infos,omitempty"`
	RawXML      string      `json:"raw_xml,omitempty"`
	Warnings    []string    `json:"warnings,omitempty"`
}

// AlertInfo contains the public-safety fields Haze policy services need.
type AlertInfo struct {
	Language    string      `json:"language,omitempty"`
	Category    []string    `json:"category,omitempty"`
	Event       string      `json:"event,omitempty"`
	Response    []string    `json:"response_type,omitempty"`
	Urgency     string      `json:"urgency,omitempty"`
	Severity    string      `json:"severity,omitempty"`
	Certainty   string      `json:"certainty,omitempty"`
	Audience    string      `json:"audience,omitempty"`
	Effective   string      `json:"effective,omitempty"`
	Onset       string      `json:"onset,omitempty"`
	Expires     string      `json:"expires,omitempty"`
	SenderName  string      `json:"sender_name,omitempty"`
	Headline    string      `json:"headline,omitempty"`
	Description string      `json:"description,omitempty"`
	Instruction string      `json:"instruction,omitempty"`
	Web         string      `json:"web,omitempty"`
	EventCodes  []NameValue `json:"event_codes,omitempty"`
	Areas       []AlertArea `json:"areas,omitempty"`
	Parameters  []NameValue `json:"parameters,omitempty"`
	Resources   []Resource  `json:"resources,omitempty"`
	Storm       *StormInfo  `json:"storm,omitempty"`
}

// AlertArea captures CAP area metadata.
type AlertArea struct {
	Description  string      `json:"description,omitempty"`
	Polygons     []string    `json:"polygons,omitempty"`
	Circles      []string    `json:"circles,omitempty"`
	Geocodes     []NameValue `json:"geocodes,omitempty"`
	ThreatStatus string      `json:"threat_status,omitempty"`
}

// StormInfo contains ECCC's August 2026 storm-specific CAP parameters. Raw
// values remain available in AlertInfo.Parameters.
type StormInfo struct {
	Speed                   string     `json:"speed,omitempty"`
	SpeedValue              *float64   `json:"speed_value,omitempty"`
	SpeedUnit               string     `json:"speed_unit,omitempty"`
	DirectionDegrees        *float64   `json:"direction_degrees,omitempty"`
	GeometryType            string     `json:"geometry_type,omitempty"`
	Points                  []GeoPoint `json:"points,omitempty"`
	Time                    string     `json:"time,omitempty"`
	MotionDescription       string     `json:"motion_description,omitempty"`
	PositionDescription     string     `json:"position_description,omitempty"`
	ReferenceLocationPoints string     `json:"reference_location_points,omitempty"`
}

// GeoPoint is a CAP latitude/longitude coordinate.
type GeoPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// NameValue is a generic CAP name/value pair.
type NameValue struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Resource represents a CAP resource block.
type Resource struct {
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mime_type,omitempty"`
	URI         string `json:"uri,omitempty"`
	DerefURI    string `json:"deref_uri,omitempty"`
}
