// Package locationclient provides the shared broker request client for haze-location.
package locationclient

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const APIVersion = 1

type Mode string

const (
	ModeLegacy        Mode = "legacy"
	ModeShadow        Mode = "shadow"
	ModeAuthoritative Mode = "authoritative"
)

func RolloutMode() Mode {
	return ParseMode(os.Getenv("HAZE_LOCATION_MODE"))
}

func ParseMode(value string) Mode {
	switch Mode(strings.ToLower(strings.TrimSpace(value))) {
	case ModeShadow:
		return ModeShadow
	case ModeAuthoritative:
		return ModeAuthoritative
	default:
		return ModeLegacy
	}
}

type Input struct {
	Kind      string   `json:"kind"`
	Scheme    string   `json:"scheme,omitempty"`
	Authority string   `json:"authority,omitempty"`
	Value     string   `json:"value,omitempty"`
	Text      string   `json:"text,omitempty"`
	ID        string   `json:"id,omitempty"`
	Latitude  *float64 `json:"latitude,omitempty"`
	Longitude *float64 `json:"longitude,omitempty"`
}

type Filters struct {
	Kinds             []string `json:"kinds,omitempty"`
	Capabilities      []string `json:"capabilities,omitempty"`
	Country           string   `json:"country,omitempty"`
	Region            string   `json:"region,omitempty"`
	Roles             []string `json:"roles,omitempty"`
	RelationshipTypes []string `json:"relationship_types,omitempty"`
}

type Options struct {
	Limit                  int             `json:"limit,omitempty"`
	MaxDistanceKM          *float64        `json:"max_distance_km,omitempty"`
	MinimumConfidence      string          `json:"minimum_confidence,omitempty"`
	IncludeInactive        bool            `json:"include_inactive,omitempty"`
	DedupeMode             string          `json:"dedupe_mode,omitempty"`
	ExpandMembers          bool            `json:"expand_members,omitempty"`
	StationModePreference  *string         `json:"station_mode_preference,omitempty"`
	StationModeRequirement string          `json:"station_mode_requirement,omitempty"`
	Locale                 string          `json:"locale,omitempty"`
	GeographicBias         *GeographicBias `json:"geographic_bias,omitempty"`
	AsOf                   string          `json:"as_of,omitempty"`
	InputMode              string          `json:"input_mode,omitempty"`
	MaxDepth               int             `json:"max_depth,omitempty"`
	MaxVisited             int             `json:"max_visited,omitempty"`
	IncludeAreaGeometry    bool            `json:"include_area_geometry,omitempty"`
}

type GeographicBias struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Request struct {
	APIVersion int     `json:"api_version"`
	RequestID  string  `json:"request_id"`
	Operation  string  `json:"operation"`
	Input      *Input  `json:"input,omitempty"`
	Inputs     []Input `json:"inputs,omitempty"`
	Filters    Filters `json:"filters"`
	Options    Options `json:"options"`
}

type Identifier struct {
	Authority       string `json:"authority"`
	Scheme          string `json:"scheme"`
	Value           string `json:"value"`
	NormalizedValue string `json:"normalized_value"`
	Primary         bool   `json:"primary"`
	Confidence      string `json:"confidence"`
	SourceID        string `json:"source_id,omitempty"`
}

type Name struct {
	Locale          string `json:"locale,omitempty"`
	Value           string `json:"value"`
	NormalizedValue string `json:"normalized_value"`
	Kind            string `json:"name_kind"`
	Primary         bool   `json:"primary"`
	SourceID        string `json:"source_id,omitempty"`
}

type Geometry struct {
	Type      string     `json:"geometry_type"`
	Latitude  *float64   `json:"latitude,omitempty"`
	Longitude *float64   `json:"longitude,omitempty"`
	BBox      [4]float64 `json:"bbox"`
	AccuracyM *float64   `json:"accuracy_m,omitempty"`
	SourceID  string     `json:"source_id,omitempty"`
}

type Deployment struct {
	ProviderDeploymentID string         `json:"provider_deployment_id,omitempty"`
	Owner                string         `json:"owner,omitempty"`
	PlatformType         string         `json:"platform_type,omitempty"`
	Latitude             *float64       `json:"latitude,omitempty"`
	Longitude            *float64       `json:"longitude,omitempty"`
	ElevationM           *float64       `json:"elevation_m,omitempty"`
	ValidFrom            string         `json:"valid_from,omitempty"`
	ValidTo              string         `json:"valid_to,omitempty"`
	ReportingStatus      string         `json:"reporting_status"`
	SourceID             string         `json:"source_id,omitempty"`
	Attributes           map[string]any `json:"attributes,omitempty"`
}

type Entity struct {
	ID              string         `json:"id"`
	Kind            string         `json:"kind"`
	Capabilities    []string       `json:"capabilities,omitempty"`
	Country         string         `json:"country,omitempty"`
	Region          string         `json:"region,omitempty"`
	LifecycleStatus string         `json:"lifecycle_status"`
	ReportingStatus string         `json:"reporting_status"`
	SourceQuality   float64        `json:"source_quality"`
	Identifiers     []Identifier   `json:"identifiers,omitempty"`
	Names           []Name         `json:"names,omitempty"`
	Geometry        *Geometry      `json:"geometry,omitempty"`
	Deployments     []Deployment   `json:"deployments,omitempty"`
	Attributes      map[string]any `json:"attributes,omitempty"`
}

func (entity Entity) DisplayName() string {
	for _, name := range entity.Names {
		if name.Primary {
			return name.Value
		}
	}
	if len(entity.Names) > 0 {
		return entity.Names[0].Value
	}
	return entity.ID
}

func (entity Entity) Identifier(schemes ...string) string {
	for _, primary := range []bool{true, false} {
		for _, identifier := range entity.Identifiers {
			if identifier.Primary != primary {
				continue
			}
			for _, scheme := range schemes {
				if strings.EqualFold(identifier.Scheme, scheme) {
					return identifier.Value
				}
			}
		}
	}
	return ""
}

type Match struct {
	Score      float64        `json:"score"`
	Confidence string         `json:"confidence"`
	Method     string         `json:"method"`
	Algorithm  string         `json:"algorithm"`
	Evidence   map[string]any `json:"evidence,omitempty"`
}

type Grouping struct {
	GroupID          string         `json:"group_id"`
	RepresentativeID string         `json:"representative_id"`
	Mode             string         `json:"mode"`
	Algorithm        string         `json:"algorithm"`
	Confidence       float64        `json:"confidence"`
	MemberIDs        []string       `json:"member_ids"`
	MemberCount      int            `json:"member_count"`
	Members          []Entity       `json:"members,omitempty"`
	Evidence         map[string]any `json:"evidence,omitempty"`
}

type RelationshipStep struct {
	FromID           string  `json:"from_id"`
	ToID             string  `json:"to_id"`
	RelationshipType string  `json:"relationship_type"`
	Confidence       string  `json:"confidence"`
	Score            float64 `json:"score"`
	Method           string  `json:"method"`
}

type Candidate struct {
	Entity   Entity             `json:"entity"`
	Match    Match              `json:"match"`
	Facet    string             `json:"facet,omitempty"`
	Distance *float64           `json:"distance_m,omitempty"`
	Path     []RelationshipStep `json:"relationship_path,omitempty"`
	Grouping *Grouping          `json:"grouping,omitempty"`
}

type BatchResult struct {
	InputIndex int         `json:"input_index"`
	Status     string      `json:"status"`
	Results    []Candidate `json:"results"`
}

type Response struct {
	APIVersion        int           `json:"api_version"`
	RequestID         string        `json:"request_id"`
	Operation         string        `json:"operation"`
	Status            string        `json:"status"`
	Ambiguous         bool          `json:"ambiguous"`
	ScoreMargin       *float64      `json:"score_margin,omitempty"`
	CatalogGeneration string        `json:"catalog_generation"`
	CatalogPacks      []string      `json:"catalog_packs"`
	Truncated         bool          `json:"truncated"`
	Results           []Candidate   `json:"results,omitempty"`
	Batches           []BatchResult `json:"batches,omitempty"`
}

type Failure struct {
	APIVersion int    `json:"api_version"`
	RequestID  string `json:"request_id"`
	Code       string `json:"code"`
	ErrorText  string `json:"error"`
	Retryable  bool   `json:"retryable"`
}

func (failure Failure) Error() string {
	if failure.ErrorText != "" {
		return failure.ErrorText
	}
	return failure.Code
}

type Client struct {
	Address  string
	ClientID string
	Timeout  time.Duration
}

func New(address, clientPrefix string) *Client {
	return &Client{
		Address:  strings.TrimSpace(address),
		ClientID: uniqueID(clientPrefix),
		Timeout:  5 * time.Second,
	}
}

func (client *Client) Query(ctx context.Context, request Request) (Response, error) {
	if client == nil || client.Address == "" {
		return Response{}, fmt.Errorf("location service bridge is unavailable")
	}
	if request.APIVersion == 0 {
		request.APIVersion = APIVersion
	}
	if request.RequestID == "" {
		request.RequestID = uniqueID("location-query")
	}
	dialer := net.Dialer{Timeout: client.timeout()}
	connection, err := dialer.DialContext(ctx, "tcp", client.Address)
	if err != nil {
		return Response{}, fmt.Errorf("location bridge connect failed: %w", err)
	}
	defer connection.Close()
	deadline := time.Now().Add(client.timeout())
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	_ = connection.SetDeadline(deadline)
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = connection.SetDeadline(time.Now())
	})
	defer stopCancellation()
	encoder := json.NewEncoder(connection)
	if err := encoder.Encode(map[string]any{
		"type": "bridge.client",
		"data": map[string]any{
			"client_id":      client.ClientID,
			"receive_events": true,
			"subscriptions":  []string{"location.query.completed", "location.query.failed"},
		},
	}); err != nil {
		return Response{}, err
	}
	if err := encoder.Encode(map[string]any{
		"type":     "location.query.request",
		"source":   client.ClientID,
		"reply_to": client.ClientID,
		"target":   "haze-location",
		"subject":  request.RequestID,
		"data":     request,
	}); err != nil {
		return Response{}, err
	}
	scanner := bufio.NewScanner(connection)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var envelope struct {
			Type    string          `json:"type"`
			Subject string          `json:"subject"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil || envelope.Subject != request.RequestID {
			continue
		}
		switch envelope.Type {
		case "location.query.completed":
			var response Response
			if err := json.Unmarshal(envelope.Data, &response); err != nil {
				return Response{}, err
			}
			return response, nil
		case "location.query.failed":
			var failure Failure
			if err := json.Unmarshal(envelope.Data, &failure); err != nil {
				return Response{}, err
			}
			return Response{}, failure
		}
	}
	if err := scanner.Err(); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return Response{}, contextErr
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return Response{}, context.DeadlineExceeded
		}
		return Response{}, err
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return Response{}, contextErr
	}
	return Response{}, fmt.Errorf("location bridge closed before replying")
}

func (client *Client) timeout() time.Duration {
	if client.Timeout <= 0 {
		return 5 * time.Second
	}
	return client.Timeout
}

func uniqueID(prefix string) string {
	if strings.TrimSpace(prefix) == "" {
		prefix = "haze-location-client"
	}
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(random)
}
