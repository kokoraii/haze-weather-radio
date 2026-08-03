package productrender

import "time"

const serviceID = "haze-product-render"

// Product is the complete TTS payload that playout queues for a feed.
type Product struct {
	ID          string            `json:"id"`
	FeedID      string            `json:"feed_id"`
	PackageID   string            `json:"package_id"`
	Title       string            `json:"title"`
	Text        string            `json:"text"`
	ReaderID    string            `json:"reader_id,omitempty"`
	Language    string            `json:"language"`
	Segments    []Segment         `json:"segments"`
	Inputs      []InputRef        `json:"inputs,omitempty"`
	GeneratedAt time.Time         `json:"generated_at"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Segment preserves the product structure before it is flattened for TTS.
type Segment struct {
	Kind      string `json:"kind"`
	Label     string `json:"label,omitempty"`
	Text      string `json:"text,omitempty"`
	AudioPath string `json:"audio_path,omitempty"`
}

// InputRef identifies a provider snapshot or event that shaped the render.
type InputRef struct {
	Type      string    `json:"type"`
	ID        string    `json:"id"`
	Observed  time.Time `json:"observed_at,omitempty"`
	Freshness string    `json:"freshness,omitempty"`
}

type renderRequest struct {
	RequestID    string
	FeedID       string
	PackageID    string
	Language     string
	Force        bool
	FeedOverride *feedXML
	Telephone    bool
}

type wxOnDemandRequest struct {
	RequestID    string
	CanonicalID  string
	FeedID       string
	Code         string
	Source       string
	Identity     string
	LocationName string
	Province     string
	Country      string
	ForecastID   string
	StationID    string
	Latitude     string
	Longitude    string
	Timezone     string
	Language     string
	ReaderID     string
	Packages     []string
	Force        bool
	Telephone    bool
}

type observationSnapshot struct {
	ReportedAt       string        `json:"reported_at"`
	Primary          observation   `json:"primary_observation"`
	Observations     []observation `json:"observations"`
	AreaObservations []observation `json:"area_observations"`
}

type observation struct {
	ID               string   `json:"id"`
	Source           string   `json:"source,omitempty"`
	LocationName     string   `json:"location_name"`
	Condition        string   `json:"condition"`
	SkyCondition     string   `json:"sky_condition,omitempty"`
	TemperatureC     *float64 `json:"temperature_c"`
	DewpointC        *float64 `json:"dewpoint_c"`
	HumidityPercent  *float64 `json:"humidity_percent"`
	WindDirection    string   `json:"wind_direction"`
	WindSpeedKMH     *float64 `json:"wind_speed_kmh"`
	WindGustKMH      *float64 `json:"wind_gust_kmh"`
	VisibilityKM     *float64 `json:"visibility_km"`
	PressureKPA      *float64 `json:"pressure_kpa"`
	PressureTendency string   `json:"pressure_tendency"`
	Altimeter        string   `json:"altimeter,omitempty"`
	ObservedAt       string   `json:"observed_at"`
}

type forecastSnapshot struct {
	IssuedAt string           `json:"issued_at"`
	Regions  []forecastRegion `json:"regions"`
	Periods  []forecastPeriod `json:"periods"`
}

type forecastRegion struct {
	Name    string           `json:"name"`
	Periods []forecastPeriod `json:"periods"`
}

type forecastPeriod struct {
	Name string `json:"name"`
	Text string `json:"text"`
}

type airQualitySnapshot struct {
	ReportedAt   string                     `json:"reported_at"`
	Location     string                     `json:"location"`
	AQHI         string                     `json:"aqhi"`
	Risk         string                     `json:"risk"`
	Forecast     string                     `json:"forecast"`
	SpecialNotes string                     `json:"special_notes,omitempty"`
	Periods      []airQualityPeriodSnapshot `json:"periods,omitempty"`
}

type airQualityPeriodSnapshot struct {
	Name        string `json:"name"`
	AQHI        string `json:"aqhi"`
	AQHIInSmoke string `json:"aqhi_insmoke,omitempty"`
	Risk        string `json:"risk,omitempty"`
}

type climateSnapshot struct {
	ReportedAt string   `json:"reported_at"`
	Location   string   `json:"location"`
	Summary    []string `json:"summary"`
}

type marineForecastSnapshot struct {
	IssuedAt  string
	UpdatedAt string
	Area      string
	Regular   []marineForecastLocation
	Waves     []marineForecastLocation
	Extended  []marineForecastPeriod
	Warnings  []marineForecastLocation
}

type marineForecastLocation struct {
	Name   string
	Period string
	Text   string
}

type marineForecastPeriod struct {
	Name string
	Text string
}

type bulletinSnapshot struct {
	Title       string   `json:"title"`
	Lines       []string `json:"lines"`
	ContentType string   `json:"content_type,omitempty"`
	AudioPath   string   `json:"audio_path,omitempty"`
	AudioURL    string   `json:"audio_url,omitempty"`
}
