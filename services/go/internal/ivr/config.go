package ivr

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
	ttspkg "github.com/meowraii/haze-weather-radio/services/go/internal/tts"
	"gopkg.in/yaml.v3"
)

// Options are command-line/runtime settings for the IVR edge service.
type Options struct {
	ConfigPath      string
	BridgeAddr      string
	MediaBridgeAddr string
	HTTPAddr        string
	SIPAddr         string
	CacheDir        string
}

type rootConfig struct {
	FeedsFile string                  `yaml:"feeds_file"`
	Storage   datastore.StorageConfig `yaml:"storage"`
	Operator  struct {
		OnAirName     any `yaml:"on_air_name"`
		TelephoneName any `yaml:"telephone_name"`
	} `yaml:"operator"`
	Services struct {
		Rust struct {
			Location struct {
				Mode string `yaml:"mode"`
			} `yaml:"location"`
			Media struct {
				Enabled bool   `yaml:"enabled"`
				Addr    string `yaml:"addr"`
				Listen  string `yaml:"listen"`
			} `yaml:"media"`
		} `yaml:"rust"`
		Go struct {
			TTS struct {
				Readers string `yaml:"readers"`
			} `yaml:"tts"`
			IVR Config `yaml:"ivr"`
		} `yaml:"go"`
	} `yaml:"services"`
}

// Config controls the scalable telephone IVR edge.
type Config struct {
	Enabled             *bool             `yaml:"enabled"`
	Mode                string            `yaml:"mode"`
	Extensions          []extensionConfig `yaml:"extensions"`
	HTTP                httpConfig        `yaml:"http"`
	SIP                 sipConfig         `yaml:"sip"`
	Search              searchConfig      `yaml:"search"`
	Twilio              twilioConfig      `yaml:"twilio"`
	RTP                 rtpConfig         `yaml:"rtp"`
	Cache               cacheConfig       `yaml:"cache"`
	PromptsFile         string            `yaml:"prompts_file"`
	DefaultLanguage     string            `yaml:"default_language"`
	DefaultReaderID     string            `yaml:"default_reader_id"`
	DefaultPackages     []string          `yaml:"default_packages"`
	BroadcastPackages   []string          `yaml:"broadcast_packages"`
	MaxCallSeconds      int               `yaml:"max_call_seconds"`
	DigitTimeoutSeconds int               `yaml:"digit_timeout_seconds"`
	RenderTimeout       time.Duration     `yaml:"-"`
	RenderTimeoutRaw    string            `yaml:"render_timeout"`
	MaxConcurrentCalls  int               `yaml:"max_concurrent_calls"`
	MaxRenderInflight   int               `yaml:"max_render_inflight"`
}

type searchConfig struct {
	Enabled           *bool         `yaml:"enabled"`
	VoiceEnabled      *bool         `yaml:"voice_enabled"`
	SpoolDir          string        `yaml:"spool_dir"`
	MultitapWindowRaw string        `yaml:"multitap_window"`
	IdleSubmitRaw     string        `yaml:"idle_submit"`
	ASRTimeoutRaw     string        `yaml:"asr_timeout"`
	MaxAttempts       int           `yaml:"max_attempts"`
	VADPrerollMS      int           `yaml:"vad_preroll_ms"`
	VADTrailingMS     int           `yaml:"vad_trailing_ms"`
	MinVoiceMS        int           `yaml:"min_voice_ms"`
	MaxVoiceMS        int           `yaml:"max_voice_ms"`
	MultitapWindow    time.Duration `yaml:"-"`
	IdleSubmit        time.Duration `yaml:"-"`
	ASRTimeout        time.Duration `yaml:"-"`
}

func (cfg searchConfig) enabled() bool {
	return cfg.Enabled != nil && *cfg.Enabled
}

func (cfg searchConfig) voiceEnabled() bool {
	return cfg.enabled() && cfg.VoiceEnabled != nil && *cfg.VoiceEnabled
}

type twilioConfig struct {
	Enabled              bool          `yaml:"enabled"`
	ExternalURL          string        `yaml:"external_url"`
	AccountSIDEnv        string        `yaml:"account_sid_env"`
	AuthTokenEnv         string        `yaml:"auth_token_env"`
	SigningKeyEnv        string        `yaml:"signing_key_env"`
	SessionTTLRaw        string        `yaml:"session_ttl"`
	MaxMessageBytes      int64         `yaml:"max_message_bytes"`
	MaxConcurrentStreams int           `yaml:"max_concurrent_streams"`
	SessionTTL           time.Duration `yaml:"-"`
}

type extensionConfig struct {
	Extension    string `yaml:"extension"`
	Name         any    `yaml:"name"`
	NamePosition string `yaml:"name_position"`
	Province     string `yaml:"province"`
	Enabled      *bool  `yaml:"enabled"`
}

func (line extensionConfig) enabled() bool {
	return line.Enabled == nil || *line.Enabled
}

func (line extensionConfig) directProvince() string {
	province := normalizeProvinceCode(line.Province)
	if province == "CA" {
		return ""
	}
	return province
}

type httpConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
}

type sipConfig struct {
	Enabled        bool           `yaml:"enabled"`
	Listen         string         `yaml:"listen"`
	ListenPorts    sipListenPorts `yaml:"listen_ports"`
	Domain         string         `yaml:"domain"`
	PublicHost     string         `yaml:"public_host"`
	AllowedSources []string       `yaml:"allowed_sources"`
	Auth           struct {
		Enabled     bool   `yaml:"enabled"`
		Username    string `yaml:"username"`
		PasswordEnv string `yaml:"password_env"`
	} `yaml:"auth"`
	Registration sipRegistrationConfig `yaml:"registration"`
}

type sipListenPort struct {
	Port   int    `yaml:"port"`
	Domain string `yaml:"domain"`
}

type sipListenPorts []sipListenPort

type sipListenBinding struct {
	Addr   string
	Domain string
}

func (ports *sipListenPorts) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.SequenceNode {
		return fmt.Errorf("listen_ports must be a sequence")
	}
	out := make([]sipListenPort, 0, len(value.Content))
	for _, item := range value.Content {
		switch item.Kind {
		case yaml.ScalarNode:
			port, err := strconv.Atoi(strings.TrimSpace(item.Value))
			if err != nil {
				return fmt.Errorf("invalid SIP listen port %q", item.Value)
			}
			out = append(out, sipListenPort{Port: port})
		case yaml.MappingNode:
			var port sipListenPort
			if err := item.Decode(&port); err != nil {
				return err
			}
			out = append(out, port)
		default:
			return fmt.Errorf("invalid SIP listen_ports entry")
		}
	}
	*ports = out
	return nil
}

type sipRegistrationConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Server        string `yaml:"server"`
	Domain        string `yaml:"domain"`
	RegisterURI   string `yaml:"register_uri"`
	Username      string `yaml:"username"`
	AuthUsername  string `yaml:"auth_username"`
	PasswordEnv   string `yaml:"password_env"`
	FromUser      string `yaml:"from_user"`
	ContactUser   string `yaml:"contact_user"`
	ContactHost   string `yaml:"contact_host"`
	ViaHost       string `yaml:"via_host"`
	UserAgent     string `yaml:"user_agent"`
	SupportedPath *bool  `yaml:"supported_path"`
	Expires       int    `yaml:"expires"`
	RetrySeconds  int    `yaml:"retry_seconds"`
}

type rtpConfig struct {
	PortMin int `yaml:"port_min"`
	PortMax int `yaml:"port_max"`
}

type cacheConfig struct {
	Dir             string `yaml:"dir"`
	TTL             string `yaml:"ttl"`
	PhoneSampleRate int    `yaml:"phone_sample_rate"`
	PhoneCodec      string `yaml:"phone_codec"`
	MaxEntries      int    `yaml:"max_entries"`
	StampedeWaiters int    `yaml:"stampede_waiters"`
	StaticOnStartup bool   `yaml:"static_prompts_on_startup"`
}

type loadedConfig struct {
	Root              rootConfig
	IVR               Config
	BaseDir           string
	PromptsPath       string
	Feeds             []feedXML
	ForecastLocations map[string]locationRecord
	HelloWeather      map[string]locationRecord
	CLCs              map[string]locationRecord
	Geocodes          map[string]locationRecord
	NWS               map[string]locationRecord
	Prompts           PromptConfig
}

type feedsXML struct {
	Feeds []feedXML `xml:"feed"`
}

type feedXML struct {
	ID         string `xml:"id,attr"`
	EnabledRaw string `xml:"enabled,attr"`
	Timezone   string `xml:"timezone,attr"`
	Languages  struct {
		Langs []struct {
			Code string `xml:"code,attr"`
		} `xml:"lang"`
	} `xml:"languages"`
	Locations struct {
		Coverage struct {
			Regions []coverageRegionXML `xml:"region"`
		} `xml:"coverage"`
		ObservationLocations struct {
			Locations []feedLocationXML `xml:"location"`
		} `xml:"observationLocations"`
	} `xml:"locations"`
	Transmitter struct {
		Transmitters []transmitterXML `xml:"transmitter"`
	} `xml:"transmitter_metadata"`
}

type transmitterXML struct {
	Network      transmitterNetworkXML   `xml:"network"`
	SiteName     string                  `xml:"site_name"`
	Callsign     string                  `xml:"callsign"`
	Relationship string                  `xml:"relationship"`
	FrequencyMHz transmitterFrequencyXML `xml:"frequency_mhz"`
	HostName     string                  `xml:"host_name"`
}

type transmitterNetworkXML struct {
	Name           string `xml:"name"`
	Pronounciation string `xml:"pronounciation"`
	Pronunciation  string `xml:"pronunciation"`
}

type transmitterFrequencyXML struct {
	GPCLK string `xml:"gpclk,attr"`
	Value string `xml:",chardata"`
}

type coverageRegionXML struct {
	ID             string                 `xml:"id,attr"`
	Source         string                 `xml:"source,attr"`
	Name           string                 `xml:"name,attr"`
	DeriveForecast string                 `xml:"derive_forecast,attr"`
	Subregions     []coverageSubregionXML `xml:"subregion"`
}

type coverageSubregionXML struct {
	ID string `xml:"id,attr"`
}

type feedLocationXML struct {
	ID           string `xml:"id,attr"`
	Source       string `xml:"source,attr"`
	NameOverride string `xml:"name_override,attr"`
}

type locationRecord struct {
	Code      string `json:"code"`
	Source    string `json:"source"`
	Name      string `json:"name"`
	Province  string `json:"province,omitempty"`
	Country   string `json:"country,omitempty"`
	Kind      string `json:"kind,omitempty"`
	FeedID    string `json:"feed_id,omitempty"`
	Forecast  string `json:"forecast_id,omitempty"`
	StationID string `json:"station_id,omitempty"`
	Latitude  string `json:"latitude,omitempty"`
	Longitude string `json:"longitude,omitempty"`
}

func loadConfig(path string, overrides Options) (loadedConfig, error) {
	if strings.TrimSpace(path) == "" {
		path = "config.yaml"
	}
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return loadedConfig{}, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var root rootConfig
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return loadedConfig{}, err
	}
	baseDir := filepath.Dir(filepath.Clean(path))
	cfg := root.Services.Go.IVR
	normalizeIVRConfig(&cfg)
	if err := validateIVRExtensions(cfg.Extensions); err != nil {
		return loadedConfig{}, err
	}
	if overrides.HTTPAddr != "" {
		cfg.HTTP.Addr = overrides.HTTPAddr
	}
	if overrides.SIPAddr != "" {
		applySIPListenOverride(&cfg.SIP, overrides.SIPAddr)
	}
	if overrides.CacheDir != "" {
		cfg.Cache.Dir = overrides.CacheDir
	}
	feeds, err := loadFeeds(resolvePath(baseDir, fallbackText(root.FeedsFile, "managed/configs/feeds.xml")))
	if err != nil {
		return loadedConfig{}, err
	}
	promptsPath := resolvePath(baseDir, cfg.PromptsFile)
	prompts, err := loadPromptConfig(promptsPath)
	if err != nil {
		return loadedConfig{}, err
	}
	if cfg.Search.enabled() {
		spoolPath := resolvePath(baseDir, cfg.Search.SpoolDir)
		name := strings.ToLower(filepath.Base(filepath.Clean(spoolPath)))
		spoolAbs, _ := filepath.Abs(filepath.Clean(spoolPath))
		baseAbs, _ := filepath.Abs(filepath.Clean(baseDir))
		if spoolAbs == baseAbs || !strings.Contains(name, "asr") {
			return loadedConfig{}, fmt.Errorf("IVR search spool must be a dedicated directory whose name contains asr")
		}
	}
	return loadedConfig{
		Root:              root,
		IVR:               cfg,
		BaseDir:           baseDir,
		PromptsPath:       promptsPath,
		Feeds:             feeds,
		ForecastLocations: loadForecastLocationsForBase(baseDir),
		HelloWeather:      loadHelloWeatherForBase(baseDir),
		CLCs:              loadCLCsForBase(baseDir),
		Geocodes:          loadGeocodesForBase(baseDir),
		NWS:               loadNWSForBase(baseDir),
		Prompts:           prompts,
	}, nil
}

func applySIPListenOverride(cfg *sipConfig, addr string) {
	if cfg == nil || strings.TrimSpace(addr) == "" {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(cfg.Listen), strings.TrimSpace(addr)) {
		cfg.ListenPorts = nil
	}
	cfg.Listen = addr
}

func normalizeIVRConfig(cfg *Config) {
	if cfg.Mode == "" {
		cfg.Mode = "sip-edge"
	}
	normalizeIVRExtensions(cfg)
	if cfg.HTTP.Addr == "" {
		cfg.HTTP.Addr = "127.0.0.1:8096"
	}
	if cfg.SIP.Listen == "" {
		cfg.SIP.Listen = "0.0.0.0:5060"
	}
	if cfg.SIP.Registration.Expires == 0 {
		cfg.SIP.Registration.Expires = 300
	}
	if cfg.SIP.Registration.RetrySeconds == 0 {
		cfg.SIP.Registration.RetrySeconds = 30
	}
	if cfg.SIP.Registration.UserAgent == "" {
		cfg.SIP.Registration.UserAgent = "Haze Weather Radio IVR"
	}
	if cfg.SIP.Registration.SupportedPath == nil {
		defaultSupportedPath := true
		cfg.SIP.Registration.SupportedPath = &defaultSupportedPath
	}
	if cfg.Search.Enabled == nil {
		enabled := false
		cfg.Search.Enabled = &enabled
	}
	if cfg.Search.VoiceEnabled == nil {
		enabled := true
		cfg.Search.VoiceEnabled = &enabled
	}
	if cfg.Search.SpoolDir == "" {
		cfg.Search.SpoolDir = "runtime/ivr/asr"
	}
	cfg.Search.MultitapWindow = parsePositiveDuration(cfg.Search.MultitapWindowRaw, 700*time.Millisecond)
	cfg.Search.IdleSubmit = parsePositiveDuration(cfg.Search.IdleSubmitRaw, 2500*time.Millisecond)
	cfg.Search.ASRTimeout = parsePositiveDuration(cfg.Search.ASRTimeoutRaw, 18*time.Second)
	if cfg.Search.MaxAttempts <= 0 || cfg.Search.MaxAttempts > 2 {
		cfg.Search.MaxAttempts = 2
	}
	if cfg.Search.VADPrerollMS <= 0 {
		cfg.Search.VADPrerollMS = 300
	} else if cfg.Search.VADPrerollMS > 1000 {
		cfg.Search.VADPrerollMS = 1000
	}
	if cfg.Search.VADTrailingMS <= 0 {
		cfg.Search.VADTrailingMS = 900
	} else if cfg.Search.VADTrailingMS > 2000 {
		cfg.Search.VADTrailingMS = 2000
	}
	if cfg.Search.MinVoiceMS < 2000 {
		cfg.Search.MinVoiceMS = 2000
	}
	if cfg.Search.MaxVoiceMS <= 0 {
		cfg.Search.MaxVoiceMS = 8000
	} else if cfg.Search.MaxVoiceMS < 2000 {
		cfg.Search.MaxVoiceMS = 2000
	}
	if cfg.Search.MaxVoiceMS > 8000 {
		cfg.Search.MaxVoiceMS = 8000
	}
	if cfg.Search.MinVoiceMS > cfg.Search.MaxVoiceMS {
		cfg.Search.MinVoiceMS = cfg.Search.MaxVoiceMS
	}
	if cfg.Twilio.AccountSIDEnv == "" {
		cfg.Twilio.AccountSIDEnv = "HAZE_TWILIO_ACCOUNT_SID"
	}
	if cfg.Twilio.AuthTokenEnv == "" {
		cfg.Twilio.AuthTokenEnv = "HAZE_TWILIO_AUTH_TOKEN"
	}
	if cfg.Twilio.SigningKeyEnv == "" {
		cfg.Twilio.SigningKeyEnv = "HAZE_IVR_SIGNING_KEY"
	}
	cfg.Twilio.SessionTTL = parsePositiveDuration(cfg.Twilio.SessionTTLRaw, 2*time.Minute)
	if cfg.Twilio.SessionTTL > 2*time.Minute {
		cfg.Twilio.SessionTTL = 2 * time.Minute
	}
	if cfg.Twilio.MaxMessageBytes <= 0 {
		cfg.Twilio.MaxMessageBytes = 64 * 1024
	} else if cfg.Twilio.MaxMessageBytes > 256*1024 {
		cfg.Twilio.MaxMessageBytes = 256 * 1024
	}
	if cfg.RTP.PortMin == 0 {
		cfg.RTP.PortMin = 30000
	}
	if cfg.RTP.PortMax == 0 {
		cfg.RTP.PortMax = 39999
	}
	if cfg.Cache.Dir == "" {
		cfg.Cache.Dir = "runtime/ivr/cache"
	}
	if cfg.Cache.TTL == "" {
		cfg.Cache.TTL = "10m"
	}
	if cfg.Cache.PhoneSampleRate == 0 {
		cfg.Cache.PhoneSampleRate = 8000
	}
	if cfg.Cache.PhoneCodec == "" {
		cfg.Cache.PhoneCodec = "pcmu"
	}
	if cfg.Cache.StampedeWaiters == 0 {
		cfg.Cache.StampedeWaiters = 64
	}
	if cfg.PromptsFile == "" {
		cfg.PromptsFile = "managed/configs/ivr.xml"
	}
	if cfg.DefaultLanguage == "" {
		cfg.DefaultLanguage = "en-US"
	}
	if len(cfg.DefaultPackages) == 0 {
		cfg.DefaultPackages = []string{"current_conditions", "forecast"}
	}
	if len(cfg.BroadcastPackages) == 0 {
		cfg.BroadcastPackages = []string{"alerts", "current_conditions", "air_quality", "forecast", "geophysical_alert"}
	}
	if cfg.MaxCallSeconds == 0 {
		cfg.MaxCallSeconds = 240
	}
	if cfg.DigitTimeoutSeconds == 0 {
		cfg.DigitTimeoutSeconds = 8
	}
	if cfg.RenderTimeoutRaw == "" {
		cfg.RenderTimeoutRaw = "60s"
	}
	timeout, err := time.ParseDuration(cfg.RenderTimeoutRaw)
	if err != nil || timeout <= 0 {
		timeout = 60 * time.Second
	}
	cfg.RenderTimeout = timeout
	if cfg.MaxConcurrentCalls <= 0 {
		cfg.MaxConcurrentCalls = 256
	}
	if cfg.Twilio.MaxConcurrentStreams <= 0 {
		cfg.Twilio.MaxConcurrentStreams = cfg.MaxConcurrentCalls
	} else if cfg.Twilio.MaxConcurrentStreams > cfg.MaxConcurrentCalls {
		cfg.Twilio.MaxConcurrentStreams = cfg.MaxConcurrentCalls
	}
	if cfg.MaxRenderInflight == 0 {
		cfg.MaxRenderInflight = 8
	}
}

func parsePositiveDuration(raw string, fallback time.Duration) time.Duration {
	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func normalizeIVRExtensions(cfg *Config) {
	if cfg == nil {
		return
	}
	if len(cfg.Extensions) == 0 {
		cfg.Extensions = []extensionConfig{defaultCanadaExtension()}
	}
	hasCanada := false
	for index := range cfg.Extensions {
		line := &cfg.Extensions[index]
		line.Extension = normalizeExtensionID(line.Extension)
		line.Province = normalizeProvinceCode(line.Province)
		if line.Province == "CA" {
			hasCanada = true
		}
		line.NamePosition = strings.ToLower(strings.TrimSpace(line.NamePosition))
		if line.NamePosition == "" {
			line.NamePosition = "before"
		}
		if displayText(line.Name) == "" {
			line.Name = provinceDisplayName(line.Province)
		}
	}
	if !hasCanada {
		cfg.Extensions = append([]extensionConfig{defaultCanadaExtension()}, cfg.Extensions...)
	}
}

func defaultCanadaExtension() extensionConfig {
	enabled := true
	return extensionConfig{
		Extension:    "haze",
		Name:         "Canada",
		NamePosition: "before",
		Province:     "CA",
		Enabled:      &enabled,
	}
}

func validateIVRExtensions(lines []extensionConfig) error {
	seen := make(map[string]struct{}, len(lines))
	for index, line := range lines {
		if line.Extension == "" {
			return fmt.Errorf("ivr extension %d has an empty extension", index+1)
		}
		if _, exists := seen[line.Extension]; exists {
			return fmt.Errorf("ivr extension %q is configured more than once", line.Extension)
		}
		seen[line.Extension] = struct{}{}
		if !validProvinceCode(line.Province) {
			return fmt.Errorf("ivr extension %q has unsupported province %q", line.Extension, line.Province)
		}
		if line.NamePosition != "before" && line.NamePosition != "after" {
			return fmt.Errorf("ivr extension %q has unsupported name_position %q", line.Extension, line.NamePosition)
		}
	}
	return nil
}

func normalizeExtensionID(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeProvinceCode(value string) string {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "Canada") {
		return "CA"
	}
	return provinceCode(value)
}

func validProvinceCode(value string) bool {
	switch normalizeProvinceCode(value) {
	case "CA", "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT":
		return true
	default:
		return false
	}
}

func provinceDisplayName(value string) string {
	code := normalizeProvinceCode(value)
	if code == "CA" {
		return "Canada"
	}
	for _, province := range helloWeatherProvinces() {
		if province.Code == code {
			return province.Name
		}
	}
	return code
}

func (cfg Config) extensionLine(extension string) (extensionConfig, bool) {
	extension = normalizeExtensionID(extension)
	for _, line := range cfg.Extensions {
		if line.Extension == extension {
			return line, true
		}
	}
	for _, line := range cfg.Extensions {
		if line.Province == "CA" {
			return line, false
		}
	}
	return defaultCanadaExtension(), false
}

func (cfg sipConfig) listenBindings() []sipListenBinding {
	host, fallbackPort := sipListenHostPort(cfg.Listen)
	if len(cfg.ListenPorts) == 0 {
		return []sipListenBinding{{
			Addr:   net.JoinHostPort(host, strconv.Itoa(fallbackPort)),
			Domain: normalizeSIPDomain(cfg.Domain),
		}}
	}
	seen := map[string]struct{}{}
	bindings := make([]sipListenBinding, 0, len(cfg.ListenPorts))
	for _, configured := range cfg.ListenPorts {
		port := configured.Port
		if port <= 0 || port > 65535 {
			continue
		}
		addr := net.JoinHostPort(host, strconv.Itoa(port))
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		bindings = append(bindings, sipListenBinding{
			Addr:   addr,
			Domain: normalizeSIPDomain(firstNonBlank(configured.Domain, cfg.Domain)),
		})
	}
	if len(bindings) == 0 {
		return []sipListenBinding{{
			Addr:   net.JoinHostPort(host, strconv.Itoa(fallbackPort)),
			Domain: normalizeSIPDomain(cfg.Domain),
		}}
	}
	return bindings
}

func (cfg sipConfig) listenAddrs() []string {
	bindings := cfg.listenBindings()
	addrs := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		addrs = append(addrs, binding.Addr)
	}
	return addrs
}

func sipListenHostPort(listen string) (string, int) {
	listen = strings.TrimSpace(listen)
	if listen == "" {
		return "0.0.0.0", 5060
	}
	host, portText, err := net.SplitHostPort(listen)
	if err == nil {
		port, portErr := strconv.Atoi(portText)
		if portErr == nil && port > 0 && port <= 65535 {
			if strings.TrimSpace(host) == "" {
				host = "0.0.0.0"
			}
			return host, port
		}
	}
	if port, err := strconv.Atoi(listen); err == nil && port > 0 && port <= 65535 {
		return "0.0.0.0", port
	}
	return "0.0.0.0", 5060
}

func normalizeSIPDomain(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "<>[]")
	value = strings.TrimSuffix(value, ".")
	return strings.ToLower(value)
}

func (cfg loadedConfig) enabled() bool {
	if cfg.IVR.Enabled == nil {
		return false
	}
	return *cfg.IVR.Enabled
}

func (cfg loadedConfig) cacheDir() string {
	return resolvePath(cfg.BaseDir, cfg.IVR.Cache.Dir)
}

func (cfg loadedConfig) searchSpoolDir() string {
	return resolvePath(cfg.BaseDir, cfg.IVR.Search.SpoolDir)
}

func (cfg loadedConfig) ttsReadersPath() string {
	return resolvePath(cfg.BaseDir, fallbackText(cfg.Root.Services.Go.TTS.Readers, "managed/configs/readers.xml"))
}

func (cfg loadedConfig) mediaServiceBaseURL() string {
	media := cfg.Root.Services.Rust.Media
	if !media.Enabled {
		return ""
	}
	addr := strings.TrimSpace(firstNonBlank(media.Addr, media.Listen))
	if addr == "" {
		addr = "127.0.0.1:8097"
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + addr
}

func (cfg loadedConfig) ttsReaderFingerprint(readerID string, language string, gender string) string {
	readerID = strings.TrimSpace(readerID)
	if readerID == "" {
		return ""
	}
	readers, err := ttspkg.LoadReaders(cfg.ttsReadersPath())
	if err != nil {
		return "error:" + err.Error()
	}
	reader, ok := ttspkg.SelectReader(readers, readerID, language, gender)
	if !ok {
		return "missing:" + readerID
	}
	return strings.Join([]string{
		reader.ID,
		reader.Provider,
		reader.VoiceID,
		reader.Language,
		reader.Gender,
	}, "|")
}

func (cfg loadedConfig) cacheTTL() time.Duration {
	ttl, err := time.ParseDuration(cfg.IVR.Cache.TTL)
	if err != nil || ttl <= 0 {
		return 10 * time.Minute
	}
	return ttl
}

func loadFeeds(path string) ([]feedXML, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var parsed feedsXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse feeds XML: %w", err)
	}
	return parsed.Feeds, nil
}

func loadForecastLocations(path string) map[string]locationRecord {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return map[string]locationRecord{}
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	_, _ = reader.Read()
	header, err := reader.Read()
	if err != nil {
		return map[string]locationRecord{}
	}
	codeIndex := csvIndex(header, "CODE")
	nameIndex := csvIndex(header, "NAME")
	provIndex := csvIndex(header, "PROVINCE/WATERBODY 2 PROVINCE/PLAN D'EAU 2")
	out := map[string]locationRecord{}
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		if codeIndex < 0 || nameIndex < 0 || len(row) <= maxInt(codeIndex, nameIndex) {
			continue
		}
		code := strings.Trim(strings.TrimSpace(row[codeIndex]), `"`)
		if code == "" {
			continue
		}
		province := ""
		if provIndex >= 0 && len(row) > provIndex {
			province = strings.Trim(strings.TrimSpace(row[provIndex]), `"`)
		}
		if _, exists := out[code]; !exists {
			out[code] = locationRecord{
				Code:     code,
				Source:   "eccc_forecast",
				Name:     strings.Trim(strings.TrimSpace(row[nameIndex]), `"`),
				Province: province,
			}
		}
	}
	return out
}

func loadForecastLocationsForBase(baseDir string) map[string]locationRecord {
	if snap, ok := locationdb.Load(baseDir); ok {
		out := map[string]locationRecord{}
		for _, place := range snap.PlacesBySource("forecast") {
			out[place.Code] = locationRecord{
				Code:      place.Code,
				Source:    "eccc_forecast",
				Name:      place.Name,
				Province:  place.Region,
				Latitude:  floatText(place.Lat),
				Longitude: floatText(place.Lon),
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return loadForecastLocations(resolvePath(baseDir, "managed/csv/FORECAST_LOCATIONS.csv"))
}

func loadHelloWeatherForBase(baseDir string) map[string]locationRecord {
	if snap, ok := locationdb.Load(baseDir); ok {
		out := map[string]locationRecord{}
		for _, place := range snap.PlacesBySource("hello_weather") {
			out[place.Code] = locationRecord{
				Code:      place.Code,
				Source:    "hello_weather",
				Name:      place.Name,
				Province:  place.Region,
				Latitude:  floatText(place.Lat),
				Longitude: floatText(place.Lon),
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return loadHelloWeatherCSV(resolvePath(baseDir, "managed/csv/HELLO_WEATHER_LOCATIONS.csv"))
}

func loadHelloWeatherCSV(path string) map[string]locationRecord {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return map[string]locationRecord{}
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return map[string]locationRecord{}
	}
	codeIndex := csvIndex(header, "CODE")
	nameIndex := csvIndex(header, "NAME")
	provinceIndex := csvIndex(header, "PROVINCE")
	out := map[string]locationRecord{}
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		code := valueAt(row, codeIndex)
		name := valueAt(row, nameIndex)
		province := normalizeProvinceCode(valueAt(row, provinceIndex))
		if len(code) != 5 || name == "" || !validProvinceCode(province) || province == "CA" {
			continue
		}
		out[code] = locationRecord{Code: code, Source: "hello_weather", Name: name, Province: province}
	}
	return out
}

func loadCLCsForBase(baseDir string) map[string]locationRecord {
	if snap, ok := locationdb.Load(baseDir); ok {
		out := map[string]locationRecord{}
		for _, place := range snap.PlacesBySource("clc") {
			out[place.Code] = locationRecord{
				Code:      place.Code,
				Source:    "clc",
				Name:      place.Name,
				Province:  place.Region,
				Latitude:  floatText(place.Lat),
				Longitude: floatText(place.Lon),
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return loadCommaCSV(resolvePath(baseDir, "managed/csv/CLC_Base_Zone.csv"), "CLC", "NAME", "PROVINCE_C", "clc")
}

func loadGeocodesForBase(baseDir string) map[string]locationRecord {
	if snap, ok := locationdb.Load(baseDir); ok {
		out := map[string]locationRecord{}
		for _, place := range snap.PlacesBySource("sgc") {
			out[place.Code] = locationRecord{
				Code:      place.Code,
				Source:    "capcp_geocode",
				Name:      place.Name,
				Province:  place.Region,
				Latitude:  floatText(place.Lat),
				Longitude: floatText(place.Lon),
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return loadCommaCSV(resolvePath(baseDir, "managed/csv/CAP-CP_Geocodes.csv"), "CAPCPGCODE", "NAME", "PROVINCE_C", "capcp_geocode")
}

func loadNWSForBase(baseDir string) map[string]locationRecord {
	if snap, ok := locationdb.Load(baseDir); ok {
		out := map[string]locationRecord{}
		for _, place := range snap.PlacesBySource("nws_zone") {
			out[place.Code] = locationRecord{Code: place.Code, Source: "nws_zone", Name: place.Name, Province: place.Region, Forecast: place.Code}
		}
		for _, place := range snap.PlacesBySource("nws_same") {
			out[place.Code] = locationRecord{Code: place.Code, Source: "nws_same", Name: place.Name, Province: place.Region}
		}
		for _, place := range snap.PlacesBySource("nws_marine_zone") {
			out[place.Code] = locationRecord{Code: place.Code, Source: "nws_marine_zone", Name: place.Name, Province: place.Region, Forecast: place.Code}
		}
		for _, place := range snap.PlacesBySource("nws_marine_same") {
			out[place.Code] = locationRecord{Code: place.Code, Source: "nws_marine_same", Name: place.Name, Province: place.Region}
		}
		if len(out) > 0 {
			return out
		}
	}
	return loadPipeCSV(resolvePath(baseDir, "managed/csv/NWS_ZONE_COUNTY_CORRELATION.csv"))
}

func floatText(value float64) string {
	if value == 0 {
		return ""
	}
	return fmt.Sprintf("%.8f", value)
}

func loadCommaCSV(path string, codeHeader string, nameHeader string, provinceHeader string, source string) map[string]locationRecord {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return map[string]locationRecord{}
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return map[string]locationRecord{}
	}
	codeIndex := csvIndex(header, codeHeader)
	nameIndex := csvIndex(header, nameHeader)
	provIndex := csvIndex(header, provinceHeader)
	latIndex := csvIndex(header, "LAT_DD")
	lonIndex := csvIndex(header, "LON_DD")
	out := map[string]locationRecord{}
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		if codeIndex < 0 || nameIndex < 0 || len(row) <= maxInt(codeIndex, nameIndex) {
			continue
		}
		code := strings.Trim(strings.TrimSpace(row[codeIndex]), `"`)
		if code == "" {
			continue
		}
		province := ""
		if provIndex >= 0 && len(row) > provIndex {
			province = strings.Trim(strings.TrimSpace(row[provIndex]), `"`)
		}
		latitude := ""
		if latIndex >= 0 && len(row) > latIndex {
			latitude = strings.Trim(strings.TrimSpace(row[latIndex]), `"`)
		}
		longitude := ""
		if lonIndex >= 0 && len(row) > lonIndex {
			longitude = strings.Trim(strings.TrimSpace(row[lonIndex]), `"`)
		}
		out[code] = locationRecord{
			Code:      code,
			Source:    source,
			Name:      strings.Trim(strings.TrimSpace(row[nameIndex]), `"`),
			Province:  province,
			Latitude:  latitude,
			Longitude: longitude,
		}
	}
	return out
}

func loadPipeCSV(path string) map[string]locationRecord {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return map[string]locationRecord{}
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '|'
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return map[string]locationRecord{}
	}
	zoneIndex := csvIndex(header, "STATE+ZONE")
	nameIndex := csvIndex(header, "ZONE_NAME")
	countyIndex := csvIndex(header, "COUNTY_NAME")
	fipsIndex := csvIndex(header, "FIPS/SAME")
	stateIndex := csvIndex(header, "STATE")
	out := map[string]locationRecord{}
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		if zoneIndex >= 0 && len(row) > zoneIndex {
			code := strings.ToUpper(strings.TrimSpace(row[zoneIndex]))
			if code != "" {
				out[code] = locationRecord{Code: code, Source: "nws_zone", Name: valueAt(row, nameIndex), Province: valueAt(row, stateIndex), Forecast: code}
			}
		}
		if fipsIndex >= 0 && len(row) > fipsIndex {
			code := strings.TrimSpace(row[fipsIndex])
			if code != "" {
				out[code] = locationRecord{Code: code, Source: "nws_same", Name: valueAt(row, countyIndex), Province: valueAt(row, stateIndex)}
			}
		}
	}
	return out
}

func csvIndex(header []string, name string) int {
	for index, value := range header {
		if strings.EqualFold(strings.TrimSpace(value), name) {
			return index
		}
	}
	return -1
}

func valueAt(row []string, index int) string {
	if index < 0 || index >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[index])
}

func maxInt(values ...int) int {
	max := values[0]
	for _, value := range values[1:] {
		if value > max {
			max = value
		}
	}
	return max
}

func resolvePath(base string, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return filepath.Clean(base)
	}
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Clean(filepath.Join(base, value))
}

func fallbackText(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return strings.TrimSpace(fallback)
	}
	return strings.TrimSpace(value)
}

func displayText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []any:
		merged := map[string]any{}
		for _, item := range typed {
			if child, ok := item.(map[string]any); ok {
				for key, value := range child {
					merged[key] = value
				}
			}
		}
		for _, key := range []string{"pronunciation", "text", "name"} {
			if text := displayText(merged[key]); text != "" {
				return text
			}
		}
	case map[string]any:
		for _, key := range []string{"pronunciation", "text", "name", "value"} {
			if text := displayText(typed[key]); text != "" {
				return text
			}
		}
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
	return ""
}

func xmlBool(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return fallback
	case "1", "true", "yes", "on", "enabled":
		return true
	case "0", "false", "no", "off", "disabled":
		return false
	default:
		return fallback
	}
}
