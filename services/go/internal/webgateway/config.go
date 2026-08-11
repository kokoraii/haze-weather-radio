package webgateway

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"gopkg.in/yaml.v3"
)

// Config contains the minimal web metadata needed by the Go gateway.
type Config struct {
	Version  string                  `yaml:"version"`
	Storage  datastore.StorageConfig `yaml:"storage"`
	Webpanel struct {
		Host       string   `yaml:"host"`
		Port       yamlPort `yaml:"port"`
		PublicPort struct {
			Enabled   bool     `yaml:"enabled"`
			Host      string   `yaml:"host"`
			HTTPPort  yamlPort `yaml:"http_port"`
			HTTPSPort yamlPort `yaml:"https_port"`
		} `yaml:"public_port"`
		Public struct {
			SiteName      string `yaml:"site_name"`
			AlertsArchive struct {
				Access string `yaml:"access"`
			} `yaml:"alerts_archive"`
			Feeds struct {
				Access string `yaml:"access"`
				WebRTC struct {
					Enabled bool `yaml:"enabled"`
				} `yaml:"webrtc"`
			} `yaml:"feeds"`
		} `yaml:"public"`
		Admin struct {
			Host string   `yaml:"host"`
			Port yamlPort `yaml:"port"`
		} `yaml:"admin"`
		TLS struct {
			Enabled       bool     `yaml:"enabled"`
			Mode          string   `yaml:"mode"`
			Domains       []string `yaml:"domains"`
			Email         string   `yaml:"email"`
			CacheDir      string   `yaml:"cache_dir"`
			CertFile      string   `yaml:"cert_file"`
			KeyFile       string   `yaml:"key_file"`
			RedirectHTTP  bool     `yaml:"redirect_http"`
			HSTS          bool     `yaml:"hsts"`
			Staging       bool     `yaml:"staging"`
			HTTPChallenge struct {
				Enabled *bool    `yaml:"enabled"`
				Addr    string   `yaml:"addr"`
				Host    string   `yaml:"host"`
				Port    yamlPort `yaml:"port"`
			} `yaml:"http_challenge"`
		} `yaml:"tls"`
		Receiver struct {
			Enabled              bool   `yaml:"enabled"`
			BasePath             string `yaml:"base_path"`
			RequireTLS           bool   `yaml:"require_tls"`
			ChallengeTTLSeconds  int    `yaml:"challenge_ttl_seconds"`
			CookieTTLSeconds     int    `yaml:"cookie_ttl_seconds"`
			CredentialTTLSeconds int    `yaml:"credential_ttl_seconds"`
			CredentialsPath      string `yaml:"credentials_path"`
			TransmitterDefaults  struct {
				BandwidthKHz float64 `yaml:"bandwidth_khz"`
				DeviationHz  int     `yaml:"deviation_hz"`
				Preemphasis  string  `yaml:"preemphasis"`
			} `yaml:"transmitter_defaults"`
			PairingTokens []struct {
				ID       string   `yaml:"id"`
				Enabled  *bool    `yaml:"enabled"`
				Token    string   `yaml:"token"`
				TokenEnv string   `yaml:"token_env"`
				FeedIDs  []string `yaml:"feed_ids"`
			} `yaml:"pairing_tokens"`
		} `yaml:"receiver"`
		Authentication struct {
			Enabled                     *bool    `yaml:"enabled"`
			Mode                        string   `yaml:"mode"`
			SessionTTLSeconds           int      `yaml:"session_ttl_seconds"`
			IdleTimeoutSeconds          int      `yaml:"idle_timeout_seconds"`
			PersistentSessionTTLSeconds int      `yaml:"persistent_session_ttl_seconds"`
			SecureCookies               bool     `yaml:"secure_cookies"`
			LoginRateLimit              int      `yaml:"login_rate_limit"`
			LoginRateWindowSeconds      int      `yaml:"login_rate_window_seconds"`
			AccountLockDurationSeconds  int      `yaml:"account_lock_duration_seconds"`
			OriginationRatePerSecond    int      `yaml:"origination_rate_per_second"`
			EnforceMFA                  bool     `yaml:"enforce_mfa"`
			RedisRequired               bool     `yaml:"redis_required"`
			RedisURLEnv                 string   `yaml:"redis_url_env"`
			RedisKeyPrefix              string   `yaml:"redis_key_prefix"`
			PasetoKeyEnv                string   `yaml:"paseto_key_env"`
			PasswordPepperEnv           string   `yaml:"password_pepper_env"`
			MFAKeyEnv                   string   `yaml:"mfa_key_env"`
			AuditKeyEnv                 string   `yaml:"audit_key_env"`
			AuditDir                    string   `yaml:"audit_dir"`
			BootstrapUsernameEnv        string   `yaml:"bootstrap_username_env"`
			BootstrapPasswordEnv        string   `yaml:"bootstrap_password_env"`
			TrustedProxyCIDRs           []string `yaml:"trusted_proxy_cidrs"`
			LoginCIDRAllowlist          []string `yaml:"login_cidr_allowlist"`
			LegacyLoginCIDRWhitelist    []string `yaml:"login_cidr_whitelist"`
			MaxAudioUploadBytes         int64    `yaml:"max_audio_upload_bytes"`
		} `yaml:"authentication"`
	} `yaml:"webpanel"`
	Operator struct {
		OnAirName    any `yaml:"on_air_name"`
		OperatorName any `yaml:"operator_name"`
	} `yaml:"operator"`
	Services struct {
		Rust struct {
			Media struct {
				Enabled bool   `yaml:"enabled"`
				Addr    string `yaml:"addr"`
				Listen  string `yaml:"listen"`
				Backend string `yaml:"backend"`
			} `yaml:"media"`
		} `yaml:"rust"`
	} `yaml:"services"`
}

type yamlPort int

func (p *yamlPort) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return nil
	}
	raw := strings.TrimSpace(value.Value)
	if raw == "" {
		*p = 0
		return nil
	}
	port, err := strconv.Atoi(raw)
	if err != nil {
		return err
	}
	*p = yamlPort(port)
	return nil
}

func (p yamlPort) Int() int {
	return int(p)
}

// LoadConfig reads a Haze YAML config. Missing files produce a default config.
func LoadConfig(path string) (Config, error) {
	var config Config
	if path == "" {
		path = "config.yaml"
	}
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return config, nil
		}
		return config, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	if err := yaml.Unmarshal(raw, &config); err != nil {
		return config, err
	}
	if len(config.Webpanel.Authentication.LoginCIDRAllowlist) == 0 && len(config.Webpanel.Authentication.LegacyLoginCIDRWhitelist) > 0 {
		config.Webpanel.Authentication.LoginCIDRAllowlist = append([]string(nil), config.Webpanel.Authentication.LegacyLoginCIDRWhitelist...)
	}
	return config, nil
}

func displayText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		for _, item := range typed {
			if text := displayText(item); text != "" {
				return text
			}
		}
	case map[string]any:
		if text, ok := typed["text"].(string); ok {
			return text
		}
		if text, ok := typed["name"].(string); ok {
			return text
		}
	}
	return ""
}
