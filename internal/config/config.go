// Package config loads and validates data source configuration from JSON
// files, environment variables and .env files.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/joho/godotenv"
)

// SourceType identifies the kind of a data source.
type SourceType string

const (
	TypeElasticsearch SourceType = "elasticsearch"
	TypeRedis         SourceType = "redis"
	TypeKafka         SourceType = "kafka"
)

// RedisMode controls whether a single-node or cluster client is used.
type RedisMode string

const (
	RedisModeAuto    RedisMode = "auto"
	RedisModeSingle  RedisMode = "single"
	RedisModeCluster RedisMode = "cluster"
)

// ESVersion controls Elasticsearch client selection.
type ESVersion string

const (
	ESVersionAuto ESVersion = "auto"
	ESVersionV7   ESVersion = "7"
	ESVersionV8   ESVersion = "8"
)

// DeploymentMode distinguishes single-instance from cluster deployments.
type DeploymentMode string

const (
	ModeSingle  DeploymentMode = "single"
	ModeCluster DeploymentMode = "cluster"
)

// MaskingRule defines how a field is masked in result sets.
type MaskingRule struct {
	Field    string `json:"field"`
	Strategy string `json:"strategy"` // fixed_string | null | partial
	Value    string `json:"value,omitempty"`
	KeepLast int    `json:"keep_last,omitempty"`
}

// SourceConfig is the configuration of a single data source.
type SourceConfig struct {
	ID               string         `json:"id"`
	Type             SourceType     `json:"type"`
	Version          ESVersion      `json:"version,omitempty"`
	DeploymentMode   DeploymentMode `json:"deployment_mode,omitempty"`
	Addresses        []string       `json:"addresses,omitempty"`
	Brokers          []string       `json:"brokers,omitempty"`
	Username         string         `json:"username,omitempty"`
	Password         string         `json:"password,omitempty"`
	APIKey           string         `json:"api_key,omitempty"`
	CloudID          string         `json:"cloud_id,omitempty"`
	DB               int            `json:"db,omitempty"`
	Mode             RedisMode      `json:"mode,omitempty"`
	DefaultIndex     string         `json:"default_index,omitempty"`
	DefaultTopic     string         `json:"default_topic,omitempty"`
	ReadOnly         bool           `json:"read_only"`
	MaxDocs          int            `json:"max_docs,omitempty"`
	MaxKeys          int            `json:"max_keys,omitempty"`
	MaxMessages      int            `json:"max_messages,omitempty"`
	SkipTLSVerify    bool           `json:"skip_tls_verify,omitempty"`
	TLSEnabled       bool           `json:"tls_enabled,omitempty"`
	CACert           string         `json:"ca_cert,omitempty"`
	SASLMechanism    string         `json:"sasl_mechanism,omitempty"`
	QueryTimeout     int            `json:"query_timeout,omitempty"`
	CommandTimeout   int            `json:"command_timeout,omitempty"`
	OperationTimeout int            `json:"operation_timeout,omitempty"`
	MaskingRules     []MaskingRule  `json:"masking_rules,omitempty"`
	Description      string         `json:"description,omitempty"`
}

// Config is the root configuration object.
type Config struct {
	Sources []SourceConfig `json:"sources"`
}

// placeholderRe matches ${VAR} style environment variable references.
var placeholderRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load reads the configuration file at path, resolves ${VAR} placeholders
// against environment variables (after loading the optional .env file) and
// validates the result.
func Load(path string) (*Config, error) {
	// A missing .env file is ignored; real environment variables still apply.
	_ = godotenv.Load()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}

	resolved, err := resolvePlaceholders(data)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(resolved, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file %q: %w", path, err)
	}

	configDir := filepath.Dir(path)
	if err := cfg.validate(configDir); err != nil {
		return nil, err
	}

	cfg.applyDefaults()

	return &cfg, nil
}

// resolvePlaceholders replaces every ${VAR} reference with the corresponding
// environment variable value. A missing variable is a fail-closed error.
func resolvePlaceholders(data []byte) ([]byte, error) {
	var resolveErr error
	out := placeholderRe.ReplaceAllFunc(data, func(match []byte) []byte {
		name := placeholderRe.FindSubmatch(match)[1]
		value, ok := os.LookupEnv(string(name))
		if !ok {
			resolveErr = fmt.Errorf("environment variable %q referenced in config is not set", name)
			return match
		}
		return []byte(value)
	})
	if resolveErr != nil {
		return nil, resolveErr
	}
	return out, nil
}

// validate verifies structural invariants before the server starts.
func (c *Config) validate(configDir string) error {
	if len(c.Sources) == 0 {
		return fmt.Errorf("config contains no data sources")
	}

	seen := make(map[string]bool, len(c.Sources))
	for i := range c.Sources {
		s := &c.Sources[i]

		if strings.TrimSpace(s.ID) == "" {
			return fmt.Errorf("source at index %d has empty id", i)
		}
		if seen[s.ID] {
			return fmt.Errorf("duplicate source id %q", s.ID)
		}
		seen[s.ID] = true

		switch s.Type {
		case TypeElasticsearch:
			if err := validateES(s); err != nil {
				return fmt.Errorf("source %q: %w", s.ID, err)
			}
		case TypeRedis:
			if err := validateRedis(s); err != nil {
				return fmt.Errorf("source %q: %w", s.ID, err)
			}
		case TypeKafka:
			if err := validateKafka(s); err != nil {
				return fmt.Errorf("source %q: %w", s.ID, err)
			}
		default:
			return fmt.Errorf("source %q: unsupported type %q", s.ID, s.Type)
		}

		for _, rule := range s.MaskingRules {
			if strings.TrimSpace(rule.Field) == "" {
				return fmt.Errorf("source %q: masking rule has empty field", s.ID)
			}
			switch rule.Strategy {
			case "fixed_string", "null", "partial":
			default:
				return fmt.Errorf("source %q: unknown masking strategy %q", s.ID, rule.Strategy)
			}
		}

		// Relative CA certificate paths are resolved against the config directory.
		if s.CACert != "" && !filepath.IsAbs(s.CACert) {
			s.CACert = filepath.Join(configDir, s.CACert)
		}
	}

	return nil
}

func validateKafka(s *SourceConfig) error {
	if len(s.Brokers) == 0 {
		return fmt.Errorf("brokers must not be empty")
	}
	switch s.DeploymentMode {
	case ModeSingle:
		if len(s.Brokers) != 1 {
			return fmt.Errorf("deployment_mode %q requires exactly one broker, got %d", ModeSingle, len(s.Brokers))
		}
	case ModeCluster:
		// One or more bootstrap brokers are accepted; discovery adds the rest.
	case "":
		return fmt.Errorf("deployment_mode must be %q or %q", ModeSingle, ModeCluster)
	default:
		return fmt.Errorf("invalid deployment_mode %q (allowed: single, cluster)", s.DeploymentMode)
	}
	return nil
}

func validateRedis(s *SourceConfig) error {
	if len(s.Addresses) == 0 {
		return fmt.Errorf("addresses must not be empty")
	}
	switch s.Mode {
	case "", RedisModeAuto, RedisModeSingle, RedisModeCluster:
		return nil
	default:
		return fmt.Errorf("invalid redis mode %q (allowed: auto, single, cluster)", s.Mode)
	}
}

func validateES(s *SourceConfig) error {
	if strings.TrimSpace(s.CloudID) != "" {
		// Elastic Cloud handles its own endpoint and discovery.
		s.DeploymentMode = ModeCluster
	} else {
		if len(s.Addresses) == 0 {
			return fmt.Errorf("addresses or cloud_id must be provided")
		}
		switch s.DeploymentMode {
		case ModeSingle:
			if len(s.Addresses) != 1 {
				return fmt.Errorf("deployment_mode %q requires exactly one address, got %d", ModeSingle, len(s.Addresses))
			}
		case ModeCluster:
			// One or more seed nodes are accepted; discovery adds the rest.
		default:
			return fmt.Errorf("deployment_mode must be %q or %q", ModeSingle, ModeCluster)
		}
	}
	switch s.Version {
	case "", ESVersionAuto, ESVersionV7, ESVersionV8:
		return nil
	default:
		return fmt.Errorf("invalid es version %q (allowed: auto, 7, 8)", s.Version)
	}
}

// applyDefaults fills in per-source limits and timeouts when they are unset.
func (c *Config) applyDefaults() {
	for i := range c.Sources {
		s := &c.Sources[i]
		if s.MaxDocs == 0 {
			s.MaxDocs = 200
		}
		if s.MaxKeys == 0 {
			s.MaxKeys = 500
		}
		if s.MaxMessages == 0 {
			s.MaxMessages = 100
		}
		if s.QueryTimeout == 0 {
			s.QueryTimeout = 30
		}
		if s.CommandTimeout == 0 {
			s.CommandTimeout = 5
		}
		if s.OperationTimeout == 0 {
			s.OperationTimeout = 10
		}
		if s.Version == "" {
			s.Version = ESVersionAuto
		}
		if s.Type == TypeRedis && s.Mode == "" {
			s.Mode = RedisModeAuto
		}
	}
}
