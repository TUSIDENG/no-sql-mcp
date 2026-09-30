// Package redis provides connection management for single-node and clustered
// Redis deployments.
package redis

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/TUSIDENG/no-sql-mcp/internal/config"
)

// universalCmd is the command surface shared by single-node and cluster
// clients. Both *goredis.Client and *goredis.ClusterClient implement it.
type universalCmd interface {
	goredis.Cmdable
	Close() error
}

// Client wraps the concrete go-redis client and the resolved deployment mode.
type Client struct {
	cmd  universalCmd
	mode config.RedisMode
}

// New builds a Client from configuration. In auto mode it probes the target:
// a successful CLUSTER INFO request selects the cluster client, otherwise the
// single-node client.
func New(cfg config.SourceConfig) (*Client, error) {
	opts, err := buildNodeOptions(cfg.Addresses[0], cfg)
	if err != nil {
		return nil, err
	}

	switch cfg.DeploymentMode {
	case config.ModeSingle:
		return &Client{cmd: goredis.NewClient(opts), mode: config.RedisModeSingle}, nil
	case config.ModeCluster:
		addrs, err := clusterAddrs(cfg.Addresses, opts)
		if err != nil {
			return nil, err
		}
		return &Client{
			cmd:  goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs, Username: opts.Username, Password: opts.Password, TLSConfig: opts.TLSConfig}),
			mode: config.RedisModeCluster,
		}, nil
	default:
		return nil, fmt.Errorf("unknown redis deployment mode %q", cfg.DeploymentMode)
	}
}

// buildNodeOptions parses a redis:// or rediss:// URL and applies explicit
// configuration overrides.
func buildNodeOptions(rawURL string, cfg config.SourceConfig) (*goredis.Options, error) {
	opts := &goredis.Options{}

	if strings.HasPrefix(rawURL, "redis://") || strings.HasPrefix(rawURL, "rediss://") {
		parsed, err := goredis.ParseURL(rawURL)
		if err != nil {
			return nil, fmt.Errorf("parse redis address %q: %w", rawURL, err)
		}
		opts = parsed
	} else {
		opts.Addr = rawURL
	}

	if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	opts.DB = cfg.DB
	opts.TLSConfig = buildTLS(cfg)

	return opts, nil
}

// clusterAddrs normalizes every address into host:port form. URL schemes and
// paths are stripped because cluster options expect bare addresses.
func clusterAddrs(addresses []string, opts *goredis.Options) ([]string, error) {
	out := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		if strings.HasPrefix(addr, "redis://") || strings.HasPrefix(addr, "rediss://") {
			parsed, err := url.Parse(addr)
			if err != nil {
				return nil, fmt.Errorf("parse redis cluster address %q: %w", addr, err)
			}
			host := parsed.Host
			if !strings.Contains(host, ":") {
				host += ":6379"
			}
			out = append(out, host)
		} else {
			if !strings.Contains(addr, ":") {
				addr += ":6379"
			}
			out = append(out, addr)
		}
	}
	_ = opts
	return out, nil
}

// buildTLS constructs the TLS configuration from source settings.
func buildTLS(cfg config.SourceConfig) *tls.Config {
	var tlsCfg *tls.Config

	if strings.HasPrefix(cfg.Addresses[0], "rediss://") || cfg.TLSEnabled {
		tlsCfg = &tls.Config{}
	}
	if cfg.SkipTLSVerify {
		if tlsCfg == nil {
			tlsCfg = &tls.Config{}
		}
		tlsCfg.InsecureSkipVerify = true
	}
	if cfg.CACert != "" {
		if tlsCfg == nil {
			tlsCfg = &tls.Config{}
		}
		pem, err := os.ReadFile(cfg.CACert)
		if err == nil {
			pool := x509.NewCertPool()
			if pool.AppendCertsFromPEM(pem) {
				tlsCfg.RootCAs = pool
			}
		}
	}
	return tlsCfg
}
