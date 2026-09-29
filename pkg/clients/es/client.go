// Package es manages Elasticsearch connections for both single-instance and
// cluster deployments, with automatic version detection selecting between the
// go-elasticsearch v7 and v8 client libraries.
package es

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	v7 "github.com/elastic/go-elasticsearch/v7"
	v8 "github.com/elastic/go-elasticsearch/v8"

	"github.com/TUSIDENG/no-sql-mcp/internal/config"
)

// nodeDiscoveryInterval is how often a cluster client refreshes the node list
// through the sniffing mechanism.
const nodeDiscoveryInterval = 10 * time.Minute

// Client is a connected Elasticsearch source. It owns exactly one of the v7 or
// v8 concrete clients based on the detected major version.
type Client struct {
	cfg       config.SourceConfig
	logger    *log.Logger
	transport http.RoundTripper

	majorVersion int
	version      string
	clientV7     *v7.Client
	clientV8     *v8.Client
}

// New creates an unconnected Elasticsearch client.
func New(cfg config.SourceConfig, logger *log.Logger) (*Client, error) {
	tr, err := buildTransport(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, logger: logger, transport: tr}, nil
}

// Connect detects the server version (unless pinned by configuration) and
// builds the matching concrete client. It is safe to call multiple times.
func (c *Client) Connect() error {
	if c.clientV7 != nil || c.clientV8 != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.cfg.QueryTimeout)*time.Second)
	defer cancel()

	detected, full, err := c.probeVersion(ctx)
	if err != nil {
		if c.cfg.Version == config.ESVersionAuto {
			// Fail closed: never guess a client on an undeterminable cluster.
			return fmt.Errorf("probe elasticsearch version: %w", err)
		}
		c.logger.Printf("source %q: version probe failed (%v); falling back to pinned version %s", c.cfg.ID, err, c.cfg.Version)
	} else {
		c.version = full
	}

	switch c.cfg.Version {
	case config.ESVersionV7:
		c.majorVersion = 7
	case config.ESVersionV8:
		c.majorVersion = 8
	default:
		c.majorVersion = detected
	}

	if detected != 0 && ((c.cfg.Version == config.ESVersionV7 && detected != 7) ||
		(c.cfg.Version == config.ESVersionV8 && detected != 8)) {
		c.logger.Printf("source %q: WARNING configured version %s disagrees with detected version %q", c.cfg.ID, c.cfg.Version, full)
	}

	switch c.majorVersion {
	case 7:
		cl, err := v7.NewClient(*c.v7Config())
		if err != nil {
			return fmt.Errorf("build v7 client: %w", err)
		}
		c.clientV7 = cl
	case 8:
		cl, err := v8.NewClient(*c.v8Config())
		if err != nil {
			return fmt.Errorf("build v8 client: %w", err)
		}
		c.clientV8 = cl
	default:
		return fmt.Errorf("unsupported elasticsearch major version %d (only 7.x and 8.x are supported)", c.majorVersion)
	}
	return nil
}

// probeVersion returns the major version and full version string reported by
// the server. It issues a root GET request directly so that the probe works
// before a version-specific client has been chosen.
func (c *Client) probeVersion(ctx context.Context) (int, string, error) {
	endpoint, err := c.probeEndpoint()
	if err != nil {
		return 0, "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, "", err
	}
	if c.cfg.Username != "" || c.cfg.Password != "" {
		req.SetBasicAuth(c.cfg.Username, c.cfg.Password)
	}
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "ApiKey "+c.cfg.APIKey)
	}

	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return 0, "", fmt.Errorf("info request failed: %s", resp.Status)
	}

	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return 0, "", fmt.Errorf("decode version info: %w", err)
	}

	major, err := parseMajor(info.Version.Number)
	if err != nil {
		return 0, "", err
	}
	return major, info.Version.Number, nil
}

// probeEndpoint builds the absolute root URL from the first configured
// address or the cloud endpoint.
func (c *Client) probeEndpoint() (string, error) {
	if len(c.cfg.Addresses) > 0 {
		return strings.TrimRight(c.cfg.Addresses[0], "/") + "/", nil
	}
	// Elastic Cloud exposes its endpoint through the decoded cloud id; building
	// it here would duplicate that logic, so rely on the v8 config error path.
	return "", fmt.Errorf("cannot probe: no address configured (cloud_id probing requires a version-pinned client)")
}

func parseMajor(number string) (int, error) {
	part := strings.SplitN(number, ".", 2)[0]
	major, err := strconv.Atoi(part)
	if err != nil {
		return 0, fmt.Errorf("parse version number %q: %w", number, err)
	}
	return major, nil
}

func buildTransport(cfg config.SourceConfig) (http.RoundTripper, error) {
	tlsCfg := &tls.Config{InsecureSkipVerify: cfg.SkipTLSVerify} //nolint:gosec // explicit operator opt-in
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read ca certificate %q: %w", cfg.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca certificate %q contains no valid PEM block", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	}
	return &http.Transport{TLSClientConfig: tlsCfg}, nil
}

func (c *Client) v7Config() *v7.Config {
	discoverNodes := c.cfg.DeploymentMode == config.ModeCluster
	if strings.TrimSpace(c.cfg.CloudID) != "" {
		return &v7.Config{
			CloudID:               c.cfg.CloudID,
			Username:              c.cfg.Username,
			Password:              c.cfg.Password,
			APIKey:                c.cfg.APIKey,
			Transport:             c.transport,
			DiscoverNodesInterval: nodeDiscoveryInterval,
		}
	}
	return &v7.Config{
		Addresses:             c.cfg.Addresses,
		Username:              c.cfg.Username,
		Password:              c.cfg.Password,
		APIKey:                c.cfg.APIKey,
		Transport:             c.transport,
		DiscoverNodesOnStart:  discoverNodes,
		DiscoverNodesInterval: nodeDiscoveryInterval,
	}
}

func (c *Client) v8Config() *v8.Config {
	discoverNodes := c.cfg.DeploymentMode == config.ModeCluster
	if strings.TrimSpace(c.cfg.CloudID) != "" {
		return &v8.Config{
			CloudID:               c.cfg.CloudID,
			Username:              c.cfg.Username,
			Password:              c.cfg.Password,
			APIKey:                c.cfg.APIKey,
			Transport:             c.transport,
			DiscoverNodesInterval: nodeDiscoveryInterval,
		}
	}
	return &v8.Config{
		Addresses:             c.cfg.Addresses,
		Username:              c.cfg.Username,
		Password:              c.cfg.Password,
		APIKey:                c.cfg.APIKey,
		Transport:             c.transport,
		DiscoverNodesOnStart:  discoverNodes,
		DiscoverNodesInterval: nodeDiscoveryInterval,
	}
}

// Ping verifies connectivity by requesting the root resource.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.Connect(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	if err != nil {
		return err
	}

	resp, err := c.Perform(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("ping failed: %s", resp.Status)
	}
	return nil
}

// Perform executes an HTTP request against Elasticsearch using the selected
// concrete client.
func (c *Client) Perform(req *http.Request) (*http.Response, error) {
	switch c.majorVersion {
	case 7:
		return c.clientV7.Perform(req)
	default:
		return c.clientV8.Perform(req)
	}
}

// Close releases the client. The go-elasticsearch clients keep no dedicated
// close hook beyond their idle connection pools.
func (c *Client) Close() error { return nil }

// Config accessors used by the adapter layer.

func (c *Client) SourceConfig() config.SourceConfig { return c.cfg }
func (c *Client) MajorVersion() int                 { return c.majorVersion }
func (c *Client) VersionString() string             { return c.version }
func (c *Client) V7() *v7.Client                    { return c.clientV7 }
func (c *Client) V8() *v8.Client                    { return c.clientV8 }
