package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"

	"github.com/TUSIDENG/no-sql-mcp/internal/config"
)

// Cmd returns the underlying command interface.
func (c *Client) Cmd() goredis.Cmdable {
	return c.cmd
}

// Raw returns the concrete go-redis client (*goredis.Client or
// *goredis.ClusterClient) for operations not available on Cmdable.
func (c *Client) Raw() any {
	return c.cmd
}

// Mode returns the resolved deployment mode.
func (c *Client) Mode() config.RedisMode {
	return c.mode
}

// Ping verifies connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.cmd.Ping(ctx).Err()
}

// Close releases the client.
func (c *Client) Close() error {
	return c.cmd.Close()
}
