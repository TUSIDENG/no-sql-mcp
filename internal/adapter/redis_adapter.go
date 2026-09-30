// Package adapter adapts concrete clients to domain interfaces.
package adapter

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/TUSIDENG/no-sql-mcp/internal/config"
	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
	redisclient "github.com/TUSIDENG/no-sql-mcp/pkg/clients/redis"
)

// RedisAdapter adapts the Redis client to domain.DataSource and domain.KVStore.
type RedisAdapter struct {
	id       string
	readOnly bool
	timeout  time.Duration
	client   *redisclient.Client
}

// NewRedisAdapter wraps an unconnected Redis client.
func NewRedisAdapter(cfg config.SourceConfig, client *redisclient.Client) *RedisAdapter {
	return &RedisAdapter{
		id:       cfg.ID,
		readOnly: cfg.ReadOnly,
		timeout:  time.Duration(cfg.CommandTimeout) * time.Second,
		client:   client,
	}
}

// --- domain.DataSource ---

func (a *RedisAdapter) ID() string        { return a.id }
func (a *RedisAdapter) Kind() domain.Kind { return domain.KindRedis }
func (a *RedisAdapter) Connect() error    { return nil }
func (a *RedisAdapter) Close() error      { return a.client.Close() }
func (a *RedisAdapter) IsReadOnly() bool  { return a.readOnly }

func (a *RedisAdapter) Ping(ctx context.Context) error {
	return a.client.Ping(ctx)
}

// ctx applies the per-command timeout.
func (a *RedisAdapter) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, a.timeout)
}

// --- domain.KVStore ---

// Get returns the value, type and TTL of a key.
func (a *RedisAdapter) Get(ctx context.Context, key string) (domain.RedisValue, error) {
	ctx, cancel := a.ctx(ctx)
	defer cancel()

	cmd := a.client.Cmd()
	typeName, err := cmd.Type(ctx, key).Result()
	if err != nil {
		return domain.RedisValue{}, err
	}

	ttl, err := cmd.TTL(ctx, key).Result()
	if err != nil {
		return domain.RedisValue{}, err
	}

	out := domain.RedisValue{Type: mapRedisType(typeName), TTL: int64(ttl.Seconds())}

	switch typeName {
	case "none":
		out.Value = nil
	case "string":
		out.Value, err = cmd.Get(ctx, key).Result()
	case "hash":
		out.Value, err = cmd.HGetAll(ctx, key).Result()
	case "list":
		out.Value, err = cmd.LRange(ctx, key, 0, -1).Result()
	case "set":
		out.Value, err = cmd.SMembers(ctx, key).Result()
	case "zset":
		out.Value, err = cmd.ZRangeWithScores(ctx, key, 0, -1).Result()
	case "stream":
		out.Value, err = cmd.XRange(ctx, key, "-", "+").Result()
	default:
		out.Value = nil
	}
	if err != nil {
		return domain.RedisValue{}, err
	}
	return out, nil
}

// Set writes a string value with an optional TTL. A zero or negative TTL keeps
// the key without expiration.
func (a *RedisAdapter) Set(ctx context.Context, key string, value any, ttlSec int) error {
	ctx, cancel := a.ctx(ctx)
	defer cancel()

	ttl := time.Duration(ttlSec) * time.Second
	if ttlSec <= 0 {
		ttl = 0
	}
	return a.client.Cmd().Set(ctx, key, value, ttl).Err()
}

// Del deletes keys and returns the number of removed keys.
func (a *RedisAdapter) Del(ctx context.Context, keys []string) (int64, error) {
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	return a.client.Cmd().Del(ctx, keys...).Result()
}

// Exists returns the number of existing keys among the given keys.
func (a *RedisAdapter) Exists(ctx context.Context, keys []string) (int64, error) {
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	return a.client.Cmd().Exists(ctx, keys...).Result()
}

// Expire sets a TTL on a key.
func (a *RedisAdapter) Expire(ctx context.Context, key string, ttlSec int) error {
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	return a.client.Cmd().Expire(ctx, key, time.Duration(ttlSec)*time.Second).Err()
}

// Keys performs a bounded SCAN matching pattern. KEYS is never used because it
// blocks the server. On cluster clients every master node is scanned.
func (a *RedisAdapter) Keys(ctx context.Context, pattern string, limit int) ([]string, error) {
	ctx, cancel := a.ctx(ctx)
	defer cancel()

	if limit <= 0 {
		limit = 100
	}

	seen := make(map[string]struct{})

	switch c := a.client.Raw().(type) {
	case *goredis.ClusterClient:
		err := c.ForEachMaster(ctx, func(ctx context.Context, shard *goredis.Client) error {
			return scanShard(ctx, shard, pattern, limit, seen)
		})
		if err != nil {
			return nil, err
		}
	case *goredis.Client:
		if err := scanShard(ctx, c, pattern, limit, seen); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported redis client type for scanning")
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func scanShard(ctx context.Context, shard *goredis.Client, pattern string, limit int, seen map[string]struct{}) error {
	var cursor uint64
	for {
		keys, next, err := shard.Scan(ctx, cursor, pattern, int64(limit)).Result()
		if err != nil {
			return err
		}
		for _, k := range keys {
			seen[k] = struct{}{}
		}
		cursor = next
		if cursor == 0 || len(seen) >= limit {
			return nil
		}
	}
}

// Type returns the type name of a key.
func (a *RedisAdapter) Type(ctx context.Context, key string) (string, error) {
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	return a.client.Cmd().Type(ctx, key).Result()
}

// Generic executes a command with structured arguments. The command must be
// classified and approved by the guard before reaching this method.
func (a *RedisAdapter) Generic(ctx context.Context, cmd string, args []any) (any, error) {
	ctx, cancel := a.ctx(ctx)
	defer cancel()

	parts := make([]any, 0, len(args)+1)
	parts = append(parts, cmd)
	parts = append(parts, args...)

	var res *goredis.Cmd
	switch c := a.client.Raw().(type) {
	case *goredis.Client:
		res = c.Do(ctx, parts...)
	case *goredis.ClusterClient:
		res = c.Do(ctx, parts...)
	default:
		return nil, fmt.Errorf("unsupported redis client type for generic commands")
	}
	if err := res.Err(); err != nil {
		return nil, err
	}
	return res.Val(), nil
}

// Info returns the server INFO output, optionally for one section.
func (a *RedisAdapter) Info(ctx context.Context, section string) (string, error) {
	ctx, cancel := a.ctx(ctx)
	defer cancel()
	if strings.TrimSpace(section) == "" {
		return a.client.Cmd().Info(ctx).Result()
	}
	return a.client.Cmd().Info(ctx, section).Result()
}

// mapRedisType maps the Redis TYPE response to the names used by the domain.
func mapRedisType(t string) string {
	switch t {
	case "string", "hash", "list", "set", "zset", "stream":
		return t
	case "none":
		return "none"
	default:
		return fmt.Sprintf("other:%s", t)
	}
}
