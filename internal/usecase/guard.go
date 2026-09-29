// Package usecase contains business logic: per-kind operations, read-only
// guards, audit, truncation and masking.
package usecase

import (
	"fmt"
	"strings"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// Operation identifies a guarded operation in source-agnostic terms.
type Operation struct {
	Kind   domain.Kind
	Name   string // operation/endpoint/command name, e.g. "search", "SET"
	Write  bool   // whether the operation mutates data
}

// Guard enforces read-only operation whitelists and the dangerous-command
// blacklist. It is the single classification entry point before any adapter
// call.
type Guard struct {
	// allowDangerous permits operations on the dangerous blacklist when true.
	// It defaults to false and must be enabled explicitly.
	allowDangerous bool
}

// NewGuard creates a Guard.
func NewGuard(allowDangerous bool) *Guard {
	return &Guard{allowDangerous: allowDangerous}
}

// Check validates the operation against the operation whitelist, the source
// read-only flag and the dangerous-operation blacklist. Unknown operations
// are rejected by default.
func (g *Guard) Check(src domain.DataSource, op Operation) error {
	if !isKnown(op) {
		return fmt.Errorf("operation %q rejected: not in the allowed operation list", op.Name)
	}

	if src.IsReadOnly() && op.Write {
		return fmt.Errorf("operation %q rejected: source %q is read-only", op.Name, src.ID())
	}

	if isDangerous(op) && !g.allowDangerous {
		return fmt.Errorf("operation %q rejected: dangerous operations are disabled by default", op.Name)
	}

	return nil
}

// isKnown reports whether op is part of the per-kind operation whitelist.
func isKnown(op Operation) bool {
	switch op.Kind {
	case domain.KindES:
		return esAllowedOps[op.Name]
	default:
		// Redis and Kafka whitelists are introduced with their milestones.
		return true
	}
}

// esAllowedOps is the Elasticsearch application-layer whitelist. Only these
// operations are ever issued; endpoints such as _bulk, _update_by_query and
// _delete_by_query are intentionally absent.
var esAllowedOps = map[string]bool{
	"search":         true,
	"get":            true,
	"index":          true,
	"delete":         true,
	"list_indices":   true,
	"mapping":        true,
	"cluster_health": true,
}

// CheckRedisCommand validates a raw Redis command against the read
// classification table, read-only flag and dangerous blacklist. Commands not
// present in the classification table are denied by default.
func (g *Guard) CheckRedisCommand(src domain.DataSource, command string) error {
	cmd := strings.ToUpper(strings.TrimSpace(command))
	if cmd == "" {
		return fmt.Errorf("empty redis command")
	}

	write, known := redisCommands[cmd]
	if !known {
		return fmt.Errorf("redis command %q rejected: command is not classified (deny by default)", cmd)
	}

	return g.Check(src, Operation{Kind: domain.KindRedis, Name: cmd, Write: write})
}

// redisCommands is the single classification table for Redis commands: command
// -> whether it mutates data. Every allowed command must be listed here.
var redisCommands = map[string]bool{
	// Read-only commands.
	"GET": false, "MGET": false, "STRLEN": false, "APPEND": true,
	"TYPE": false, "TTL": false, "PTTL": false, "EXISTS": false,
	"OBJECT": false, "SCAN": false,
	"HGET": false, "HMGET": false, "HGETALL": false, "HKEYS": false, "HVALS": false, "HLEN": false, "HSCAN": false,
	"LRANGE": false, "LLEN": false, "LINDEX": false,
	"SMEMBERS": false, "SISMEMBER": false, "SCARD": false, "SSCAN": false, "SRANDMEMBER": false,
	"ZRANGE": false, "ZRANGEBYSCORE": false, "ZRANK": false, "ZSCORE": false, "ZCARD": false, "ZSCAN": false,
	"XRANGE": false, "XREVRANGE": false, "XLEN": false, "XINFO": false,
	"INFO": false, "DBSIZE": false, "TIME": false, "PING": false, "ECHO": false,
	// Write commands (blocked on read-only sources).
	"SET": true, "MSET": true, "SETEX": true, "SETNX": true, "GETSET": true, "INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true,
	"DEL": true, "UNLINK": true, "EXPIRE": true, "PEXPIRE": true, "PERSIST": true, "RENAME": true, "COPY": true,
	"HSET": true, "HMSET": true, "HDEL": true, "HINCRBY": true,
	"RPUSH": true, "LPUSH": true, "RPOP": true, "LPOP": true, "LSET": true, "LREM": true,
	"SADD": true, "SREM": true, "SPOP": true,
	"ZADD": true, "ZREM": true, "ZINCRBY": true,
	"XADD": true, "XDEL": true, "XACK": true,
}

// dangerousRedisCommands are blocked even on writable sources unless
// dangerous operations are explicitly enabled.
var dangerousRedisCommands = map[string]bool{
	"FLUSHALL": true,
	"FLUSHDB":  true,
	"CONFIG":   true,
	"KEYS":     true,
	"EVAL":     true,
	"EVALSHA":  true,
	"SHUTDOWN": true,
	"DEBUG":    true,
}

// isDangerous reports whether the operation is irreversible or cluster-scoped
// and must be denied unless explicitly enabled.
func isDangerous(op Operation) bool {
	switch op.Kind {
	case domain.KindES:
		return op.Name == "_shutdown"
	case domain.KindRedis:
		return dangerousRedisCommands[strings.ToUpper(op.Name)]
	case domain.KindKafka:
		return op.Name == "delete_topic"
	}
	return false
}
