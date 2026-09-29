package usecase

import (
	"context"
	"fmt"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// RedisUseCase implements Redis tool business logic. Every operation resolves
// the source, asserts the KVStore capability and runs the guard.
type RedisUseCase struct {
	repo  domain.DataSourceRepository
	guard *Guard
}

// NewRedisUseCase creates a RedisUseCase.
func NewRedisUseCase(repo domain.DataSourceRepository, guard *Guard) *RedisUseCase {
	return &RedisUseCase{repo: repo, guard: guard}
}

// kv resolves the source and validates the command through the guard.
func (uc *RedisUseCase) kv(sourceID, command string) (domain.KVStore, error) {
	src, err := uc.repo.Get(sourceID)
	if err != nil {
		return nil, err
	}
	if err := uc.guard.CheckRedisCommand(src, command); err != nil {
		return nil, err
	}
	store, ok := src.(domain.KVStore)
	if !ok {
		return nil, fmt.Errorf("source %q does not implement KVStore", sourceID)
	}
	return store, nil
}

// Get returns the value, type and TTL of a key.
func (uc *RedisUseCase) Get(ctx context.Context, sourceID, key string) (domain.RedisValue, error) {
	store, err := uc.kv(sourceID, "GET")
	if err != nil {
		return domain.RedisValue{}, err
	}
	return store.Get(ctx, key)
}

// Scan returns keys matching pattern, bounded by limit.
func (uc *RedisUseCase) Scan(ctx context.Context, sourceID, pattern string, limit int) ([]string, error) {
	store, err := uc.kv(sourceID, "SCAN")
	if err != nil {
		return nil, err
	}
	return store.Keys(ctx, pattern, limit)
}

// Type returns the type and TTL information of a key.
func (uc *RedisUseCase) Type(ctx context.Context, sourceID, key string) (domain.RedisValue, error) {
	store, err := uc.kv(sourceID, "TYPE")
	if err != nil {
		return domain.RedisValue{}, err
	}
	return store.Get(ctx, key)
}

// Data reads structured data through a whitelisted read command such as
// HGETALL, LRANGE, SMEMBERS, ZRANGE or XRANGE.
func (uc *RedisUseCase) Data(ctx context.Context, sourceID, command string, args []any) (any, error) {
	store, err := uc.kv(sourceID, command)
	if err != nil {
		return nil, err
	}
	return store.Generic(ctx, command, args)
}

// Set executes a whitelisted write command with structured arguments. The
// common SET case accepts a value and optional TTL; other commands are passed
// through as-is.
func (uc *RedisUseCase) Set(ctx context.Context, sourceID, command string, args []any) (any, error) {
	store, err := uc.kv(sourceID, command)
	if err != nil {
		return nil, err
	}
	return store.Generic(ctx, command, args)
}

// Info returns the server INFO output for an optional section.
func (uc *RedisUseCase) Info(ctx context.Context, sourceID, section string) (string, error) {
	store, err := uc.kv(sourceID, "INFO")
	if err != nil {
		return "", err
	}
	return store.Info(ctx, section)
}
