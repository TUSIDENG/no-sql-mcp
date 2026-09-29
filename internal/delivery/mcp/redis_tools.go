package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/cortex/pkg/types"

	"github.com/no-sql-mcp/server/internal/domain"
)

// redisToolCount is the number of tools registered per Redis source.
const redisToolCount = 6

// registerRedisTools registers the Redis tools for every configured Redis
// source. It is self-contained, mirroring registerESTools.
func (r *ToolRegistry) registerRedisTools(ctx context.Context, mcpServer *server.MCPServer) error {
	sources, err := r.sourceUC.ListSources()
	if err != nil {
		return fmt.Errorf("list sources for redis tool registration: %w", err)
	}

	for _, s := range sources {
		if s.Kind != domain.KindRedis {
			continue
		}
		if err := r.registerRedisToolsForSource(ctx, mcpServer, s.ID, s.ReadOnly); err != nil {
			return err
		}
	}
	return nil
}

// registerRedisToolsForSource registers the 6 Redis tools for one source.
func (r *ToolRegistry) registerRedisToolsForSource(ctx context.Context, mcpServer *server.MCPServer, sourceID string, readOnly bool) error {
	note := readOnlyNote(readOnly)

	get := tools.NewTool("redis_get_"+sourceID,
		tools.WithDescription("Get the value, type and TTL of a Redis key. "+note),
		tools.WithString("key", tools.Description("Redis key"), tools.Required()))

	scan := tools.NewTool("redis_scan_"+sourceID,
		tools.WithDescription("Scan Redis keys matching a glob pattern using SCAN (KEYS is never used). "+note),
		tools.WithString("pattern", tools.Description("Glob pattern, default *")),
		tools.WithNumber("limit", tools.Description("Maximum number of keys to return, default 100")))

	typeTool := tools.NewTool("redis_type_"+sourceID,
		tools.WithDescription("Get the type and TTL of a Redis key. "+note),
		tools.WithString("key", tools.Description("Redis key"), tools.Required()))

	data := tools.NewTool("redis_data_"+sourceID,
		tools.WithDescription("Read structured data via a whitelisted command: HGETALL, LRANGE, SMEMBERS, ZRANGE, XRANGE. "+note),
		tools.WithString("command", tools.Description("Read command, default HGETALL")),
		tools.WithArray("args", tools.Description("Structured command arguments"),
			tools.Items(map[string]any{"type": "string"})))

	set := tools.NewTool("redis_set_"+sourceID,
		tools.WithDescription("Write data via a whitelisted command such as SET, HSET or LPUSH. "+note),
		tools.WithString("command", tools.Description("Write command, default SET"), tools.Required()),
		tools.WithArray("args", tools.Description("Structured command arguments"), tools.Required(),
			tools.Items(map[string]any{"type": "string"})))

	info := tools.NewTool("redis_info_"+sourceID,
		tools.WithDescription("Get Redis server INFO output. "+note),
		tools.WithString("section", tools.Description("Optional INFO section, e.g. server, memory")))

	specs := []*types.Tool{get, scan, typeTool, data, set, info}
	handlers := []server.ToolHandler{
		r.redisGetHandler(sourceID),
		r.redisScanHandler(sourceID),
		r.redisTypeHandler(sourceID),
		r.redisDataHandler(sourceID),
		r.redisSetHandler(sourceID),
		r.redisInfoHandler(sourceID),
	}

	for i := range specs {
		if err := mcpServer.AddTool(ctx, specs[i], handlers[i]); err != nil {
			return fmt.Errorf("register %s: %w", specs[i].Name, err)
		}
	}
	return nil
}

func readOnlyNote(readOnly bool) string {
	if readOnly {
		return "This source is read-only: write commands are rejected."
	}
	return "Commands that are not classified are rejected by default."
}

// --- Handlers ---

func (r *ToolRegistry) redisGetHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		key, ok := req.Parameters["key"].(string)
		if !ok || strings.TrimSpace(key) == "" {
			return ErrorContent(sourceID, "redis_get", "parameter 'key' is required"), nil
		}

		val, err := r.redisUC.Get(ctx, sourceID, key)
		if err != nil {
			return ErrorContent(sourceID, "redis_get", err.Error()), nil
		}
		return TextContent(formatRedisValue(key, val)), nil
	}
}

func (r *ToolRegistry) redisScanHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		pattern, _ := req.Parameters["pattern"].(string)
		if pattern == "" {
			pattern = "*"
		}
		limit := intParam(req.Parameters, "limit", 100)

		keys, err := r.redisUC.Scan(ctx, sourceID, pattern, limit)
		if err != nil {
			return ErrorContent(sourceID, "redis_scan", err.Error()), nil
		}
		return TextContent(formatKeyList(keys)), nil
	}
}

func (r *ToolRegistry) redisTypeHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		key, ok := req.Parameters["key"].(string)
		if !ok || strings.TrimSpace(key) == "" {
			return ErrorContent(sourceID, "redis_type", "parameter 'key' is required"), nil
		}

		val, err := r.redisUC.Type(ctx, sourceID, key)
		if err != nil {
			return ErrorContent(sourceID, "redis_type", err.Error()), nil
		}
		return TextContent(fmt.Sprintf("key: %s\ntype: %s\nttl_seconds: %d", key, val.Type, val.TTL)), nil
	}
}

func (r *ToolRegistry) redisDataHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		command, _ := req.Parameters["command"].(string)
		if command == "" {
			command = "HGETALL"
		}
		args := stringSliceParam(req.Parameters, "args")

		result, err := r.redisUC.Data(ctx, sourceID, command, toAnySlice(args))
		if err != nil {
			return ErrorContent(sourceID, "redis_data", err.Error()), nil
		}
		return TextContent(formatAnyResult(command, result)), nil
	}
}

func (r *ToolRegistry) redisSetHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		command, _ := req.Parameters["command"].(string)
		if command == "" {
			command = "SET"
		}
		args := stringSliceParam(req.Parameters, "args")

		result, err := r.redisUC.Set(ctx, sourceID, command, toAnySlice(args))
		if err != nil {
			return ErrorContent(sourceID, "redis_set", err.Error()), nil
		}
		return TextContent(fmt.Sprintf("command %s completed: %s", strings.ToUpper(command), formatScalar(result))), nil
	}
}

func (r *ToolRegistry) redisInfoHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		section, _ := req.Parameters["section"].(string)

		out, err := r.redisUC.Info(ctx, sourceID, section)
		if err != nil {
			return ErrorContent(sourceID, "redis_info", err.Error()), nil
		}
		return TextContent(out), nil
	}
}

// --- Formatting helpers ---

func formatRedisValue(key string, val domain.RedisValue) string {
	encoded, err := json.MarshalIndent(val.Value, "", "  ")
	valueText := string(encoded)
	if err != nil {
		valueText = fmt.Sprintf("%v", val.Value)
	}
	return fmt.Sprintf("key: %s\ntype: %s\nttl_seconds: %d\nvalue:\n%s", key, val.Type, val.TTL, valueText)
}

func formatKeyList(keys []string) string {
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "Found %d key(s):\n", len(keys))
	for _, k := range keys {
		fmt.Fprintf(&b, "- %s\n", k)
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatAnyResult(command string, result any) string {
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", result)
	}
	return fmt.Sprintf("%s result:\n%s", strings.ToUpper(command), string(encoded))
}

func formatScalar(v any) string {
	if v == nil {
		return "OK"
	}
	return fmt.Sprintf("%v", v)
}
