package mcp

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"

	"github.com/no-sql-mcp/server/internal/usecase"
)

// ToolRegistry builds and registers MCP tools on a cortex server.
type ToolRegistry struct {
	logger   *log.Logger
	sourceUC *usecase.DataSourceUseCase
	esUC     *usecase.ESUseCase
	redisUC  *usecase.RedisUseCase
}

// NewToolRegistry creates a ToolRegistry.
func NewToolRegistry(logger *log.Logger, sourceUC *usecase.DataSourceUseCase, esUC *usecase.ESUseCase, redisUC *usecase.RedisUseCase) *ToolRegistry {
	return &ToolRegistry{logger: logger, sourceUC: sourceUC, esUC: esUC, redisUC: redisUC}
}

// Register registers the global and per-source tools on the given cortex
// server.
func (r *ToolRegistry) Register(ctx context.Context, mcpServer *server.MCPServer) error {
	if err := r.registerListSources(ctx, mcpServer); err != nil {
		return err
	}
	if err := r.registerESTools(ctx, mcpServer); err != nil {
		return err
	}
	if err := r.registerRedisTools(ctx, mcpServer); err != nil {
		return err
	}
	return nil
}

func (r *ToolRegistry) registerListSources(ctx context.Context, mcpServer *server.MCPServer) error {
	listTool := tools.NewTool("list_sources",
		tools.WithDescription(
			"List all configured data source IDs and their kinds. No credentials are returned."),
	)

	handler := func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		sources, err := r.sourceUC.ListSources()
		if err != nil {
			return ErrorContent("", "list_sources", err.Error()), nil
		}
		return TextContent(formatSources(sources)), nil
	}

	if err := mcpServer.AddTool(ctx, listTool, handler); err != nil {
		return fmt.Errorf("register list_sources: %w", err)
	}
	return nil
}

// formatSources renders source summaries as stable, readable text.
func formatSources(sources []usecase.SourceSummary) string {
	sort.Slice(sources, func(i, j int) bool { return sources[i].ID < sources[j].ID })

	var b strings.Builder
	fmt.Fprintf(&b, "Found %d data source(s):\n", len(sources))
	for _, s := range sources {
		fmt.Fprintf(&b, "- %s (%s, read_only=%t)\n", s.ID, s.Kind, s.ReadOnly)
	}
	return strings.TrimRight(b.String(), "\n")
}
