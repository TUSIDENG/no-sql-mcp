package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// esTool describes one Elasticsearch tool family generated per source.
type esTool struct {
	suffix string
	desc   string
	params []tools.ToolOption
	handle func(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error)
}

// registerESTools registers the Elasticsearch tools for every ES source.
func (r *ToolRegistry) registerESTools(ctx context.Context, mcpServer *server.MCPServer) error {
	sources, err := r.sourceUC.ListSources()
	if err != nil {
		return fmt.Errorf("list sources for es tool registration: %w", err)
	}

	specs := r.esToolSpecs()
	for _, s := range sources {
		if s.Kind != domain.KindES {
			continue
		}
		sourceID := s.ID

		for _, spec := range specs {
			name := "es_" + spec.suffix + "_" + sourceID
			desc := spec.desc + fmt.Sprintf(" Source id: %q. Available sources (run list_sources for the full list).", sourceID)
			if s.ReadOnly {
				desc += " This source is read-only: write operations are rejected."
			}

			tool := tools.NewTool(name, append(spec.params, tools.WithDescription(desc))...)
			spec := spec
			handler := func(ctx context.Context, req server.ToolCallRequest) (any, error) {
				src, err := r.sourceUC.Get(sourceID)
				if err != nil {
					return ErrorContent(sourceID, spec.suffix, err.Error()), nil
				}
				out, err := spec.handle(ctx, src, req)
				if err != nil {
					return ErrorContent(sourceID, spec.suffix, err.Error()), nil
				}
				return out, nil
			}

			if err := mcpServer.AddTool(ctx, tool, handler); err != nil {
				return fmt.Errorf("register %s: %w", name, err)
			}
		}
	}
	return nil
}

func (r *ToolRegistry) esToolSpecs() []esTool {
	return []esTool{
		{
			suffix: "search",
			desc:   "Execute a native Elasticsearch Query DSL search and return matched documents.",
			params: []tools.ToolOption{
				tools.WithArray("indices",
					tools.Description("Target index names or patterns. Defaults to the source default index."),
					tools.Items(map[string]any{"type": "string"})),
				tools.WithObject("query_dsl",
					tools.Description("Elasticsearch Query DSL as a JSON object, e.g. {\"match\": {\"title\": \"book\"}}."),
					tools.Required()),
				tools.WithNumber("from", tools.Description("Pagination offset (default 0).")),
				tools.WithNumber("size", tools.Description("Maximum number of documents to return (bounded by max_docs).")),
				tools.WithArray("sort",
					tools.Description("Sort expressions, e.g. [\"created_at:desc\"]."),
					tools.Items(map[string]any{"type": "string"})),
				tools.WithArray("_source",
					tools.Description("Restrict returned source fields to this list."),
					tools.Items(map[string]any{"type": "string"})),
			},
			handle: r.handleESSearch,
		},
		{
			suffix: "get",
			desc:   "Get a single Elasticsearch document by index and document id.",
			params: []tools.ToolOption{
				tools.WithString("index", tools.Description("Target index name."), tools.Required()),
				tools.WithString("doc_id", tools.Description("Document id (_id)."), tools.Required()),
			},
			handle: r.handleESGet,
		},
		{
			suffix: "indices",
			desc:   "List Elasticsearch indices (_cat/indices style), optionally filtered by a pattern.",
			params: []tools.ToolOption{
				tools.WithString("pattern", tools.Description("Index name pattern, e.g. \"app-logs-*\" (default \"*\").")),
			},
			handle: r.handleESIndices,
		},
		{
			suffix: "mapping",
			desc:   "Get the mapping (field definitions) of an Elasticsearch index.",
			params: []tools.ToolOption{
				tools.WithString("index", tools.Description("Target index name or pattern (default all indices).")),
			},
			handle: r.handleESMapping,
		},
		{
			suffix: "cluster",
			desc:   "Get Elasticsearch cluster health, node/index counts and active shards.",
			handle: r.handleESCluster,
		},
		{
			suffix: "index",
			desc:   "Write or update a single Elasticsearch document. Rejected on read-only sources.",
			params: []tools.ToolOption{
				tools.WithString("index", tools.Description("Target index name."), tools.Required()),
				tools.WithString("doc_id", tools.Description("Document id; omit to let Elasticsearch generate one.")),
				tools.WithObject("document", tools.Description("Document body as a JSON object."), tools.Required()),
			},
			handle: r.handleESIndex,
		},
		{
			suffix: "delete",
			desc:   "Delete a single Elasticsearch document by index and document id. Rejected on read-only sources.",
			params: []tools.ToolOption{
				tools.WithString("index", tools.Description("Target index name."), tools.Required()),
				tools.WithString("doc_id", tools.Description("Document id (_id)."), tools.Required()),
			},
			handle: r.handleESDelete,
		},
	}
}

// --- handlers ---

func (r *ToolRegistry) handleESSearch(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error) {
	in, err := parseSearchInput(req.Parameters)
	if err != nil {
		return nil, err
	}

	result, err := r.esUC.Search(ctx, src, in)
	if err != nil {
		return nil, err
	}
	return TextContent(formatSearchResult(result)), nil
}

func (r *ToolRegistry) handleESGet(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error) {
	index, err := requireString(req.Parameters, "index")
	if err != nil {
		return nil, err
	}
	docID, err := requireString(req.Parameters, "doc_id")
	if err != nil {
		return nil, err
	}

	doc, err := r.esUC.GetDoc(ctx, src, index, docID)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return TextContent(fmt.Sprintf("Document %q not found in index %q.", docID, index)), nil
	}
	return TextContent(formatJSON(fmt.Sprintf("Document %q in index %q:", docID, index), doc)), nil
}

func (r *ToolRegistry) handleESIndices(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error) {
	pattern, _ := optionalString(req.Parameters, "pattern")

	indices, err := r.esUC.ListIndices(ctx, src, pattern)
	if err != nil {
		return nil, err
	}
	return TextContent(formatIndices(indices)), nil
}

func (r *ToolRegistry) handleESMapping(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error) {
	index, _ := optionalString(req.Parameters, "index")

	mappings, err := r.esUC.GetMapping(ctx, src, index)
	if err != nil {
		return nil, err
	}
	return TextContent(formatJSON("Mappings:", mappings)), nil
}

func (r *ToolRegistry) handleESCluster(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error) {
	health, err := r.esUC.ClusterHealth(ctx, src)
	if err != nil {
		return nil, err
	}
	return TextContent(formatHealth(health)), nil
}

func (r *ToolRegistry) handleESIndex(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error) {
	index, err := requireString(req.Parameters, "index")
	if err != nil {
		return nil, err
	}
	docID, _ := optionalString(req.Parameters, "doc_id")
	document, err := requireObject(req.Parameters, "document")
	if err != nil {
		return nil, err
	}

	in := domain.IndexInput{Index: index, DocID: docID, Document: document}
	if err := r.esUC.IndexDoc(ctx, src, in); err != nil {
		return nil, err
	}
	return TextContent(fmt.Sprintf("Document indexed successfully in %q (doc_id=%q).", index, docID)), nil
}

func (r *ToolRegistry) handleESDelete(ctx context.Context, src domain.DataSource, req server.ToolCallRequest) (any, error) {
	index, err := requireString(req.Parameters, "index")
	if err != nil {
		return nil, err
	}
	docID, err := requireString(req.Parameters, "doc_id")
	if err != nil {
		return nil, err
	}

	if err := r.esUC.DeleteDoc(ctx, src, index, docID); err != nil {
		return nil, err
	}
	return TextContent(fmt.Sprintf("Document %q deleted from index %q.", docID, index)), nil
}

// --- parameter parsing ---

func parseSearchInput(params map[string]any) (domain.SearchInput, error) {
	var in domain.SearchInput

	indices, err := optionalStringSlice(params, "indices")
	if err != nil {
		return in, err
	}
	in.Indices = indices

	dsl, err := requireObject(params, "query_dsl")
	if err != nil {
		return in, err
	}
	in.QueryDSL = dsl

	in.From, _ = optionalInt(params, "from")
	in.Size, _ = optionalInt(params, "size")

	sortExpr, err := optionalStringSlice(params, "sort")
	if err != nil {
		return in, err
	}
	in.Sort = sortExpr

	source, err := optionalStringSlice(params, "_source")
	if err != nil {
		return in, err
	}
	in.SourceIncludes = source

	return in, nil
}

func requireString(params map[string]any, name string) (string, error) {
	value, ok := params[name]
	if !ok {
		return "", fmt.Errorf("missing required parameter %q", name)
	}
	str, ok := value.(string)
	if !ok || strings.TrimSpace(str) == "" {
		return "", fmt.Errorf("parameter %q must be a non-empty string", name)
	}
	return str, nil
}

func optionalString(params map[string]any, name string) (string, error) {
	value, ok := params[name]
	if !ok || value == nil {
		return "", nil
	}
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("parameter %q must be a string", name)
	}
	return str, nil
}

func requireObject(params map[string]any, name string) (map[string]any, error) {
	value, ok := params[name]
	if !ok || value == nil {
		return nil, fmt.Errorf("missing required parameter %q", name)
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("parameter %q must be a JSON object", name)
	}
	return obj, nil
}

func optionalInt(params map[string]any, name string) (int, error) {
	value, ok := params[name]
	if !ok || value == nil {
		return 0, nil
	}
	switch n := value.(type) {
	case float64:
		return int(n), nil
	case int:
		return n, nil
	default:
		return 0, fmt.Errorf("parameter %q must be a number", name)
	}
}

func optionalStringSlice(params map[string]any, name string) ([]string, error) {
	value, ok := params[name]
	if !ok || value == nil {
		return nil, nil
	}
	raw, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("parameter %q must be an array", name)
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		str, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("parameter %q must contain only strings", name)
		}
		out = append(out, str)
	}
	return out, nil
}

// --- output formatting ---

func formatSearchResult(result *domain.SearchResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Took %d ms, total hits: %d, returned: %d\n", result.TookMs, result.Total, len(result.Hits))
	if result.Truncated {
		fmt.Fprintln(&b, "[Truncated] result exceeds the per-source limit.")
	}
	for i, hit := range result.Hits {
		fmt.Fprintf(&b, "\n--- hit %d ---\n", i+1)
		fmt.Fprintf(&b, "%s\n", mustJSON(hit))
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatIndices(indices []domain.IndexInfo) string {
	if len(indices) == 0 {
		return "No indices found."
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i].Name < indices[j].Name })

	var b strings.Builder
	fmt.Fprintln(&b, "Index | Health | Docs | StoreSize")
	for _, idx := range indices {
		fmt.Fprintf(&b, "%s | %s | %d | %s\n", idx.Name, idx.Health, idx.Docs, idx.StoreSize)
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatHealth(health domain.ClusterHealth) string {
	return fmt.Sprintf(
		"Cluster health: %s\nNodes: %d\nIndices: %d\nActive shards: %d",
		health.Status, health.NodeCount, health.IndexCount, health.ActiveShards)
}

func formatJSON(header string, value any) string {
	return header + "\n" + mustJSON(value)
}

func mustJSON(value any) string {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(raw)
}
