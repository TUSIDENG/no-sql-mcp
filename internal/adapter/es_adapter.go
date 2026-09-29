// Package adapter adapts concrete data source clients to the domain
// interfaces.
package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
	"github.com/TUSIDENG/no-sql-mcp/pkg/clients/es"
)

// ESAdapter adapts an Elasticsearch connection to domain.Searchable. Version
// differences are hidden inside the es client; this adapter is version
// neutral.
type ESAdapter struct {
	conn *es.Client
}

// NewESAdapter creates an ESAdapter.
func NewESAdapter(conn *es.Client) *ESAdapter {
	return &ESAdapter{conn: conn}
}

// --- domain.DataSource ---

func (a *ESAdapter) ID() string                     { return a.conn.SourceConfig().ID }
func (a *ESAdapter) Kind() domain.Kind              { return domain.KindES }
func (a *ESAdapter) Connect() error                 { return a.conn.Connect() }
func (a *ESAdapter) Close() error                   { return a.conn.Close() }
func (a *ESAdapter) Ping(ctx context.Context) error { return a.conn.Ping(ctx) }
func (a *ESAdapter) IsReadOnly() bool               { return a.conn.SourceConfig().ReadOnly }

// ESMaxDocs returns the per-source document limit.
func (a *ESAdapter) ESMaxDocs() int { return a.conn.SourceConfig().MaxDocs }

// ESTimeoutSec returns the per-source query timeout in seconds.
func (a *ESAdapter) ESTimeoutSec() int { return a.conn.SourceConfig().QueryTimeout }

// do builds and executes a request and returns the response when its status
// code is below 400.
func (a *ESAdapter) do(ctx context.Context, method, path string, query map[string]string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	if len(query) > 0 {
		q := req.URL.Query()
		for k, v := range query {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	resp, err := a.conn.Perform(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer func() { _ = resp.Body.Close() }()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("elasticsearch returned %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	return resp, nil
}

// Search executes a native Query DSL search.
func (a *ESAdapter) Search(ctx context.Context, in domain.SearchInput) (*domain.SearchResult, error) {
	body := map[string]any{}
	if in.QueryDSL != nil {
		body["query"] = in.QueryDSL
	}
	if in.From > 0 {
		body["from"] = in.From
	}
	if in.Size > 0 {
		body["size"] = in.Size
	}
	if len(in.Sort) > 0 {
		body["sort"] = buildSort(in.Sort)
	}
	if len(in.SourceIncludes) > 0 {
		body["_source"] = in.SourceIncludes
	}

	resp, err := a.do(ctx, http.MethodPost, joinIndicesPath(in.Indices, "_search"), nil, body)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var parsed struct {
		Took int64 `json:"took"`
		Hits struct {
			Total *struct {
				Value    int64  `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			Hits []struct {
				Index  string         `json:"_index"`
				ID     string         `json:"_id"`
				Score  *float64       `json:"_score"`
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}

	hits := make([]map[string]any, 0, len(parsed.Hits.Hits))
	for _, h := range parsed.Hits.Hits {
		doc := h.Source
		if doc == nil {
			doc = map[string]any{}
		}
		doc["_id"] = h.ID
		doc["_index"] = h.Index
		if h.Score != nil {
			doc["_score"] = *h.Score
		}
		hits = append(hits, doc)
	}

	result := &domain.SearchResult{TookMs: parsed.Took, Hits: hits}
	if parsed.Hits.Total != nil {
		result.Total = parsed.Hits.Total.Value
	}

	maxDocs := a.conn.SourceConfig().MaxDocs
	if maxDocs > 0 && len(hits) > maxDocs {
		result.Hits = hits[:maxDocs]
		result.Truncated = true
	}
	return result, nil
}

// GetDoc fetches a single document by id. It returns (nil, nil) when the
// document does not exist.
func (a *ESAdapter) GetDoc(ctx context.Context, index, docID string) (map[string]any, error) {
	resp, err := a.do(ctx, http.MethodGet, joinPath(index, "_doc", docID), nil, nil)
	if err != nil {
		if strings.Contains(err.Error(), "404 Not Found") {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var parsed struct {
		Found  bool           `json:"found"`
		Source map[string]any `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode get response: %w", err)
	}
	if !parsed.Found {
		return nil, nil
	}
	return parsed.Source, nil
}

// IndexDoc writes or updates a single document.
func (a *ESAdapter) IndexDoc(ctx context.Context, in domain.IndexInput) error {
	segments := []string{in.Index, "_doc"}
	if in.DocID != "" {
		segments = append(segments, in.DocID)
	}
	resp, err := a.do(ctx, http.MethodPost, joinPath(segments...), nil, in.Document)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// BulkIndex writes multiple documents in one bulk request.
func (a *ESAdapter) BulkIndex(ctx context.Context, in domain.BulkInput) (domain.BulkResult, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, item := range in.Items {
		meta := map[string]any{"index": map[string]any{"_index": in.Index}}
		if item.DocID != "" {
			meta["index"].(map[string]any)["_id"] = item.DocID
		}
		if err := enc.Encode(meta); err != nil {
			return domain.BulkResult{}, err
		}
		if err := enc.Encode(item.Document); err != nil {
			return domain.BulkResult{}, err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/_bulk", &buf)
	if err != nil {
		return domain.BulkResult{}, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := a.conn.Perform(req)
	if err != nil {
		return domain.BulkResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return domain.BulkResult{}, fmt.Errorf("elasticsearch returned %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}

	var parsed struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				Error *json.RawMessage `json:"error"`
			} `json:"index"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return domain.BulkResult{}, fmt.Errorf("decode bulk response: %w", err)
	}

	result := domain.BulkResult{}
	for _, item := range parsed.Items {
		if item.Index.Error != nil {
			result.Failed++
		} else {
			result.Succeeded++
		}
	}
	return result, nil
}

// DeleteDoc removes a document by id.
func (a *ESAdapter) DeleteDoc(ctx context.Context, index, docID string) error {
	resp, err := a.do(ctx, http.MethodDelete, joinPath(index, "_doc", docID), nil, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// ListIndices lists indices matching pattern in _cat/indices style.
func (a *ESAdapter) ListIndices(ctx context.Context, pattern string) ([]domain.IndexInfo, error) {
	if pattern == "" {
		pattern = "*"
	}
	resp, err := a.do(ctx, http.MethodGet, "/_cat/indices/"+pattern, map[string]string{"format": "json"}, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var rows []struct {
		Health    string `json:"health"`
		Index     string `json:"index"`
		DocsCount string `json:"docs.count"`
		StoreSize string `json:"store.size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, fmt.Errorf("decode cat indices response: %w", err)
	}

	out := make([]domain.IndexInfo, 0, len(rows))
	for _, r := range rows {
		info := domain.IndexInfo{Name: r.Index, StoreSize: r.StoreSize, Health: r.Health}
		fmt.Sscanf(r.DocsCount, "%d", &info.Docs)
		out = append(out, info)
	}
	return out, nil
}

// GetMapping returns the mapping of an index.
func (a *ESAdapter) GetMapping(ctx context.Context, index string) (map[string]any, error) {
	if index == "" {
		index = "_all"
	}
	resp, err := a.do(ctx, http.MethodGet, joinPath(index, "_mapping"), nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var mappings map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&mappings); err != nil {
		return nil, fmt.Errorf("decode mapping response: %w", err)
	}
	return mappings, nil
}

// ClusterHealth returns cluster health together with node and shard counts.
func (a *ESAdapter) ClusterHealth(ctx context.Context) (domain.ClusterHealth, error) {
	resp, err := a.do(ctx, http.MethodGet, "/_cluster/health", nil, nil)
	if err != nil {
		return domain.ClusterHealth{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	var parsed struct {
		Status        string `json:"status"`
		NumberOfNodes int    `json:"number_of_nodes"`
		ActiveShards  int    `json:"active_shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return domain.ClusterHealth{}, fmt.Errorf("decode cluster health response: %w", err)
	}

	indicesResp, err := a.do(ctx, http.MethodGet, "/_cat/indices", map[string]string{"format": "json"}, nil)
	if err != nil {
		return domain.ClusterHealth{}, err
	}
	defer func() { _ = indicesResp.Body.Close() }()
	var indices []map[string]any
	_ = json.NewDecoder(indicesResp.Body).Decode(&indices)

	return domain.ClusterHealth{
		Status:       parsed.Status,
		NodeCount:    parsed.NumberOfNodes,
		IndexCount:   len(indices),
		ActiveShards: parsed.ActiveShards,
	}, nil
}

// buildSort converts shorthand sort entries such as "price:asc" into the
// Elasticsearch object form {"price":{"order":"asc"}}. Entries without a colon
// are passed through unchanged.
func buildSort(sort []string) []any {
	out := make([]any, 0, len(sort))
	for _, s := range sort {
		if field, order, ok := strings.Cut(s, ":"); ok && order != "" {
			out = append(out, map[string]any{field: map[string]any{"order": order}})
		} else {
			out = append(out, s)
		}
	}
	return out
}

// joinPath joins path segments with single slashes.
func joinPath(segments ...string) string {
	var parts []string
	for _, seg := range segments {
		if s := strings.Trim(seg, "/"); s != "" {
			parts = append(parts, s)
		}
	}
	return "/" + strings.Join(parts, "/")
}

// joinIndicesPath joins index names with extra path segments.
func joinIndicesPath(indices []string, segments ...string) string {
	all := make([]string, 0, len(indices)+len(segments))
	all = append(all, indices...)
	all = append(all, segments...)
	return joinPath(all...)
}
