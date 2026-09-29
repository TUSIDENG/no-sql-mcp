package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// ESLimitProvider exposes per-source limits without coupling the usecase to
// configuration or concrete client types. Elasticsearch adapters implement it.
type ESLimitProvider interface {
	ESMaxDocs() int
	ESTimeoutSec() int
}

// ESUseCase implements Elasticsearch business operations: guard checks,
// timeouts and per-source limits are applied here before the adapter is
// invoked.
type ESUseCase struct {
	guard *Guard
}

// NewESUseCase creates an ESUseCase.
func NewESUseCase(guard *Guard) *ESUseCase {
	return &ESUseCase{guard: guard}
}

// Search executes an Elasticsearch search.
func (uc *ESUseCase) Search(ctx context.Context, src domain.DataSource, in domain.SearchInput) (*domain.SearchResult, error) {
	ctx2, cancel, err := uc.prepare(ctx, src, "search", false, &in.Size)
	if err != nil {
		return nil, err
	}
	defer cancel()

	searchable, err := asSearchable(src)
	if err != nil {
		return nil, err
	}
	return searchable.Search(ctx2, in)
}

// GetDoc fetches a single document.
func (uc *ESUseCase) GetDoc(ctx context.Context, src domain.DataSource, index, docID string) (map[string]any, error) {
	ctx2, cancel, err := uc.prepare(ctx, src, "get", false, nil)
	if err != nil {
		return nil, err
	}
	defer cancel()

	searchable, err := asSearchable(src)
	if err != nil {
		return nil, err
	}
	return searchable.GetDoc(ctx2, index, docID)
}

// IndexDoc writes a single document.
func (uc *ESUseCase) IndexDoc(ctx context.Context, src domain.DataSource, in domain.IndexInput) error {
	ctx2, cancel, err := uc.prepare(ctx, src, "index", true, nil)
	if err != nil {
		return err
	}
	defer cancel()

	searchable, err := asSearchable(src)
	if err != nil {
		return err
	}
	return searchable.IndexDoc(ctx2, in)
}

// DeleteDoc deletes a document.
func (uc *ESUseCase) DeleteDoc(ctx context.Context, src domain.DataSource, index, docID string) error {
	ctx2, cancel, err := uc.prepare(ctx, src, "delete", true, nil)
	if err != nil {
		return err
	}
	defer cancel()

	searchable, err := asSearchable(src)
	if err != nil {
		return err
	}
	return searchable.DeleteDoc(ctx2, index, docID)
}

// ListIndices lists indices matching pattern.
func (uc *ESUseCase) ListIndices(ctx context.Context, src domain.DataSource, pattern string) ([]domain.IndexInfo, error) {
	ctx2, cancel, err := uc.prepare(ctx, src, "list_indices", false, nil)
	if err != nil {
		return nil, err
	}
	defer cancel()

	searchable, err := asSearchable(src)
	if err != nil {
		return nil, err
	}
	return searchable.ListIndices(ctx2, pattern)
}

// GetMapping returns an index mapping.
func (uc *ESUseCase) GetMapping(ctx context.Context, src domain.DataSource, index string) (map[string]any, error) {
	ctx2, cancel, err := uc.prepare(ctx, src, "mapping", false, nil)
	if err != nil {
		return nil, err
	}
	defer cancel()

	searchable, err := asSearchable(src)
	if err != nil {
		return nil, err
	}
	return searchable.GetMapping(ctx2, index)
}

// ClusterHealth returns cluster health.
func (uc *ESUseCase) ClusterHealth(ctx context.Context, src domain.DataSource) (domain.ClusterHealth, error) {
	ctx2, cancel, err := uc.prepare(ctx, src, "cluster_health", false, nil)
	if err != nil {
		return domain.ClusterHealth{}, err
	}
	defer cancel()

	searchable, err := asSearchable(src)
	if err != nil {
		return domain.ClusterHealth{}, err
	}
	return searchable.ClusterHealth(ctx2)
}

// prepare performs guard checks, enforces the result-size limit and attaches
// the query timeout.
func (uc *ESUseCase) prepare(ctx context.Context, src domain.DataSource, opName string, write bool, size *int) (context.Context, context.CancelFunc, error) {
	if err := uc.guard.Check(src, Operation{Kind: domain.KindES, Name: opName, Write: write}); err != nil {
		return nil, nil, err
	}

	maxDocs := 0
	timeoutSec := 30
	if provider, ok := src.(ESLimitProvider); ok {
		maxDocs = provider.ESMaxDocs()
		timeoutSec = provider.ESTimeoutSec()
	}

	if size != nil && maxDocs > 0 && *size > maxDocs {
		// Clamp instead of rejecting so the caller still receives bounded data.
		*size = maxDocs
	}

	ctx2, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	return ctx2, cancel, nil
}

func asSearchable(src domain.DataSource) (domain.Searchable, error) {
	searchable, ok := src.(domain.Searchable)
	if !ok {
		return nil, fmt.Errorf("source %q does not support elasticsearch operations", src.ID())
	}
	return searchable, nil
}
