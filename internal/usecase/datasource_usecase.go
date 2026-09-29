package usecase

import (
	"fmt"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// SourceSummary is the safe, credential-free view of a source returned to AI
// clients.
type SourceSummary struct {
	ID       string
	Kind     domain.Kind
	ReadOnly bool
}

// DataSourceUseCase is the facade for source discovery and dispatch.
type DataSourceUseCase struct {
	repo domain.DataSourceRepository
}

// NewDataSourceUseCase creates a DataSourceUseCase.
func NewDataSourceUseCase(repo domain.DataSourceRepository) *DataSourceUseCase {
	return &DataSourceUseCase{repo: repo}
}

// ListSources returns safe summaries of every configured source.
func (uc *DataSourceUseCase) ListSources() ([]SourceSummary, error) {
	ids := uc.repo.List()
	out := make([]SourceSummary, 0, len(ids))
	for _, id := range ids {
		kind, err := uc.repo.GetKind(id)
		if err != nil {
			return nil, fmt.Errorf("resolve kind of source %q: %w", id, err)
		}

		// The read-only flag comes from configuration so that listing sources
		// never triggers a connection (important in lazy mode).
		out = append(out, SourceSummary{ID: id, Kind: kind, ReadOnly: uc.repo.ReadOnly(id)})
	}
	return out, nil
}

// Get resolves a source by id.
func (uc *DataSourceUseCase) Get(id string) (domain.DataSource, error) {
	return uc.repo.Get(id)
}
