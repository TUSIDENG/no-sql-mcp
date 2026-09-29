// Package repository implements the domain DataSourceRepository over the
// clients connection Manager.
package repository

import (
	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
	"github.com/TUSIDENG/no-sql-mcp/pkg/clients"
)

// DataSourceRepository resolves sources through the clients Manager.
type DataSourceRepository struct {
	manager *clients.Manager
}

// New creates a DataSourceRepository backed by the given Manager.
func New(manager *clients.Manager) *DataSourceRepository {
	return &DataSourceRepository{manager: manager}
}

// Get returns the source with the given id.
func (r *DataSourceRepository) Get(id string) (domain.DataSource, error) {
	return r.manager.Get(id)
}

// List returns every configured source id.
func (r *DataSourceRepository) List() []string {
	return r.manager.List()
}

// GetKind returns the kind of the given source.
func (r *DataSourceRepository) GetKind(id string) (domain.Kind, error) {
	return r.manager.GetKind(id)
}

// ReadOnly returns the configured read-only flag of a source without
// establishing a connection.
func (r *DataSourceRepository) ReadOnly(id string) bool {
	return r.manager.ReadOnly(id)
}

// IsLazyLoading reports whether lazy connection mode is enabled.
func (r *DataSourceRepository) IsLazyLoading() bool {
	return r.manager.IsLazyLoading()
}
