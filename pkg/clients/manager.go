// Package clients manages concrete data source connections indexed by source id.
package clients

import (
	"context"
	"fmt"
	"sync"

	"github.com/TUSIDENG/no-sql-mcp/internal/config"
	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// Factory builds a concrete DataSource from its configuration. Concrete client
// packages register their factory per source type.
type Factory func(cfg config.SourceConfig) (domain.DataSource, error)

// Manager indexes data source clients and their configurations by id.
type Manager struct {
	mu          sync.RWMutex
	sources     map[string]domain.DataSource
	configs     map[string]config.SourceConfig
	kinds       map[string]domain.Kind
	factories   map[config.SourceType]Factory
	lazyLoading bool
}

// NewManager creates a Manager for the given source configurations.
func NewManager(cfgs []config.SourceConfig, factories map[config.SourceType]Factory, lazyLoading bool) (*Manager, error) {
	m := &Manager{
		sources:     make(map[string]domain.DataSource),
		configs:     make(map[string]config.SourceConfig),
		kinds:       make(map[string]domain.Kind),
		factories:   factories,
		lazyLoading: lazyLoading,
	}

	for _, cfg := range cfgs {
		if _, ok := factories[cfg.Type]; !ok {
			return nil, fmt.Errorf("no client factory registered for source %q type %q", cfg.ID, cfg.Type)
		}
		m.configs[cfg.ID] = cfg
		m.kinds[cfg.ID] = kindOf(cfg.Type)
	}

	return m, nil
}

// Get returns the source with the given id, connecting on first use in lazy
// mode.
func (m *Manager) Get(id string) (domain.DataSource, error) {
	m.mu.RLock()
	src, ok := m.sources[id]
	m.mu.RUnlock()
	if ok {
		return src, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check under the write lock in case another goroutine connected first.
	if src, ok = m.sources[id]; ok {
		return src, nil
	}

	cfg, ok := m.configs[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrDataSourceNotFound, id)
	}

	factory := m.factories[cfg.Type]
	created, err := factory(cfg)
	if err != nil {
		return nil, fmt.Errorf("create client for source %q: %w", id, err)
	}
	if err := created.Connect(); err != nil {
		return nil, fmt.Errorf("connect source %q: %w", id, err)
	}
	m.sources[id] = created
	return created, nil
}

// ConnectAll eagerly connects every configured source and pings it.
func (m *Manager) ConnectAll() error {
	for _, id := range m.List() {
		src, err := m.Get(id)
		if err != nil {
			return err
		}
		if err := src.Ping(context.Background()); err != nil {
			return fmt.Errorf("ping source %q: %w", id, err)
		}
	}
	return nil
}

// List returns every configured source id.
func (m *Manager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.configs))
	for id := range m.configs {
		ids = append(ids, id)
	}
	return ids
}

// GetKind returns the kind of the given source.
func (m *Manager) GetKind(id string) (domain.Kind, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	kind, ok := m.kinds[id]
	if !ok {
		return "", fmt.Errorf("%w: %s", domain.ErrDataSourceNotFound, id)
	}
	return kind, nil
}

// ReadOnly returns the configured read-only flag of a source without
// establishing a connection.
func (m *Manager) ReadOnly(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cfg, ok := m.configs[id]
	return ok && cfg.ReadOnly
}

// IsLazyLoading reports whether lazy connection mode is enabled.
func (m *Manager) IsLazyLoading() bool {
	return m.lazyLoading
}

// Close closes every connected source. Repeated Close calls are safe.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for id, src := range m.sources {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close source %q: %w", id, err)
		}
	}
	m.sources = make(map[string]domain.DataSource)
	return firstErr
}

func kindOf(t config.SourceType) domain.Kind {
	switch t {
	case config.TypeElasticsearch:
		return domain.KindES
	case config.TypeRedis:
		return domain.KindRedis
	case config.TypeKafka:
		return domain.KindKafka
	default:
		return domain.Kind(t)
	}
}
