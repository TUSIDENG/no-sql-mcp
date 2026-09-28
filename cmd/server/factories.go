package main

import (
	"log"

	"github.com/no-sql-mcp/server/internal/adapter"
	"github.com/no-sql-mcp/server/internal/config"
	"github.com/no-sql-mcp/server/internal/domain"
	"github.com/no-sql-mcp/server/pkg/clients"
	"github.com/no-sql-mcp/server/pkg/clients/es"
	redisclient "github.com/no-sql-mcp/server/pkg/clients/redis"
)

// registeredFactories returns the client factories available in this build.
// The Kafka factory is introduced in a later milestone.
func registeredFactories(logger *log.Logger) map[config.SourceType]clients.Factory {
	return map[config.SourceType]clients.Factory{
		config.TypeElasticsearch: func(cfg config.SourceConfig) (domain.DataSource, error) {
			return newESSource(cfg, logger)
		},
		config.TypeRedis: newRedisSource,
		config.TypeKafka: func(cfg config.SourceConfig) (domain.DataSource, error) {
			return nil, errNotImplemented("kafka")
		},
	}
}

// newESSource builds the Elasticsearch client and wraps it in the adapter.
func newESSource(cfg config.SourceConfig, logger *log.Logger) (domain.DataSource, error) {
	conn, err := es.New(cfg, logger)
	if err != nil {
		return nil, err
	}
	return adapter.NewESAdapter(conn), nil
}

// newRedisSource builds and connects the Redis client, then wraps it in the
// adapter. Deployment mode (single/cluster) is resolved inside the client.
func newRedisSource(cfg config.SourceConfig) (domain.DataSource, error) {
	client, err := redisclient.New(cfg)
	if err != nil {
		return nil, err
	}
	return adapter.NewRedisAdapter(cfg, client), nil
}

type notImplementedError struct{ kind string }

func (e *notImplementedError) Error() string {
	return "client for " + e.kind + " is not implemented yet"
}

func errNotImplemented(kind string) error { return &notImplementedError{kind: kind} }
