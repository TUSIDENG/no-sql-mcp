package main

import (
	"log"

	"github.com/TUSIDENG/no-sql-mcp/internal/adapter"
	"github.com/TUSIDENG/no-sql-mcp/internal/config"
	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
	"github.com/TUSIDENG/no-sql-mcp/pkg/clients"
	"github.com/TUSIDENG/no-sql-mcp/pkg/clients/es"
	kafkaclient "github.com/TUSIDENG/no-sql-mcp/pkg/clients/kafka"
	redisclient "github.com/TUSIDENG/no-sql-mcp/pkg/clients/redis"
)

// registeredFactories returns the client factories available in this build.
func registeredFactories(logger *log.Logger) map[config.SourceType]clients.Factory {
	return map[config.SourceType]clients.Factory{
		config.TypeElasticsearch: func(cfg config.SourceConfig) (domain.DataSource, error) {
			return newESSource(cfg, logger)
		},
		config.TypeRedis: newRedisSource,
		config.TypeKafka: newKafkaSource,
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

// newKafkaSource builds the Kafka client and wraps it in the adapter. Both
// single-broker and cluster deployments are handled by the same client.
func newKafkaSource(cfg config.SourceConfig) (domain.DataSource, error) {
	client, err := kafkaclient.New(cfg)
	if err != nil {
		return nil, err
	}
	return adapter.NewKafkaAdapter(cfg, client), nil
}
