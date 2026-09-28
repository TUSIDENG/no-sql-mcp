// Package domain defines the core interfaces and entities of the data source
// abstraction. It must not depend on any outer layer or concrete clients.
package domain

import (
	"context"
	"errors"
)

// ErrDataSourceNotFound is returned when a source id is not registered.
var ErrDataSourceNotFound = errors.New("data source not found")

// Kind identifies the kind of a data source.
type Kind string

const (
	KindES    Kind = "elasticsearch"
	KindRedis Kind = "redis"
	KindKafka Kind = "kafka"
)

// DataSource is the minimal common abstraction implemented by every source.
// Kind-specific capabilities are expressed by the dedicated interfaces and
// callers assert them by Kind.
type DataSource interface {
	ID() string
	Kind() Kind
	Connect() error
	Close() error
	Ping(ctx context.Context) error
	IsReadOnly() bool
}

// Searchable is implemented by Elasticsearch adapters.
type Searchable interface {
	Search(ctx context.Context, in SearchInput) (*SearchResult, error)
	GetDoc(ctx context.Context, index, docID string) (map[string]any, error)
	IndexDoc(ctx context.Context, in IndexInput) error
	BulkIndex(ctx context.Context, in BulkInput) (BulkResult, error)
	DeleteDoc(ctx context.Context, index, docID string) error
	ListIndices(ctx context.Context, pattern string) ([]IndexInfo, error)
	GetMapping(ctx context.Context, index string) (map[string]any, error)
	ClusterHealth(ctx context.Context) (ClusterHealth, error)
}

// KVStore is implemented by Redis adapters.
type KVStore interface {
	Get(ctx context.Context, key string) (RedisValue, error)
	Set(ctx context.Context, key string, value any, ttlSec int) error
	Del(ctx context.Context, keys []string) (int64, error)
	Exists(ctx context.Context, keys []string) (int64, error)
	Expire(ctx context.Context, key string, ttlSec int) error
	Keys(ctx context.Context, pattern string, limit int) ([]string, error)
	Type(ctx context.Context, key string) (string, error)
	// Generic executes a whitelisted Redis command with structured arguments,
	// covering string/hash/list/set/zset/stream/pubsub data structures.
	Generic(ctx context.Context, cmd string, args []any) (any, error)
	Info(ctx context.Context, section string) (string, error)
}

// Messaging is implemented by Kafka adapters.
type Messaging interface {
	ListTopics(ctx context.Context) ([]TopicInfo, error)
	TopicDetail(ctx context.Context, topic string) (TopicDetail, error)
	ListConsumerGroups(ctx context.Context) ([]ConsumerGroupInfo, error)
	Produce(ctx context.Context, in ProduceInput) (ProduceResult, error)
	Consume(ctx context.Context, in ConsumeInput) ([]Message, error)
	GroupOffsets(ctx context.Context, group string) ([]PartitionOffset, error)
	ClusterInfo(ctx context.Context) (ClusterInfo, error)
}

// DataSourceRepository retrieves registered sources.
type DataSourceRepository interface {
	Get(id string) (DataSource, error)
	List() []string
	GetKind(id string) (Kind, error)
	// ReadOnly returns the configured read-only flag without connecting.
	ReadOnly(id string) bool
	IsLazyLoading() bool
}
