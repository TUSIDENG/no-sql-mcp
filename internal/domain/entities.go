package domain

// SearchInput is the input of an Elasticsearch search operation.
type SearchInput struct {
	Indices        []string
	QueryDSL       map[string]any // native ES Query DSL as JSON
	From           int
	Size           int
	Sort           []string
	SourceIncludes []string // _source filtering to reduce payload size
}

// SearchResult is the result of an Elasticsearch search operation.
type SearchResult struct {
	Total     int64
	TookMs    int64
	Hits      []map[string]any // matched documents (_source plus _id/_index/_score)
	Truncated bool
}

// IndexInput is the input of a single document write.
type IndexInput struct {
	Index     string
	DocID     string // empty means auto-generated id
	Document  map[string]any
}

// BulkItem is one item of a bulk request.
type BulkItem struct {
	DocID    string
	Document map[string]any
}

// BulkInput is the input of a bulk write.
type BulkInput struct {
	Index string
	Items []BulkItem
}

// BulkResult summarizes a bulk write.
type BulkResult struct {
	Succeeded int
	Failed    int
}

// IndexInfo describes an Elasticsearch index.
type IndexInfo struct {
	Name     string
	Docs     int64
	StoreSize string
	Health   string
}

// ClusterHealth summarizes Elasticsearch cluster health.
type ClusterHealth struct {
	Status        string // green | yellow | red
	NodeCount     int
	IndexCount    int
	ActiveShards  int
}

// RedisValue wraps a Redis value together with its type and TTL.
type RedisValue struct {
	Type  string // string/hash/list/set/zset/stream/none
	Value any
	TTL   int64
}

// TopicInfo describes a Kafka topic.
type TopicInfo struct {
	Name       string
	Partitions int
	Replicas   int
}

// PartitionDetail describes one partition of a topic.
type PartitionDetail struct {
	Partition int
	Leader    int
	ISR       []int
	Earliest  int64
	Latest    int64
}

// TopicDetail describes a Kafka topic in detail.
type TopicDetail struct {
	Name       string
	Partitions []PartitionDetail
}

// ConsumerGroupInfo describes a Kafka consumer group.
type ConsumerGroupInfo struct {
	Name string
	Lag  int64
}

// PartitionOffset reports a group offset on one partition.
type PartitionOffset struct {
	Partition int
	Offset    int64
	Lag       int64
}

// ProduceInput is the input of a produce operation.
type ProduceInput struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Partition int // -1 means automatic partitioning
}

// ProduceResult is the result of a produce operation.
type ProduceResult struct {
	Partition int
	Offset    int64
}

// ConsumeInput is the input of a bounded consume operation.
type ConsumeInput struct {
	Topic       string
	Group       string
	Partition   int
	Offset      int64 // -2 earliest, -1 latest, or an absolute offset
	MaxMessages int
	TimeoutMs   int
	Commit      bool // commit group offset; disabled in read-only mode
}

// Message is one Kafka record.
type Message struct {
	Topic     string
	Group     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp int64
}

// ClusterInfo describes a Kafka cluster.
type ClusterInfo struct {
	Brokers    []int
	Controller int
}
