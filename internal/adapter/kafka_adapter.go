package adapter

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/TUSIDENG/no-sql-mcp/internal/config"
	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
	kafkaclient "github.com/TUSIDENG/no-sql-mcp/pkg/clients/kafka"
)

// KafkaAdapter adapts the Kafka client to domain.DataSource and domain.Messaging.
type KafkaAdapter struct {
	id           string
	readOnly     bool
	maxMessages  int
	timeout      time.Duration
	defaultTopic string
	client       *kafkaclient.Client
}

// NewKafkaAdapter wraps an unconnected Kafka client.
func NewKafkaAdapter(cfg config.SourceConfig, client *kafkaclient.Client) *KafkaAdapter {
	return &KafkaAdapter{
		id:           cfg.ID,
		readOnly:     cfg.ReadOnly,
		maxMessages:  cfg.MaxMessages,
		timeout:      time.Duration(cfg.OperationTimeout) * time.Second,
		defaultTopic: cfg.DefaultTopic,
		client:       client,
	}
}

// --- domain.DataSource ---

func (a *KafkaAdapter) ID() string        { return a.id }
func (a *KafkaAdapter) Kind() domain.Kind { return domain.KindKafka }
func (a *KafkaAdapter) Connect() error    { return nil }
func (a *KafkaAdapter) Close() error      { return a.client.Close() }
func (a *KafkaAdapter) IsReadOnly() bool  { return a.readOnly }

func (a *KafkaAdapter) Ping(ctx context.Context) error {
	return a.client.Ping(ctx)
}

// KafkaMaxMessages returns the per-source message limit.
func (a *KafkaAdapter) KafkaMaxMessages() int { return a.maxMessages }

// KafkaTimeoutSec returns the per-source operation timeout in seconds.
func (a *KafkaAdapter) KafkaTimeoutSec() int { return int(a.timeout / time.Second) }

// DefaultTopic returns the topic used when the caller does not name one.
func (a *KafkaAdapter) DefaultTopic() string { return a.defaultTopic }

// --- domain.Messaging ---

// ListTopics lists every topic with its partition and replica counts.
func (a *KafkaAdapter) ListTopics(ctx context.Context) ([]domain.TopicInfo, error) {
	topics, err := a.client.Topics(ctx, nil)
	if err != nil {
		return nil, err
	}

	out := make([]domain.TopicInfo, 0, len(topics))
	for _, t := range topics {
		replicas := 0
		if len(t.Partitions) > 0 {
			replicas = len(t.Partitions[0].Replicas)
		}
		out = append(out, domain.TopicInfo{Name: t.Name, Partitions: len(t.Partitions), Replicas: replicas})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// TopicDetail returns partition leaders, ISRs and first/last offsets.
func (a *KafkaAdapter) TopicDetail(ctx context.Context, topic string) (domain.TopicDetail, error) {
	partitions, offsets, err := a.client.TopicDetail(ctx, topic)
	if err != nil {
		return domain.TopicDetail{}, err
	}

	byID := make(map[int]kafka.PartitionOffsets, len(offsets))
	for _, o := range offsets {
		byID[o.Partition] = o
	}

	details := make([]domain.PartitionDetail, 0, len(partitions))
	for _, p := range partitions {
		isr := make([]int, 0, len(p.Isr))
		for _, replica := range p.Isr {
			isr = append(isr, replica.ID)
		}
		d := domain.PartitionDetail{Partition: p.ID, Leader: p.Leader.ID, ISR: isr}
		if o, ok := byID[p.ID]; ok {
			d.Earliest = o.FirstOffset
			d.Latest = o.LastOffset
		}
		details = append(details, d)
	}
	sort.Slice(details, func(i, j int) bool { return details[i].Partition < details[j].Partition })

	return domain.TopicDetail{Name: topic, Partitions: details}, nil
}

// ListConsumerGroups lists every group with its total lag.
func (a *KafkaAdapter) ListConsumerGroups(ctx context.Context) ([]domain.ConsumerGroupInfo, error) {
	groups, err := a.client.ListGroups(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]domain.ConsumerGroupInfo, 0, len(groups.Groups))
	for _, g := range groups.Groups {
		var lag int64
		offsets, err := a.client.GroupOffsets(ctx, g.GroupID)
		if err == nil {
			lag = a.totalLag(ctx, offsets)
		}
		out = append(out, domain.ConsumerGroupInfo{Name: g.GroupID, Lag: lag})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// totalLag sums the lag across all topic partitions of a group.
func (a *KafkaAdapter) totalLag(ctx context.Context, groupOffsets map[string][]kafka.OffsetFetchPartition) int64 {
	latest, err := a.latestFor(ctx, topicNames(groupOffsets), groupOffsets)
	if err != nil {
		return 0
	}

	var lag int64
	for topic, partitions := range groupOffsets {
		byPartition := make(map[int]int64, len(latest[topic]))
		for _, o := range latest[topic] {
			byPartition[o.Partition] = o.LastOffset
		}
		for _, p := range partitions {
			// An uncommitted partition reports -1 and contributes no lag.
			if p.CommittedOffset < 0 {
				continue
			}
			l := byPartition[p.Partition] - p.CommittedOffset
			if l > 0 {
				lag += l
			}
		}
	}
	return lag
}

// latestFor returns the last offsets for the partitions named in groupOffsets.
// Topics are validated against cluster metadata so that requests for unknown
// topics do not reach the broker.
func (a *KafkaAdapter) latestFor(ctx context.Context, topics []string, groupOffsets map[string][]kafka.OffsetFetchPartition) (map[string][]kafka.PartitionOffsets, error) {
	known, err := a.client.Topics(ctx, topics)
	if err != nil {
		return nil, err
	}

	requests := make(map[string][]int, len(known))
	for _, t := range known {
		partitions := make([]int, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			partitions = append(partitions, p.ID)
		}
		requests[t.Name] = partitions
	}
	return a.client.LatestOffsets(ctx, requests)
}

func topicNames(m map[string][]kafka.OffsetFetchPartition) []string {
	out := make([]string, 0, len(m))
	for topic := range m {
		out = append(out, topic)
	}
	return out
}

// Produce sends one message and returns the partition it was routed to.
func (a *KafkaAdapter) Produce(ctx context.Context, in domain.ProduceInput) (domain.ProduceResult, error) {
	msg := kafka.Message{
		Topic:   in.Topic,
		Key:     in.Key,
		Value:   in.Value,
		Headers: toKafkaHeaders(in.Headers),
	}
	if in.Partition >= 0 {
		msg.Partition = in.Partition
	} else {
		// An unset partition arrives as the zero value 0, which the client
		// would read as an explicit partition 0. Use -1 to request
		// automatic round-robin partition selection.
		msg.Partition = -1
	}

	sent, err := a.client.Produce(ctx, msg)
	if err != nil {
		return domain.ProduceResult{}, err
	}
	return domain.ProduceResult{Topic: in.Topic, Partition: sent.Partition}, nil
}

// Consume performs a bounded read. It never blocks beyond the supplied timeout
// and returns at most max messages. When a group is used together with
// commit=true the final offset is committed; otherwise offsets never advance.
func (a *KafkaAdapter) Consume(ctx context.Context, in domain.ConsumeInput) ([]domain.Message, error) {
	timeoutMs := in.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = int(a.timeout / time.Millisecond)
	}

	reader := a.client.NewReader(in.Topic, in.Group, in.Partition, in.Offset, in.Commit)
	defer func() { _ = reader.Close() }()

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	out := make([]domain.Message, 0, in.MaxMessages)

	for len(out) < in.MaxMessages {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		fetchCtx, cancel := context.WithTimeout(ctx, remaining)
		msg, err := reader.ReadMessage(fetchCtx)
		cancel()
		if err != nil {
			// A timeout or deadline error simply ends the bounded read with
			// the messages collected so far. Other errors (e.g. rebalance
			// failures) are surfaced instead of being silently swallowed.
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, fmt.Errorf("read kafka message: %w", err)
			}
			break
		}
		out = append(out, domain.Message{
			Topic:     msg.Topic,
			Group:     in.Group,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   fromKafkaHeaders(msg.Headers),
			Timestamp: msg.Time.UnixMilli(),
		})
	}

	if in.Commit && in.Group != "" && len(out) > 0 {
		last := out[len(out)-1]
		if err := reader.CommitMessages(ctx, kafka.Message{Topic: last.Topic, Partition: last.Partition, Offset: last.Offset}); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// GroupOffsets returns the committed offsets and per-partition lag of a group.
func (a *KafkaAdapter) GroupOffsets(ctx context.Context, group string) ([]domain.PartitionOffset, error) {
	fetched, err := a.client.GroupOffsets(ctx, group)
	if err != nil {
		return nil, err
	}

	topics := make([]string, 0, len(fetched))
	for topic := range fetched {
		topics = append(topics, topic)
	}
	latest, err := a.latestFor(ctx, topics, fetched)
	if err != nil {
		return nil, err
	}

	out := make([]domain.PartitionOffset, 0)
	for topic, partitions := range fetched {
		byPartition := make(map[int]int64)
		for _, o := range latest[topic] {
			byPartition[o.Partition] = o.LastOffset
		}
		for _, p := range partitions {
			var lag int64
			if p.CommittedOffset >= 0 {
				lag = byPartition[p.Partition] - p.CommittedOffset
				if lag < 0 {
					lag = 0
				}
			}
			out = append(out, domain.PartitionOffset{Partition: p.Partition, Offset: p.CommittedOffset, Lag: lag})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out, nil
}

// ClusterInfo returns the broker ids and the current controller.
func (a *KafkaAdapter) ClusterInfo(ctx context.Context) (domain.ClusterInfo, error) {
	meta, err := a.client.Metadata(ctx)
	if err != nil {
		return domain.ClusterInfo{}, err
	}

	brokers := make([]int, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		brokers = append(brokers, b.ID)
	}
	sort.Ints(brokers)
	return domain.ClusterInfo{Brokers: brokers, Controller: meta.Controller.ID}, nil
}

// --- helpers ---

func toKafkaHeaders(in map[string]string) []kafka.Header {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]kafka.Header, 0, len(keys))
	for _, k := range keys {
		out = append(out, kafka.Header{Key: k, Value: []byte(in[k])})
	}
	return out
}

func fromKafkaHeaders(in []kafka.Header) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for _, h := range in {
		out[h.Key] = string(h.Value)
	}
	return out
}
