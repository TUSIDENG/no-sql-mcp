package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// KafkaLimitProvider exposes per-source limits without coupling the usecase to
// configuration or concrete client types. Kafka adapters implement it.
type KafkaLimitProvider interface {
	KafkaMaxMessages() int
	KafkaTimeoutSec() int
}

// KafkaUseCase implements Kafka tool business logic. Every operation resolves
// the source, asserts the Messaging capability and runs the guard.
type KafkaUseCase struct {
	repo  domain.DataSourceRepository
	guard *Guard
}

// NewKafkaUseCase creates a KafkaUseCase.
func NewKafkaUseCase(repo domain.DataSourceRepository, guard *Guard) *KafkaUseCase {
	return &KafkaUseCase{repo: repo, guard: guard}
}

// messaging resolves the source and validates the operation through the guard.
func (uc *KafkaUseCase) messaging(ctx context.Context, sourceID, operation string, commit bool) (domain.Messaging, context.Context, context.CancelFunc, error) {
	src, err := uc.repo.Get(sourceID)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := uc.guard.CheckKafkaOperation(src, operation, commit); err != nil {
		return nil, nil, nil, err
	}

	timeoutSec := 10
	if provider, ok := src.(KafkaLimitProvider); ok {
		timeoutSec = provider.KafkaTimeoutSec()
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)

	broker, ok := src.(domain.Messaging)
	if !ok {
		cancel()
		return nil, nil, nil, fmt.Errorf("source %q does not implement Messaging", sourceID)
	}
	return broker, ctx, cancel, nil
}

// ListTopics lists every topic with its partition and replica counts.
func (uc *KafkaUseCase) ListTopics(ctx context.Context, sourceID string) ([]domain.TopicInfo, error) {
	broker, ctx, cancel, err := uc.messaging(ctx, sourceID, "list_topics", false)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return broker.ListTopics(ctx)
}

// TopicDetail returns the partition detail of one topic.
func (uc *KafkaUseCase) TopicDetail(ctx context.Context, sourceID, topic string) (domain.TopicDetail, error) {
	broker, ctx, cancel, err := uc.messaging(ctx, sourceID, "topic_detail", false)
	if err != nil {
		return domain.TopicDetail{}, err
	}
	defer cancel()
	return broker.TopicDetail(ctx, topic)
}

// ListConsumerGroups lists every group with its total lag.
func (uc *KafkaUseCase) ListConsumerGroups(ctx context.Context, sourceID string) ([]domain.ConsumerGroupInfo, error) {
	broker, ctx, cancel, err := uc.messaging(ctx, sourceID, "list_groups", false)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return broker.ListConsumerGroups(ctx)
}

// GroupOffsets returns the committed offsets and per-partition lag of a group.
func (uc *KafkaUseCase) GroupOffsets(ctx context.Context, sourceID, group string) ([]domain.PartitionOffset, error) {
	broker, ctx, cancel, err := uc.messaging(ctx, sourceID, "group_offsets", false)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return broker.GroupOffsets(ctx, group)
}

// ClusterInfo returns the broker list and controller.
func (uc *KafkaUseCase) ClusterInfo(ctx context.Context, sourceID string) (domain.ClusterInfo, error) {
	broker, ctx, cancel, err := uc.messaging(ctx, sourceID, "cluster_info", false)
	if err != nil {
		return domain.ClusterInfo{}, err
	}
	defer cancel()
	return broker.ClusterInfo(ctx)
}

// Produce sends one message and returns its partition.
func (uc *KafkaUseCase) Produce(ctx context.Context, sourceID string, in domain.ProduceInput) (domain.ProduceResult, error) {
	if err := uc.applyDefaultTopic(&in.Topic, sourceID); err != nil {
		return domain.ProduceResult{}, err
	}
	broker, ctx, cancel, err := uc.messaging(ctx, sourceID, "produce", false)
	if err != nil {
		return domain.ProduceResult{}, err
	}
	defer cancel()
	return broker.Produce(ctx, in)
}

// applyDefaultTopic fills topic with the source default topic when empty.
func (uc *KafkaUseCase) applyDefaultTopic(topic *string, sourceID string) error {
	if *topic != "" {
		return nil
	}
	src, err := uc.repo.Get(sourceID)
	if err != nil {
		return err
	}
	def, ok := src.(interface{ DefaultTopic() string })
	if !ok || def.DefaultTopic() == "" {
		return fmt.Errorf("parameter 'topic' is required (no default topic configured)")
	}
	*topic = def.DefaultTopic()
	return nil
}

// Consume performs a bounded read. The message count is clamped to the
// per-source max_messages limit.
func (uc *KafkaUseCase) Consume(ctx context.Context, sourceID string, in domain.ConsumeInput) ([]domain.Message, error) {
	if err := uc.applyDefaultTopic(&in.Topic, sourceID); err != nil {
		return nil, err
	}

	src, err := uc.repo.Get(sourceID)
	if err != nil {
		return nil, err
	}
	if limiter, ok := src.(KafkaLimitProvider); ok {
		if limit := limiter.KafkaMaxMessages(); limit > 0 && (in.MaxMessages <= 0 || in.MaxMessages > limit) {
			in.MaxMessages = limit
		}
	}

	broker, ctx, cancel, err := uc.messaging(ctx, sourceID, "consume", in.Commit)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return broker.Consume(ctx, in)
}
