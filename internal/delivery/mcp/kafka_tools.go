package mcp

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/cortex/pkg/types"

	"github.com/TUSIDENG/no-sql-mcp/internal/domain"
)

// kafkaToolCount is the number of tools registered per Kafka source.
const kafkaToolCount = 6

// registerKafkaTools registers the Kafka tools for every configured Kafka
// source. It mirrors registerRedisTools.
func (r *ToolRegistry) registerKafkaTools(ctx context.Context, mcpServer *server.MCPServer) error {
	sources, err := r.sourceUC.ListSources()
	if err != nil {
		return fmt.Errorf("list sources for kafka tool registration: %w", err)
	}

	for _, s := range sources {
		if s.Kind != domain.KindKafka {
			continue
		}
		if err := r.registerKafkaToolsForSource(ctx, mcpServer, s.ID, s.ReadOnly); err != nil {
			return err
		}
	}
	return nil
}

// registerKafkaToolsForSource registers the 6 Kafka tools for one source.
func (r *ToolRegistry) registerKafkaToolsForSource(ctx context.Context, mcpServer *server.MCPServer, sourceID string, readOnly bool) error {
	note := readOnlyNote(readOnly)

	topics := tools.NewTool("kafka_topics_"+sourceID,
		tools.WithDescription("List Kafka topics with partition and replica counts. "+note))

	topicDetail := tools.NewTool("kafka_topic_detail_"+sourceID,
		tools.WithDescription("Get the partitions, leaders, ISRs and first/last offsets of one Kafka topic. "+note),
		tools.WithString("topic", tools.Description("Topic name"), tools.Required()))

	groups := tools.NewTool("kafka_groups_"+sourceID,
		tools.WithDescription("List Kafka consumer groups with total lag. "+note))

	consume := tools.NewTool("kafka_consume_"+sourceID,
		tools.WithDescription("Consume a bounded number of Kafka messages by topic/partition/offset or consumer group. Offsets are not committed unless commit=true. "+note),
		tools.WithString("topic", tools.Description("Topic name; defaults to the source default topic")),
		tools.WithString("group", tools.Description("Consumer group id; when set the reader uses group balancing")),
		tools.WithNumber("partition", tools.Description("Partition to read without a group, default 0")),
		tools.WithNumber("offset", tools.Description("Start offset without a group: -2 earliest, -1 latest (default -1), or an absolute offset")),
		tools.WithNumber("max_messages", tools.Description("Maximum number of messages to return, bounded by the source limit")),
		tools.WithNumber("timeout_ms", tools.Description("Maximum time in milliseconds to spend collecting messages")),
		tools.WithBoolean("commit", tools.Description("Commit the group offset; rejected on read-only sources")))

	produce := tools.NewTool("kafka_produce_"+sourceID,
		tools.WithDescription("Send one Kafka message. "+note),
		tools.WithString("topic", tools.Description("Topic name; defaults to the source default topic")),
		tools.WithString("key", tools.Description("Optional message key")),
		tools.WithString("value", tools.Description("Message value"), tools.Required()),
		tools.WithNumber("partition", tools.Description("Partition to send to; -1 (default) selects one automatically")),
		tools.WithObject("headers", tools.Description("Optional message headers as a JSON object of string values")))

	cluster := tools.NewTool("kafka_cluster_"+sourceID,
		tools.WithDescription("List Kafka brokers and the current controller. "+note))

	specs := []*types.Tool{topics, topicDetail, groups, consume, produce, cluster}
	handlers := []server.ToolHandler{
		r.kafkaTopicsHandler(sourceID),
		r.kafkaTopicDetailHandler(sourceID),
		r.kafkaGroupsHandler(sourceID),
		r.kafkaConsumeHandler(sourceID),
		r.kafkaProduceHandler(sourceID),
		r.kafkaClusterHandler(sourceID),
	}

	for i := range specs {
		if err := mcpServer.AddTool(ctx, specs[i], handlers[i]); err != nil {
			return fmt.Errorf("register %s: %w", specs[i].Name, err)
		}
	}
	return nil
}

// --- Handlers ---

func (r *ToolRegistry) kafkaTopicsHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		topics, err := r.kafkaUC.ListTopics(ctx, sourceID)
		if err != nil {
			return ErrorContent(sourceID, "kafka_topics", err.Error()), nil
		}
		return TextContent(formatKafkaTopics(topics)), nil
	}
}

func (r *ToolRegistry) kafkaTopicDetailHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		topic, ok := req.Parameters["topic"].(string)
		if !ok || strings.TrimSpace(topic) == "" {
			return ErrorContent(sourceID, "kafka_topic_detail", "parameter 'topic' is required"), nil
		}

		detail, err := r.kafkaUC.TopicDetail(ctx, sourceID, topic)
		if err != nil {
			return ErrorContent(sourceID, "kafka_topic_detail", err.Error()), nil
		}
		return TextContent(formatKafkaTopicDetail(detail)), nil
	}
}

func (r *ToolRegistry) kafkaGroupsHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		groups, err := r.kafkaUC.ListConsumerGroups(ctx, sourceID)
		if err != nil {
			return ErrorContent(sourceID, "kafka_groups", err.Error()), nil
		}
		return TextContent(formatKafkaGroups(groups)), nil
	}
}

func (r *ToolRegistry) kafkaConsumeHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		in, err := parseConsumeInput(req.Parameters)
		if err != nil {
			return ErrorContent(sourceID, "kafka_consume", err.Error()), nil
		}

		messages, err := r.kafkaUC.Consume(ctx, sourceID, in)
		if err != nil {
			return ErrorContent(sourceID, "kafka_consume", err.Error()), nil
		}
		return TextContent(formatKafkaMessages(messages)), nil
	}
}

func (r *ToolRegistry) kafkaProduceHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		in, err := parseProduceInput(req.Parameters)
		if err != nil {
			return ErrorContent(sourceID, "kafka_produce", err.Error()), nil
		}

		topic := in.Topic
		result, err := r.kafkaUC.Produce(ctx, sourceID, in)
		if err != nil {
			return ErrorContent(sourceID, "kafka_produce", err.Error()), nil
		}
		if result.Topic != "" {
			topic = result.Topic
		}
		return TextContent(fmt.Sprintf("Message produced to topic %q partition %d.", topic, result.Partition)), nil
	}
}

func (r *ToolRegistry) kafkaClusterHandler(sourceID string) server.ToolHandler {
	return func(ctx context.Context, req server.ToolCallRequest) (any, error) {
		info, err := r.kafkaUC.ClusterInfo(ctx, sourceID)
		if err != nil {
			return ErrorContent(sourceID, "kafka_cluster", err.Error()), nil
		}
		return TextContent(formatKafkaCluster(info)), nil
	}
}

// --- Parameter parsing ---

func parseConsumeInput(params map[string]any) (domain.ConsumeInput, error) {
	var in domain.ConsumeInput

	in.Topic, _ = params["topic"].(string)
	in.Group, _ = params["group"].(string)
	in.Partition = intParam(params, "partition", 0)
	// Partition reads default to the latest offset; group reads default to
	// the earliest offset so a new group does not skip existing history.
	defaultOffset := -1
	if in.Group != "" {
		defaultOffset = -2
	}
	in.Offset = int64(intParam(params, "offset", defaultOffset))
	in.MaxMessages = intParam(params, "max_messages", 0)
	in.TimeoutMs = intParam(params, "timeout_ms", 0)
	in.Commit, _ = params["commit"].(bool)
	return in, nil
}

func parseProduceInput(params map[string]any) (domain.ProduceInput, error) {
	var in domain.ProduceInput

	in.Topic, _ = params["topic"].(string)
	key, _ := params["key"].(string)
	in.Key = []byte(key)
	value, ok := params["value"].(string)
	if !ok {
		return in, fmt.Errorf("parameter 'value' is required")
	}
	in.Value = []byte(value)
	in.Partition = intParam(params, "partition", -1)
	if headers, ok := params["headers"].(map[string]any); ok {
		in.Headers = make(map[string]string, len(headers))
		for k, v := range headers {
			s, ok := v.(string)
			if !ok {
				return in, fmt.Errorf("header %q must be a string", k)
			}
			in.Headers[k] = s
		}
	}
	return in, nil
}

// --- Formatting helpers ---

func formatKafkaTopics(topics []domain.TopicInfo) string {
	if len(topics) == 0 {
		return "No topics found."
	}
	var b strings.Builder
	fmt.Fprintln(&b, "Topic | Partitions | Replicas")
	for _, t := range topics {
		fmt.Fprintf(&b, "%s | %d | %d\n", t.Name, t.Partitions, t.Replicas)
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatKafkaTopicDetail(detail domain.TopicDetail) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Topic: %s\n", detail.Name)
	fmt.Fprintln(&b, "Partition | Leader | ISR | Earliest | Latest")
	for _, p := range detail.Partitions {
		fmt.Fprintf(&b, "%d | %d | %s | %d | %d\n", p.Partition, p.Leader, intsToString(p.ISR), p.Earliest, p.Latest)
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatKafkaGroups(groups []domain.ConsumerGroupInfo) string {
	if len(groups) == 0 {
		return "No consumer groups found."
	}
	var b strings.Builder
	fmt.Fprintln(&b, "Group | Lag")
	for _, g := range groups {
		fmt.Fprintf(&b, "%s | %d\n", g.Name, g.Lag)
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatKafkaMessages(messages []domain.Message) string {
	if len(messages) == 0 {
		return "No messages found."
	}
	var b strings.Builder
	for i, m := range messages {
		fmt.Fprintf(&b, "\n--- message %d ---\n", i+1)
		fmt.Fprintf(&b, "partition: %d\noffset: %d\n", m.Partition, m.Offset)
		if m.Timestamp > 0 {
			fmt.Fprintf(&b, "timestamp: %d\n", m.Timestamp)
		}
		if len(m.Key) > 0 {
			fmt.Fprintf(&b, "key: %s\n", string(m.Key))
		}
		if len(m.Headers) > 0 {
			fmt.Fprintf(&b, "headers: %s\n", formatHeaderMap(m.Headers))
		}
		fmt.Fprintf(&b, "value: %s\n", string(m.Value))
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatKafkaCluster(info domain.ClusterInfo) string {
	return fmt.Sprintf("Brokers: %s\nController: %d", intsToString(info.Brokers), info.Controller)
}

func intsToString(in []int) string {
	parts := make([]string, 0, len(in))
	for _, v := range in {
		parts = append(parts, fmt.Sprintf("%d", v))
	}
	sort.Strings(parts)
	return "[" + strings.Join(parts, ",") + "]"
}

func formatHeaderMap(headers map[string]string) string {
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", k, headers[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
