// Package kafka manages Kafka connections for both single-broker and
// clustered deployments. It exposes metadata, produce and bounded consume
// operations used by the adapter layer.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/TUSIDENG/no-sql-mcp/internal/config"
)

// Client wraps a kafka-go client together with the dialer shared by readers,
// the leader connections used for producing and a round-robin counter used to
// pick a partition when the caller does not pin one.
type Client struct {
	cfg    config.SourceConfig
	addr   string
	client *kafka.Client
	dialer *kafka.Dialer

	rr uint64
}

// New creates a Kafka client from configuration. Both single-broker and
// cluster deployments use the same client: the configured broker list is used
// as bootstrap addresses and the cluster supplies the rest of the metadata.
func New(cfg config.SourceConfig) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers must not be empty")
	}
	dialer, err := buildDialer(cfg)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := buildTLS(cfg)
	if err != nil {
		return nil, err
	}
	saslMech := dialer.SASLMechanism
	return &Client{
		cfg:    cfg,
		addr:   cfg.Brokers[0],
		client: &kafka.Client{Addr: kafka.TCP(cfg.Brokers...), Timeout: time.Duration(cfg.OperationTimeout) * time.Second, Transport: buildTransport(tlsCfg, saslMech)},
		dialer: dialer,
	}, nil
}

// buildTransport builds the transport for the high-level client. It dials a
// raw TCP connection and lets the Transport perform TLS wrapping and SASL
// authentication itself. Returning an already-handshaken *kafka.Conn here would
// corrupt the API version negotiation, so the Dial function must return a plain
// net.Conn.
func buildTransport(tlsCfg *tls.Config, saslMech sasl.Mechanism) kafka.RoundTripper {
	return &kafka.Transport{
		Dial: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLS:  tlsCfg,
		SASL: saslMech,
	}
}

// Ping verifies connectivity by fetching cluster metadata.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.client.Metadata(ctx, &kafka.MetadataRequest{Addr: c.client.Addr})
	return err
}

// Metadata returns the full cluster metadata.
func (c *Client) Metadata(ctx context.Context) (*kafka.MetadataResponse, error) {
	return c.client.Metadata(ctx, &kafka.MetadataRequest{Addr: c.client.Addr})
}

// Topics returns the metadata for the named topics, or every topic when the
// list is empty.
func (c *Client) Topics(ctx context.Context, topics []string) ([]kafka.Topic, error) {
	res, err := c.client.Metadata(ctx, &kafka.MetadataRequest{Addr: c.client.Addr, Topics: topics})
	if err != nil {
		return nil, err
	}
	return res.Topics, nil
}

// TopicDetail fetches partition metadata together with the first and last
// offset of every partition in one ListOffsets request.
func (c *Client) TopicDetail(ctx context.Context, topic string) ([]kafka.Partition, []kafka.PartitionOffsets, error) {
	meta, err := c.client.Metadata(ctx, &kafka.MetadataRequest{Addr: c.client.Addr, Topics: []string{topic}})
	if err != nil {
		return nil, nil, err
	}
	if len(meta.Topics) != 1 {
		return nil, nil, fmt.Errorf("topic %q not found", topic)
	}
	partitions := meta.Topics[0].Partitions

	requests := make(map[string][]kafka.OffsetRequest, len(partitions))
	reqs := make([]kafka.OffsetRequest, 0, len(partitions)*2)
	for _, p := range partitions {
		reqs = append(reqs, kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
	}
	requests[topic] = reqs

	offsets, err := c.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: c.client.Addr, Topics: requests})
	if err != nil {
		return partitions, nil, err
	}
	return partitions, offsets.Topics[topic], nil
}

// LatestOffsets returns the last offset of every requested topic partition.
// requests maps a topic name to its partitions.
func (c *Client) LatestOffsets(ctx context.Context, requests map[string][]int) (map[string][]kafka.PartitionOffsets, error) {
	listReq := make(map[string][]kafka.OffsetRequest, len(requests))
	for topic, partitions := range requests {
		reqs := make([]kafka.OffsetRequest, 0, len(partitions))
		for _, p := range partitions {
			reqs = append(reqs, kafka.LastOffsetOf(p))
		}
		listReq[topic] = reqs
	}
	res, err := c.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: c.client.Addr, Topics: listReq})
	if err != nil {
		return nil, err
	}
	return res.Topics, nil
}

// ListGroups returns every consumer group in the cluster.
func (c *Client) ListGroups(ctx context.Context) (*kafka.ListGroupsResponse, error) {
	return c.client.ListGroups(ctx, &kafka.ListGroupsRequest{Addr: c.client.Addr})
}

// GroupOffsets returns the committed offsets for every topic partition of the
// named group. A nil Topics map requests all topics for the group.
func (c *Client) GroupOffsets(ctx context.Context, group string) (map[string][]kafka.OffsetFetchPartition, error) {
	res, err := c.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{Addr: c.client.Addr, GroupID: group})
	if err != nil {
		return nil, err
	}
	return res.Topics, nil
}

// Produce sends one message. When partition is negative a partition is chosen
// by round-robin; otherwise the request is routed to the leader of that
// partition. The leader connection reports the assigned partition and offset.
func (c *Client) Produce(ctx context.Context, in kafka.Message) (kafka.Message, error) {
	partition := in.Partition
	if partition < 0 {
		meta, err := c.client.Metadata(ctx, &kafka.MetadataRequest{Addr: c.client.Addr, Topics: []string{in.Topic}})
		if err != nil {
			return kafka.Message{}, err
		}
		if len(meta.Topics) != 1 || len(meta.Topics[0].Partitions) == 0 {
			return kafka.Message{}, fmt.Errorf("topic %q not found or has no partitions", in.Topic)
		}
		count := len(meta.Topics[0].Partitions)
		partition = int(atomic.AddUint64(&c.rr, 1)-1) % count
	}

	conn, err := c.dialer.DialLeader(ctx, "tcp", c.addr, in.Topic, partition)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("dial leader for topic %q partition %d: %w", in.Topic, partition, err)
	}
	defer func() { _ = conn.Close() }()

	// A leader connection addresses exactly one topic partition, so the
	// message must not carry Topic or Partition itself.
	out := in
	out.Topic = ""
	out.Partition = 0
	if _, err := conn.WriteMessages(out); err != nil {
		return kafka.Message{}, err
	}
	in.Partition = partition
	return in, nil
}

// NewReader builds a reader. When a group is supplied the reader participates
// in consumer group balancing; otherwise it reads a single partition starting
// at the given offset. Committing is disabled unless commit is true.
func (c *Client) NewReader(topic, group string, partition int, offset int64, commit bool) *kafka.Reader {
	cfg := kafka.ReaderConfig{
		Brokers:  c.cfg.Brokers,
		Topic:    topic,
		GroupID:  group,
		Dialer:   c.dialer,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  time.Second,
	}
	if group == "" {
		cfg.Partition = partition
		cfg.StartOffset = offset
	} else {
		// Groups accept only FirstOffset (-2) or LastOffset (-1); any other
		// value (including an absolute offset) falls back to FirstOffset.
		if offset != kafka.FirstOffset && offset != kafka.LastOffset {
			offset = kafka.FirstOffset
		}
		cfg.StartOffset = offset
		if !commit {
			// A zero CommitInterval selects the synchronous commit mode:
			// offsets are then committed only on an explicit CommitMessages
			// call, so a non-committing consume never advances the group.
			cfg.CommitInterval = 0
		}
	}
	return kafka.NewReader(cfg)
}

// Close has no dedicated connection to release: readers and leader
// connections are closed by their callers and the client keeps no pool.
func (c *Client) Close() error { return nil }

// buildDialer constructs the dialer shared by readers and producers.
func buildDialer(cfg config.SourceConfig) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{Timeout: time.Duration(cfg.OperationTimeout) * time.Second}

	if tlsCfg, err := buildTLS(cfg); err != nil {
		return nil, err
	} else if tlsCfg != nil {
		dialer.TLS = tlsCfg
	}

	if cfg.Username != "" {
		mechanism, err := buildSASL(cfg)
		if err != nil {
			return nil, err
		}
		dialer.SASLMechanism = mechanism
	}
	return dialer, nil
}

// buildTLS constructs the TLS configuration from source settings.
func buildTLS(cfg config.SourceConfig) (*tls.Config, error) {
	if !cfg.TLSEnabled && cfg.CACert == "" && !cfg.SkipTLSVerify {
		return nil, nil
	}
	tlsCfg := &tls.Config{InsecureSkipVerify: cfg.SkipTLSVerify} //nolint:gosec // explicit operator opt-in
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read ca certificate %q: %w", cfg.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca certificate %q contains no valid PEM block", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	}
	return tlsCfg, nil
}

// buildSASL constructs the SASL mechanism matching the configured algorithm.
func buildSASL(cfg config.SourceConfig) (sasl.Mechanism, error) {
	switch strings.ToUpper(cfg.SASLMechanism) {
	case "", "PLAIN":
		return plain.Mechanism{Username: cfg.Username, Password: cfg.Password}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
	default:
		return nil, fmt.Errorf("unsupported sasl mechanism %q", cfg.SASLMechanism)
	}
}
