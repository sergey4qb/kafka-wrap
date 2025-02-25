package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"strings"
)

type Config struct {
	brokers             []string
	groupID             string
	enableSyncProducer  bool
	enableAsyncProducer bool
	enableConsumer      bool
}

func NewConfig(
	groupID string,
	enableSyncProducer, enableAsyncProducer, enableConsumer bool,
	brokers ...string,
) *Config {
	return &Config{
		groupID:             groupID,
		brokers:             brokers,
		enableSyncProducer:  enableSyncProducer,
		enableAsyncProducer: enableAsyncProducer,
		enableConsumer:      enableConsumer,
	}
}

type Client struct {
	brokers       []string
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	consumerGroup sarama.ConsumerGroup
	admin         sarama.ClusterAdmin
}

func NewClient(cfg *Config) (*Client, error) {
	client := &Client{brokers: cfg.brokers}

	defer func() {
		if err := recover(); err != nil {
			if e := client.Close(); e != nil {
				log.Errorf("Error during client cleanup: %v", e)
			}
			panic(err)
		}
	}()

	if cfg.enableSyncProducer {
		p, err := client.initSyncProducer()
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to initialize sync producer: %v", err)
		}
		client.syncProducer = p
	}

	if cfg.enableAsyncProducer {
		p, err := client.initAsyncProducer()
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to initialize async producer: %v", err)
		}
		client.asyncProducer = p
	}

	if cfg.enableConsumer {
		cg, err := client.initConsumerGroup(cfg.groupID)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to initialize consumer group: %v", err)
		}
		client.consumerGroup = cg
	}

	a, err := sarama.NewClusterAdmin(cfg.brokers, sarama.NewConfig())
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to initialize Kafka admin client: %v", err)
	}
	client.admin = a

	return client, nil
}

func (c *Client) initSyncProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	p, err := sarama.NewSyncProducer(c.brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka sync producer: %v", err)
	}
	return p, nil
}

func (c *Client) initAsyncProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	p, err := sarama.NewAsyncProducer(c.brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka async producer: %v", err)
	}
	return p, nil
}

func (c *Client) initConsumerGroup(groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup(c.brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer group: %v", err)
	}
	return consumerGroup, nil
}

// TODO: Creation will be manual
// func (c *Client) CreateTopic(topic string, partitions int32, replicationFactor int16) error {
// 	topics, err := c.admin.ListTopics()
// 	if err != nil {
// 		return fmt.Errorf("failed to list Kafka topics: %v", err)
// 	}
//
// 	if _, exists := topics[topic]; exists {
// 		log.Printf("Topic %s already exists", topic)
// 		return nil
// 	}
//
// 	err = c.admin.CreateTopic(topic, &sarama.TopicDetail{
// 		NumPartitions:     partitions,
// 		ReplicationFactor: replicationFactor,
// 	}, false)
// 	if err != nil {
// 		return fmt.Errorf("failed to create topic %s: %v", topic, err)
// 	}
//
// 	log.Printf("Topic %s created with %d partitions and replication factor %d", topic, partitions, replicationFactor)
// 	return nil
// }

func (c *Client) verifyTopicsExist(topics []string) error {
	existingTopics, err := c.admin.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list Kafka topics: %v", err)
	}
	var missingTopics []string
	for _, topic := range topics {
		if _, exists := existingTopics[topic]; !exists {
			missingTopics = append(missingTopics, topic)
		}
	}
	if len(missingTopics) > 0 {
		return fmt.Errorf("the following topics do not exist: %s", strings.Join(missingTopics, ", "))
	}

	return nil
}

func (c *Client) Close() error {
	var errs []string

	if c.syncProducer != nil {
		if err := c.syncProducer.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close sync producer: %v", err))
		}
	}

	if c.asyncProducer != nil {
		if err := c.asyncProducer.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close async producer: %v", err))
		}
	}

	if c.consumerGroup != nil {
		if err := c.consumerGroup.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close Kafka consumer group: %v", err))
		}
	}

	if c.admin != nil {
		if err := c.admin.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close Kafka admin client: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during client close: %s", strings.Join(errs, "; "))
	}
	return nil
}
