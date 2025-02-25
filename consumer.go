package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"

	log "github.com/sirupsen/logrus"
	"sync"

	"time"
)

type Consumer interface {
	StartConsuming(ctx context.Context) (map[string]chan Message, error)
	Close() error
}

var (
	ConsumerChannelBuffer = 100
)

const (
	retryInterval = 5 * time.Second
	maxRetries    = 10
)

type consumer struct {
	client      *Client
	cancel      context.CancelFunc
	handlerDone chan struct{}
	topics      []string
}

func (c *Client) NewConsumer(topics []string) (Consumer, error) {
	if c.consumerGroup == nil {
		return nil, fmt.Errorf("consumer group was not initialized; enableConsumer must be true")
	}
	return &consumer{
		client:      c,
		handlerDone: make(chan struct{}),
		topics:      topics,
	}, nil
}

// TODO: Add offset handler
func (c *consumer) StartConsuming(ctx context.Context) (map[string]chan Message, error) {
	err := c.client.verifyTopicsExist(c.topics)
	if err != nil {
		return nil, err
	}

	outputChannels := make(map[string]chan Message)
	for _, topic := range c.topics {
		outputChannels[topic] = make(chan Message, ConsumerChannelBuffer)
	}
	handler := newConsumerGroupHandler(outputChannels)

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go func() {
		defer close(c.handlerDone)
		retries := 0
		for {
			if retries >= maxRetries {
				log.Errorf("Max retries reached. Stopping consumer.")
				return
			}

			if err = c.client.consumerGroup.Consume(ctx, c.topics, &handler); err != nil {
				handleConsumeError(err, &retries)
				continue
			}

			retries = 0

			select {
			case <-ctx.Done():
				log.Info("Context canceled. Stopping consumer.")
				return
			default:
			}
		}
	}()

	go func() {
		for {
			select {
			case err, ok := <-c.client.consumerGroup.Errors():
				if !ok {
					log.Warn("Consumer group error channel closed")
					return
				}
				log.Errorf("Consumer group error: %v", err) // NOTE: Temporary without error handling
			case <-ctx.Done():
				log.Info("Context canceled, stopping error handler goroutine")
				return
			}
		}
	}()

	return handler.outputChannels, nil
}

func handleConsumeError(err error, retries *int) {
	log.Warnf("Error consuming messages: %v", err)
	*retries++
	log.Warnf("Retrying in %s... (attempt %d/%d)", retryInterval, *retries, maxRetries)
	time.Sleep(retryInterval)
}

func (c *consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.handlerDone
	return c.client.consumerGroup.Close()
}

type consumerGroupHandler struct {
	outputChannels map[string]chan Message
	loggedTopics   map[string]bool
	mu             sync.RWMutex
}

func newConsumerGroupHandler(outputChannels map[string]chan Message) consumerGroupHandler {
	return consumerGroupHandler{
		outputChannels: outputChannels,
		loggedTopics:   make(map[string]bool),
	}
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// NOTE: Will be ran for each topic partiotion

func (h *consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	h.mu.Lock()
	if !h.loggedTopics[claim.Topic()] {
		log.Infof(
			"Connected to Kafka successfully. Consuming messages from topic: %s...",
			claim.Topic(),
		)
		h.loggedTopics[claim.Topic()] = true
	}
	h.mu.Unlock()

	for message := range claim.Messages() {
		log.Debugf("Received message from topic: %s | Partition: %d | Offset: %d | Key: %s | Value: %s",
			message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))

		h.mu.RLock()
		topicChan := h.outputChannels[message.Topic]
		h.mu.RUnlock()

		select {
		case topicChan <- Message{
			Headers:   message.Headers,
			Partition: message.Partition,
			Offset:    message.Offset,
			Key:       message.Key,
			Value:     message.Value,
		}:
			log.Debugf("Message sent to channel for topic: %s", message.Topic)
		default:
			log.Warnf("Channel for topic %s is full, message dropped", message.Topic)
		}

		session.MarkMessage(message, "")
	}
	return nil
}
