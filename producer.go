package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
)

type SyncProducer interface {
	SendStringMessage(topic, key, message string) (int32, int64, error)
	SendJSONMessage(topic, key string, data interface{}) (int32, int64, error)
	Close() error
}

type AsyncProducer interface {
	SendStringMessage(topic, key, message string) error
	SendJSONMessage(topic, key string, data interface{}) error
	Close() error
}

type syncProducer struct {
	client *Client
}

type asyncProducer struct {
	client *Client
}

func (c *Client) NewSyncProducer() (SyncProducer, error) {
	if c.syncProducer == nil {
		return nil, fmt.Errorf("sync producer was not initialized; enableSyncProducer must be true")
	}
	return &syncProducer{
		client: c,
	}, nil
}

func (c *Client) NewAsyncProducer() (AsyncProducer, error) {
	if c.asyncProducer == nil {
		return nil, fmt.Errorf("async producer was not initialized; enableAsyncProducer must be true")
	}
	return &asyncProducer{
		client: c,
	}, nil
}

func (p *syncProducer) SendStringMessage(topic, key, message string) (int32, int64, error) {
	msg := createProducerMessage(topic, key, []byte(message))

	partition, offset, err := p.client.syncProducer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to send string message: %w", err)
	}
	return partition, offset, nil
}

func (p *syncProducer) SendJSONMessage(topic, key string, data interface{}) (int32, int64, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal JSON message: %w", err)
	}

	msg := createProducerMessage(topic, key, jsonData)

	partition, offset, err := p.client.syncProducer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to send JSON message: %w", err)
	}
	return partition, offset, nil
}

func (p *syncProducer) Close() error {
	if err := p.client.syncProducer.Close(); err != nil {
		return fmt.Errorf("failed to close sync producer: %w", err)
	}

	return nil
}

func (p *asyncProducer) SendStringMessage(topic, key, message string) error {
	msg := createProducerMessage(topic, key, []byte(message))

	p.client.asyncProducer.Input() <- msg

	return nil
}

func (p *asyncProducer) SendJSONMessage(topic, key string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON message: %w", err)
	}
	msg := createProducerMessage(topic, key, jsonData)

	p.client.asyncProducer.Input() <- msg

	return nil
}

func (p *asyncProducer) Close() error {
	if err := p.client.asyncProducer.Close(); err != nil {
		return fmt.Errorf("failed to close async producer: %w", err)
	}

	return nil
}

func createProducerMessage(topic, key string, value []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}
