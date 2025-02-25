package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
)

type Message struct {
	Headers   []*sarama.RecordHeader
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

type InternalMessage struct {
	Code    int    `json:"code,omitempty"`
	Header  string `json:"header,omitempty"`
	Message string `json:"message,omitempty"`
}

func GetValue[T any](msg Message) (T, error) {
	var result T
	if err := json.Unmarshal(msg.Value, &result); err != nil {
		return result, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}
	return result, nil
}

func GetHeader(msg Message, key string) (string, error) {
	for _, header := range msg.Headers {
		if string(header.Key) == key {
			return string(header.Value), nil
		}
	}
	return "", fmt.Errorf("header with key '%s' not found", key)
}
