package kafka_test

import (
	"context"
	"fmt"
	"github.com/sergey4qb/kafka-wrap"

	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const topic = "test"

// TestClientProducerConsumerIntegration : Create manually topic before run test
func TestClientProducerConsumerIntegration(t *testing.T) {
	// log.SetLevel(log.DebugLevel) // for debug messages
	config := kafka.NewConfig(
		"gid",
		true,
		true,
		false,
		"localhost:9092",
		"localhost:9093",
	)

	client, err := kafka.NewClient(config)
	require.NoError(t, err, "Client initialization failed")
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topics := []string{topic}

	consumer, err := client.NewConsumer(topics)
	require.NoError(t, err, "Consumer initialization failed")

	producer, err := client.NewSyncProducer()
	require.NoError(t, err, "Producer creation failed")

	outputChannels, err := consumer.StartConsuming(ctx)
	require.NoError(t, err, "Failed to start consuming")

	var sendWg sync.WaitGroup
	var receiveWg sync.WaitGroup

	expectedMessageCount := 53
	sendWg.Add(expectedMessageCount)
	receiveWg.Add(expectedMessageCount)

	timeout := time.After(5 * time.Second)

	numWorkers := 5
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for {
				select {
				case msg, ok := <-outputChannels[topic]:
					if !ok {
						return
					}
					t.Logf("Worker %d: Message from topic %s: %s", workerID, topic, string(msg.Value))
					receiveWg.Done()
				case <-timeout:
					t.Errorf("Timeout reached while waiting for messages in topic: %s", topic)
					return
				}
			}
		}(i)
	}

	go func() {
		for i := 0; i < expectedMessageCount; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("Message %d", i)
			partition, offset, err := producer.SendStringMessage(topic, key, value)
			require.NoError(t, err, "Failed to send message")
			t.Logf("Message sent to partition %d with offset %d", partition, offset)
			sendWg.Done()
		}
	}()

	sendWg.Wait()
	t.Log("All messages sent")

	done := make(chan struct{})
	go func() {
		receiveWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("All messages processed successfully.")
	case <-time.After(10 * time.Second):
		t.Error("Test timed out waiting for message processing.")
	}

	cancel()
	client.Close()
}
