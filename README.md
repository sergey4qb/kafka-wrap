# Kafka Client

A Go library for interacting with Apache Kafka. This library is a wrapper over the [sarama](https://github.com/IBM/sarama) library and provides interfaces for sending and receiving messages, as well as basic topic administration.

## Installation

Install the library using Go’s package manager:

    go get github.com/sergey4qb/kafka-wrap@v0.0.1-dev

## Overview

The Kafka Client library offers the following key functionalities:

- **Client Initialization:** Create a Kafka client that can function as a producer, synchronous/asynchronous consumer, and administrator.
- **Message Production:** Send messages using either synchronous or asynchronous producers, with support for both plain text and JSON formats.
- **Message Consumption:** Receive messages from Kafka topics via a consumer that can subscribe to multiple topics.

> **Note:** Ensure that topics are created manually on the broker before starting.

## Usage

### 1. Client Initialization

Set up your Kafka client by defining a configuration using the `Config` structure. This configuration includes:

- **Kafka Brokers:** A list of brokers to connect to.
- **Operation Modes:** Enable or disable the producer, consumer, and administrator functions.
- **Consumer Group ID:** The ID for the consumer group when message consumption is required.

Example:

    config := kafka.NewConfig(
        "microservice-name", // Consumer group ID
        true,                // Enable synchronous producer
        true,                // Enable asynchronous producer
        true,                // Enable consumer
        "localhost:9092",    // Broker 1
        "localhost:9093",    // Broker 2
    )
    client, err := kafka.NewClient(config)
    if err != nil {
        log.Fatalf("Error initializing Kafka client: %v", err)
    }
    defer client.Close()

### 2. Working with the Synchronous Producer

Create a synchronous producer using the `NewSyncProducer` method. Use the `SendStringMessage` or `SendJSONMessage` methods to send messages.

Example:

    producer, err := client.NewSyncProducer()
    if err != nil {
        log.Fatalf("Failed to create producer: %v", err)
    }

    go func() {
        for i := 0; i < 5; i++ {
            partition, offset, err := producer.SendStringMessage(
                "topic-name",
                fmt.Sprintf("key-%d", i),
                fmt.Sprintf("Message %d", i),
            )
            if err != nil {
                log.Printf("Error sending message: %v", err)
            } else {
                log.Printf("Message sent to partition %d with offset %d", partition, offset)
            }
            time.Sleep(1 * time.Second)
        }
    }()

### 3. Working with the Asynchronous Producer

Create an asynchronous producer using the `NewAsyncProducer` method. Use similar message sending methods as with the synchronous producer.

Example:

    asyncProducer, err := client.NewAsyncProducer()
    if err != nil {
        log.Fatalf("Failed to create async producer: %v", err)
    }

    go func() {
        for i := 0; i < 5; i++ {
            err := asyncProducer.SendStringMessage(
                "topic-name",
                fmt.Sprintf("key-%d", i),
                fmt.Sprintf("Message %d", i),
            )
            if err != nil {
                log.Printf("Error sending async message: %v", err)
            } else {
                log.Printf("Async message sent to topic 'topic-name' with key key-%d", i)
            }
            time.Sleep(1 * time.Second)
        }
    }()

### 4. Working with the Consumer

Create a consumer by calling the `NewConsumer` method with a list of topics, and start consuming messages with the `StartConsuming` method.

Example:

    consumer, err := client.NewConsumer([]string{"topic-name"})
    if err != nil {
        log.Fatalf("Failed to initialize Kafka consumer: %v", err)
    }

    outputChannels, err := consumer.StartConsuming(ctx)
    if err != nil {
        log.Fatalf("Failed to start consuming: %v", err)
    }

    for topic, ch := range outputChannels {
        go func(t string, c <-chan kafka.Message) {
            log.Printf("Started consuming messages from topic: %s", t)
            for msg := range c {
                log.Printf("Message from topic %s: %s", t, string(msg.Value))
            }
            log.Printf("Stopped consuming messages from topic: %s", t)
        }(topic, ch)
    }

## License

Distributed under the MIT License. See `LICENSE` for more information.
