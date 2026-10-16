package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type KafkaManager struct {
	addr string
}

func NewKafkaManager(addr string) *KafkaManager {
	return &KafkaManager{addr: addr}
}

func (k *KafkaManager) EnsureTopics(topics []string) error {
	conn, err := kafka.Dial("tcp", k.addr)
	if err != nil {
		return fmt.Errorf("connecting to kafka: %w", err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("retrieving kafka controller: %w", err)
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprint(":", controller.Port)))
	if err != nil {
		return fmt.Errorf("dialing controller: %w", err)
	}
	defer controllerConn.Close()

	var topicConfigs []kafka.TopicConfig
	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		})
	}
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("creating topics: %w", err)
	}
	return nil
}

func (k *KafkaManager) HealthCheck() error {
	conn, err := kafka.Dial("tcp", k.addr)
	if err != nil {
		return fmt.Errorf("connecting to kafka: %w", err)
	}
	defer conn.Close()
	_, err = conn.Brokers()
	if err != nil {
		return fmt.Errorf("getting brokers: %w", err)
	}
	return nil
}

func WaitForKafka(addr string, timeout time.Duration) error {
	km := KafkaManager{addr: addr}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := km.HealthCheck(); err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("kafka timed out waiting for kafka to start %v", timeout)
}
func ReadAllPartitions(kafkaAddr string, topic string, handler func(msg kafka.Message) error) (int, error) {
	log.Printf("ReadAllPartitions for topics: %s", topic)
	conn, err := kafka.Dial("tcp", kafkaAddr)
	if err != nil {
		return 0, fmt.Errorf("connecting to kafka: %w", err)
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, fmt.Errorf("reading partitions: %w", err)
	}
	log.Printf("found %d partitions for topics %s", len(partitions), topic)

	totalProcessed := 0
	for _, partition := range partitions {
		log.Printf("reading partition %d for topic %s", partition.ID, topic)
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{kafkaAddr},
			Topic:     topic,
			Partition: partition.ID,
			MinBytes:  1,
			MaxBytes:  10e6,
			MaxWait:   100 * time.Millisecond,
		})
		reader.SetOffset(0)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		partitionProcessed := 0
		defer cancel()
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					log.Printf("partition %d: timed out reached after processing %d messages", partition.ID, partitionProcessed)
					break
				}
				//no message
				break
			}
			if err := handler(msg); err != nil {
				cancel()
				reader.Close()
				return totalProcessed, err
			}
			partitionProcessed++
			totalProcessed++
		}
		reader.Close()
		cancel()
		log.Printf("partition %d: processed %d messages", partition.ID, partitionProcessed)
	}
	log.Printf("Completed Reading All partitions for %s: %d total messages", topic, totalProcessed)
	return totalProcessed, nil
}
