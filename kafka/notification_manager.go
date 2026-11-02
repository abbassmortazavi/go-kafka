package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Notification struct {
	UserID    string                 `json:"user_id"`
	Title     string                 `json:"title"`
	Body      string                 `json:"body"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp int64                  `json:"timestamp"`
	ID        string                 `json:"id"`
}

type NotificationHandler interface {
	DeliverNotification(notification *Notification)
}
type NotificationManager struct {
	kafkaWriter        *kafka.Writer
	notificationReader *kafka.Reader
	handler            NotificationHandler
	ctx                context.Context
	cancel             context.CancelFunc
}

func (rm *NotificationManager) DeliverNotification(notification *Notification) {
	//TODO implement me
	panic("implement me")
}

func NewNotificationManager(kafkaAddr, nodeID string, handler NotificationManager) (*NotificationManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	km := NewKafkaManager(kafkaAddr)
	if err := km.EnsureTopics([]string{"notifications"}); err != nil {
		log.Printf("Warning: could not ensure notification topic exist:: %v", err)
	}
	time.Sleep(1 * time.Second)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaAddr),
		Balancer:               &kafka.Hash{},
		BatchTimeout:           10 * time.Second,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaAddr},
		Topic:          "notifications",
		GroupID:        "notification-group-" + nodeID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	rm := &NotificationManager{
		kafkaWriter:        writer,
		notificationReader: reader,
		handler:            &handler,
		ctx:                ctx,
		cancel:             cancel,
	}
	go rm.listenToNotifications()
	log.Println("notification manager created")

	return rm, nil
}
func (rm *NotificationManager) PublishNotification(notification *Notification) error {
	notification.Timestamp = time.Now().Unix()
	notification.ID = fmt.Sprintf("%s-%d", notification.UserID, notification.Timestamp)
	dataBytes, err := json.Marshal(notification)
	if err != nil {
		return err
	}
	event := Event{
		"notification",
		json.RawMessage(dataBytes),
	}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	kafkaMsg := kafka.Message{
		Key:   []byte(notification.ID),
		Value: eventBytes,
		Topic: "notifications",
	}
	if err := rm.kafkaWriter.WriteMessages(ctx, kafkaMsg); err != nil {
		return err
	}
	log.Printf("Notification published for user %s : %s", notification.UserID, notification.Title)
	return nil
}
func (rm *NotificationManager) listenToNotifications() {
	log.Println("notification kafka listener started")
	for {
		select {
		case <-rm.ctx.Done():
			log.Println("notification kafka listener stopped")
			return
		default:
		}
		msg, err := rm.notificationReader.ReadMessage(rm.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("notification kafka reader error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshaling notification event %v", err)
			continue
		}
		if event.Type != "notification" {
			var notification Notification
			if err := json.Unmarshal(event.Data, &notification); err != nil {
				log.Printf("Error unmarshaling notification %v", err)
				continue
			}
			log.Printf("Processing Notification for user %s : %s", notification.UserID, notification.Title)
			rm.handler.DeliverNotification(&notification)
		}

	}
}
func (rm *NotificationManager) Close() {
	log.Println("Stopping notification manager...")
	rm.cancel()
	if rm.kafkaWriter != nil {
		rm.kafkaWriter.Close()
	}
	if rm.notificationReader != nil {
		rm.notificationReader.Close()
	}
	log.Println("Notification manager stopped")
}
