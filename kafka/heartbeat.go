package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type NodeHartBeat struct {
	NodeID    string   `json:"node_id"`
	Timestamp int64    `json:"timestamp"`
	UserIDs   []string `json:"user_ids"`
}

type ChatHub interface {
	GetConnectedUserIDs() []string
	PublishUserStatus(userID, nodeID string, online bool) error
	GetClientLock() *sync.Mutex
	GetClients() map[string]interface{}
}

type HeartBeatManager struct {
	mutex              sync.Mutex
	nodeID             string
	hub                ChatHub
	kafkaWriter        *kafka.Writer
	heartBeatReader    *kafka.Reader
	nodeLastSeen       map[string]int64
	processedDeadNodes map[string]bool
	nodeUsers          map[string][]string
	heartBeatInterval  time.Duration
	timeoutThreshold   time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
}

func NewHeartBeatManager(kafkaAddr, nodeID string, hub ChatHub) (*HeartBeatManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	km := NewKafkaManager(kafkaAddr)
	if err := km.EnsureTopics([]string{"node-heartbeat"}); err != nil {
		log.Printf("kafka ensure topics failed: %v", err)
	}
	time.Sleep(time.Second * 1)
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaAddr),
		Topic:                  "node-heartbeat",
		Balancer:               &kafka.Hash{},
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaAddr},
		Topic:          "node-heartbeat",
		GroupID:        "heartbeat-monitor-" + nodeID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	hm := &HeartBeatManager{
		nodeID:             nodeID,
		hub:                hub,
		kafkaWriter:        writer,
		heartBeatReader:    reader,
		nodeLastSeen:       make(map[string]int64),
		nodeUsers:          make(map[string][]string),
		processedDeadNodes: make(map[string]bool),
		heartBeatInterval:  time.Second * 5,
		timeoutThreshold:   time.Second * 15,
		ctx:                ctx,
		cancel:             cancel,
	}
	//
	log.Printf("[%s] rebuilding node state from heartbeats...", nodeID)
	if err := hm.rebuildNodeStateFromAllPartitions(kafkaAddr); err != nil {
		log.Printf("[%s] node state rebuilding has issues: %v", nodeID, err)
		return nil, err
	}
	//sendHeatBeats
	//listenToHeartBeats
	//monitorNodeHealth
	//cleanUpOldDeadNodes
	log.Printf("HeartBeat Manager initialized for node: %s", nodeID)
	return hm, nil
}
func (hbm *HeartBeatManager) rebuildNodeStateFromAllPartitions(kafkaAddr string) error {
	log.Printf("[%s]reading all heartbeat messages from all partitions...", hbm.nodeID)
	totalProcessed, err := ReadAllPartitions(kafkaAddr, "node-heartbeat", func(msg kafka.Message) error {
		var event Event
		err := json.Unmarshal(msg.Value, &event)
		if err != nil {
			return err
		}
		if event.Type == "node_heatbeat" {
			var heartbeat NodeHartBeat
			err = json.Unmarshal(event.Data, &heartbeat)
			if err != nil {
				return err
			}
			hbm.mutex.Lock()
			hbm.nodeLastSeen[heartbeat.NodeID] = heartbeat.Timestamp
			hbm.nodeUsers[heartbeat.NodeID] = heartbeat.UserIDs
			hbm.mutex.Unlock()
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("[%s]node state rebuilding completed : %d total heartbeats", hbm.nodeID, totalProcessed)
	hbm.mutex.Lock()
	for nodeID, lastSeen := range hbm.nodeLastSeen {
		users := hbm.nodeUsers[nodeID]
		log.Printf("[%s] known node : %s (last need: %d, users: %d)", hbm.nodeID, nodeID, lastSeen, len(users))
	}
	hbm.mutex.Unlock()
	return nil
}
