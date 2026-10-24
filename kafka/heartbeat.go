package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type NodeHartBeat struct {
	NodeID    string   `json:"node_id"`
	Timestamp int64    `json:"timestamp"`
	UserIDs   []string `json:"user_ids"`
}

//type ChatHub interface {
//	GetConnectedUserIDs() []string
//	PublishUserStatus(userID, nodeID string, online bool) error
//	GetClientLock() *sync.Mutex
//	GetClients() map[string]interface{}
//}

type HeartBeatManager struct {
	mutex  sync.RWMutex
	nodeID string
	//hub                ChatHub
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

// func NewHeartBeatManager(kafkaAddr, nodeID string, hub ChatHub) (*HeartBeatManager, error) {
func NewHeartBeatManager(kafkaAddr, nodeID string) (*HeartBeatManager, error) {
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
		nodeID: nodeID,
		//hub:                hub,
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
	go hm.sendheatbeats()
	//listenToHeartBeats
	go hm.listenToHeartBeats()
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
		if event.Type == "node_heartbeat" {
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

func (hbm *HeartBeatManager) sendheatbeats() {
	ticker := time.NewTicker(hbm.heartBeatInterval)
	defer ticker.Stop()
	log.Printf("Starting to send heatbeats for node: %s...", hbm.nodeID)
	for {
		select {
		case <-hbm.ctx.Done():
			log.Printf("HeartBeat sender shutting down")
			return
		case <-ticker.C:
			if err := hbm.publishHeartbeat(); err != nil {
				log.Printf("HeartBeat sender failed: %v", err)
			}

		}

	}
}
func (hbm *HeartBeatManager) publishHeartbeat() error {
	userIDs := []string{"user-1", "user-2", "user-3"}
	//userIDs := hbm.hub.GetConnectedUserIDs()
	heartbeat := NodeHartBeat{
		NodeID:    hbm.nodeID,
		Timestamp: time.Now().Unix(),
		UserIDs:   userIDs,
	}
	data, err := json.Marshal(heartbeat)
	if err != nil {
		return err
	}
	event := Event{
		Type: "node_heartbeat",
		Data: json.RawMessage(data),
	}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := kafka.Message{
		Key:   []byte(hbm.nodeID),
		Value: eventBytes,
	}
	if err := hbm.kafkaWriter.WriteMessages(ctx, msg); err != nil {
		return err
	}
	log.Printf("HeartBeat sent: node=%s, user=%d", hbm.nodeID, len(userIDs))
	return nil

}
func (hbm *HeartBeatManager) Stop() {
	log.Printf("Stopping HeartBeat manager")
	hbm.cancel()
	if hbm.kafkaWriter != nil {
		hbm.kafkaWriter.Close()
	}
	if hbm.heartBeatReader != nil {
		hbm.heartBeatReader.Close()
	}
	log.Printf("HeartBeat Manager stopped")
}

func (hbm *HeartBeatManager) listenToHeartBeats() {
	log.Printf("HeartBeat Manager listening to heartbeats")
	for {
		select {
		case <-hbm.ctx.Done():
			log.Printf("HeartBeat sender shutting down")
			return
		default:
		}

		msg, err := hbm.heartBeatReader.ReadMessage(hbm.ctx)
		if err != nil {
			if err == context.Canceled {
				log.Printf("HeartBeat sender failed: %v", err)
				return
			}
			log.Printf("HeartBeat error : %v, retring in is", err)
			time.Sleep(1 * time.Second)
			continue
		}
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("error unmarshaling heartbeat event : %v", err)
			continue
		}
		if event.Type == "node_heartbeat" {
			var heartbeat NodeHartBeat
			if err := json.Unmarshal(event.Data, &heartbeat); err != nil {
				log.Printf("error unmarshaling heartbeat event : %v", err)
				continue
			}
			hbm.handleHeartBeat(heartbeat)

		}

	}
}

func (hbm *HeartBeatManager) handleHeartBeat(heartbeat NodeHartBeat) {
	hbm.mutex.Lock()
	defer hbm.mutex.Unlock()
	hbm.nodeLastSeen[heartbeat.NodeID] = heartbeat.Timestamp
	hbm.nodeUsers[heartbeat.NodeID] = heartbeat.UserIDs

	if hbm.processedDeadNodes[heartbeat.NodeID] {
		log.Printf("Node %s is Alive again! removing from dead list.", heartbeat.NodeID)
		delete(hbm.processedDeadNodes, heartbeat.NodeID)
	}
	log.Printf("Heartbeat receive: node=%s, users=%d, timestamp=%d", heartbeat.NodeID, len(heartbeat.UserIDs), heartbeat.Timestamp)
}

func (hbm *HeartBeatManager) monitorNodeHealth() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	log.Printf("HeartBeat Manager monitoring node health started")
	for {
		select {
		case <-hbm.ctx.Done():
			log.Printf("Node Health monitoring manager shutting down")
			return
		case <-ticker.C:
			hbm.checkDeadNodes()
		}
	}
}
func (hbm *HeartBeatManager) checkDeadNodes() {
	now := time.Now().Unix()
	timeoutSeconds := int64(hbm.timeoutThreshold.Seconds())
	hbm.mutex.Lock()
	var deadNodes []string
	for nodeID, lastSeen := range hbm.nodeLastSeen {
		if nodeID == hbm.nodeID {
			continue
		}
		timeSinceLastSeen := now - lastSeen
		if timeSinceLastSeen > timeoutSeconds {
			if !hbm.processedDeadNodes[nodeID] {
				deadNodes = append(deadNodes, nodeID)
				log.Printf("Node %s is Dead(last seen %d seconds ago)", nodeID, timeSinceLastSeen)
			}
		}
	}
	hbm.mutex.Unlock()
	if len(deadNodes) > 0 {
		if hbm.shouldHandelFailure(deadNodes) {

		}
	}
}
func (hbm *HeartBeatManager) shouldHandelFailure(nodes []string) bool {
	hbm.mutex.RLock()
	defer hbm.mutex.RUnlock()
	aliveNodes := []string{hbm.nodeID}
	now := time.Now().Unix()
	timeoutSeconds := int64(hbm.timeoutThreshold.Seconds())
	for nodeID, lastSeen := range hbm.nodeLastSeen {
		if nodeID == hbm.nodeID {
			continue
		}
		timeSinceLastSeen := now - lastSeen
		if timeSinceLastSeen <= timeoutSeconds {
			aliveNodes = append(aliveNodes, nodeID)
		}
	}
	sort.Strings(aliveNodes)
	if len(aliveNodes) == 0 {
		return true
	}
	designatedNode := aliveNodes[0]
	isDesignated := designatedNode == hbm.nodeID
	if isDesignated {
		log.Printf("Node %s is Designated", designatedNode)
	}
	return false
}
