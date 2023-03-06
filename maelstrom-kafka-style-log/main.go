package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const timeout = 30 * time.Millisecond

type SendRequest struct {
	Type string `json:"type"`
	Msg  int    `json:"msg"`
	Key  string `json:"key"`
}

type SendResponse struct {
	Offset int    `json:"offset"`
	Type   string `json:"type"`
}

type CommitOffsetRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type ListCommitOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommitOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type EmptyResponse struct {
	Type string `json:"type"`
}

type PollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

type Msg struct {
	Offset int
	Value  int
}

func (m Msg) String() string {
	return fmt.Sprintf("(offset: %d value:%d)", m.Offset, m.Value)
}

type Server struct {
	*maelstrom.Node

	dataStore *maelstrom.KV
	// logs      map[string][]Msg
	// logsMutex sync.Mutex

	// currentOffset     int
	// currentOffsetLock sync.Mutex

	// commitOffsets     map[string]int
	// commitOffsetsLock sync.Mutex
}

func (s *Server) UpdateCommitOffsets(topic string, offset int) error {
	key := s.GetCommitOffsetKey(topic)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.dataStore.Write(ctx, key, offset)
}

func (s *Server) GetCommitOffsets(topic string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	currentOffset, err := s.dataStore.ReadInt(ctx, s.GetCommitOffsetKey(topic))
	if err != nil {
		return 0, err
	}
	return currentOffset, nil
}

func InterfaceToMsgType(i interface{}) []Msg {
	switch i.(type) {
	case []Msg:
		break
	default:
		// log.Println("logsUntyped", i)
		logsUntyped := i.([]interface{})
		logs := make([]Msg, len(logsUntyped))
		for i, logUntyped := range logsUntyped {
			log := logUntyped.(map[string]interface{})
			logs[i] = Msg{
				Offset: int(log["Offset"].(float64)),
				Value:  int(log["Value"].(float64)),
			}
		}
		return logs
	}
	return i.([]Msg)
}

func (s *Server) GetTopicKey(topic string) string {
	return fmt.Sprintf("topic_%s", topic)
}

func (s *Server) GetCommitOffsetKey(topic string) string {
	return fmt.Sprintf("commit_offset_%s", topic)
}

func (s *Server) GetLogsOfTopic(topic string, offset int) ([]Msg, error) {

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	key := s.GetTopicKey(topic)
	logsUntyped, err := s.dataStore.Read(ctx, key)

	rpcErr, ok := err.(*maelstrom.RPCError)
	if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
		logsUntyped = make([]Msg, 0)
		err = nil
	} else if err != nil {
		return make([]Msg, 0), err
	}
	logs := InterfaceToMsgType(logsUntyped)
	for _, log := range logs {
		if log.Offset >= offset {
			e := int(math.Min(float64(log.Offset+2), float64(len(logs))))
			return logs[log.Offset:e], nil
		}
	}
	return make([]Msg, 0), nil
}

func (s *Server) AddToLog(topic string, msg Msg) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	key := s.GetTopicKey(topic)
	logsUntyped, err := s.dataStore.Read(ctx, key)

	rpcErr, ok := err.(*maelstrom.RPCError)
	if ok && rpcErr.Code == maelstrom.KeyDoesNotExist {
		logsUntyped = make([]Msg, 0)
		err = nil
	} else if err != nil {
		return 0, err
	}

	logs := InterfaceToMsgType(logsUntyped)
	newLogs := make([]Msg, len(logs)+1)

	copy(newLogs, logs)
	msg.Offset = len(newLogs) - 1
	newLogs[len(newLogs)-1] = msg

	// log.Println("topic", topic, "logs", logs, "newLogs", newLogs, "msg", msg)

	writeCtx, writeCancel := context.WithTimeout(context.Background(), timeout)
	defer writeCancel()
	err = s.dataStore.CompareAndSwap(
		writeCtx,
		key,
		logs,
		newLogs,
		true,
	)
	return msg.Offset, err
}

func (s *Server) CreateMessage(topic string, Value int) (Msg, error) {
	msg := Msg{
		Value: Value,
	}
	offset, err := s.AddToLog(topic, msg)

	if err != nil {
		return Msg{}, err
	}
	msg.Offset = offset
	return msg, nil
}

func (s *Server) RegisterSendHandler() {
	s.Node.Handle("send", func(msg maelstrom.Message) error {
		var req SendRequest
		err := json.Unmarshal(msg.Body, &req)
		if err != nil {
			return err
		}

		log.Printf("Received send request: %+v", req)

		m, err := s.CreateMessage(req.Key, req.Msg)
		if err != nil {
			return err
		}
		res := SendResponse{
			Offset: m.Offset,
			Type:   "send_ok",
		}
		s.Node.Reply(msg, res)
		return nil
	})
}

func (s *Server) RegisterPollHandler() {
	s.Node.Handle("poll", func(msg maelstrom.Message) error {
		var req PollRequest
		err := json.Unmarshal(msg.Body, &req)
		if err != nil {
			return err
		}

		res := PollResponse{
			Type: "poll_ok",
			Msgs: make(map[string][][]int),
		}

		for topic, offset := range req.Offsets {
			logs, err := s.GetLogsOfTopic(topic, offset)
			if err != nil {
				return err
			}
			res.Msgs[topic] = make([][]int, len(logs))
			for i, log := range logs {
				res.Msgs[topic][i] = []int{log.Offset, log.Value}
			}
		}
		log.Println("Sending poll response: ", res)
		s.Node.Reply(msg, res)
		return nil
	})
}

func (s *Server) RegisterCommitOffsetsHandler() {
	s.Node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var req CommitOffsetRequest
		err := json.Unmarshal(msg.Body, &req)
		if err != nil {
			return err
		}

		for topic, offset := range req.Offsets {
			err := s.UpdateCommitOffsets(topic, offset)
			if err != nil {
				return err
			}
		}

		res := EmptyResponse{
			Type: "commit_offsets_ok",
		}
		s.Node.Reply(msg, res)
		return nil
	})
}

func (s *Server) RegisterListCommitOffsetsHandler() {
	s.Node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var req ListCommitOffsetsRequest
		err := json.Unmarshal(msg.Body, &req)
		if err != nil {
			return err
		}

		res := ListCommitOffsetsResponse{
			Type:    "list_committed_offsets_ok",
			Offsets: make(map[string]int),
		}

		for _, topic := range req.Keys {
			off, err := s.GetCommitOffsets(topic)
			if err == nil {
				res.Offsets[topic] = off
			} else {
				res.Offsets[topic] = 0
			}
		}

		s.Node.Reply(msg, res)
		return nil
	})
}

func main() {
	node := maelstrom.NewNode()
	server := &Server{
		Node: node,
		// logs:          make(map[string][]Msg),
		// commitOffsets: make(map[string]int),
		dataStore: maelstrom.NewLinKV(node),
	}
	server.RegisterSendHandler()
	server.RegisterPollHandler()
	server.RegisterCommitOffsetsHandler()
	server.RegisterListCommitOffsetsHandler()

	if err := server.Node.Run(); err != nil {
		log.Fatal(err)
	}
}
