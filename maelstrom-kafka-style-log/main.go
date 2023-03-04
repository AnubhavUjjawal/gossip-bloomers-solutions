package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const timeout = 10 * time.Millisecond

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

type Server struct {
	*maelstrom.Node

	logsStore *maelstrom.KV
	// logs      map[string][]Msg
	// logsMutex sync.Mutex

	// currentOffset     int
	// currentOffsetLock sync.Mutex

	commitOffsets     map[string]int
	commitOffsetsLock sync.Mutex
}

func (s *Server) UpdateCommitOffsets(topic string, offset int) {
	s.commitOffsetsLock.Lock()
	defer s.commitOffsetsLock.Unlock()

	s.commitOffsets[topic] = offset
}

func (s *Server) GetCommitOffsets(topic string) int {
	s.commitOffsetsLock.Lock()
	defer s.commitOffsetsLock.Unlock()

	return s.commitOffsets[topic]
}

func (s *Server) GetLogsOfTopic(topic string, offset int) []Msg {
	s.logsMutex.Lock()
	defer s.logsMutex.Unlock()

	logs, ok := s.logs[topic]
	if !ok {
		return make([]Msg, 0)
	}
	for i := 0; i < len(logs); i++ {
		if logs[i].Offset >= offset {
			// return at most 2
			e := int(math.Min(float64(i+2), float64(len(logs))))
			return logs[i:e]
		}
	}
	return make([]Msg, 0)
}

func (s *Server) AddToLog(topic string, msg Msg) (int, error) {
	// s.logsMutex.Lock()
	// defer s.logsMutex.Unlock()

	// logs, ok := s.logs[topic]
	// if !ok {
	// 	logs = make([]Msg, 0)
	// }
	// msg.Offset = len(logs)
	// logs = append(logs, msg)
	// s.logs[topic] = logs

	// return msg.Offset
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	logs, err := s.logsStore.Read(ctx, topic)
	if err != nil {
		return 0, err
	}

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
			logs := s.GetLogsOfTopic(topic, offset)
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
			s.UpdateCommitOffsets(topic, offset)
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
			res.Offsets[topic] = s.GetCommitOffsets(topic)
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
		commitOffsets: make(map[string]int),
		logsStore:     maelstrom.NewLinKV(node),
	}
	server.RegisterSendHandler()
	server.RegisterPollHandler()
	server.RegisterCommitOffsetsHandler()
	server.RegisterListCommitOffsetsHandler()

	if err := server.Node.Run(); err != nil {
		log.Fatal(err)
	}
}
