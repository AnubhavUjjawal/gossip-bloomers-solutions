package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// const tick = 10 * time.Millisecond
const timeout = 100 * time.Millisecond
const storageKey = "value"

type StorageValue struct {
	Value int    `json:"value"`
	Id    string `json:"id"`
}

type Message struct {
	Type string `json:"type"`
}

type AddMessage struct {
	Message
	Delta int `json:"delta"`
}

type ReadMessageResponse struct {
	Message
	Value int `json:"value"`
}

type Server struct {
	Node *maelstrom.Node

	KV *maelstrom.KV

	valueChan chan int

	nextOpId      int
	nextOpIdMutex sync.Mutex
}

func (s *Server) NextOpId() string {
	s.nextOpIdMutex.Lock()
	defer s.nextOpIdMutex.Unlock()
	s.nextOpId++
	return fmt.Sprintf("%s_%d", s.Node.ID(), s.nextOpId)
}

func (s *Server) AddHandler() {
	s.Node.Handle("add", func(msg maelstrom.Message) error {
		var body AddMessage
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			log.Println("Error unmarshalling message:", msg, err)
			return err
		}
		log.Println("AddHandler recieved", body)
		// s.valueChan <- body.Delta
		err = s.UpdateValue(body.Delta)
		if err != nil {
			log.Println("Error updating value:", err)
			return err
		}

		s.Node.Reply(msg, Message{
			Type: "add_ok",
		})
		return nil
	})
}

func (s *Server) UpdateValue(val int) error {
	oldVal, err := s.Read()
	if err != nil {
		log.Println("Error reading value:", err)
		return err
	}
	// newVal := oldVal + val
	newVal := StorageValue{
		Value: oldVal.Value + val,
		Id:    s.NextOpId(),
	}

	swapCtx, swapCancel := context.WithTimeout(context.Background(), timeout)
	defer swapCancel()
	err = s.KV.CompareAndSwap(swapCtx, storageKey, oldVal, newVal, true)
	if err != nil {
		log.Println("Error swapping value:", err)
		return err
	}
	return nil
}

func (s *Server) UpdateValueLoop() {
	for val := range s.valueChan {
		err := s.UpdateValue(val)
		for err != nil {
			err = s.UpdateValue(val)
		}

	}
}

func (s *Server) Read() (StorageValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	body, err := s.KV.Read(ctx, storageKey)

	rpcErr, ok := err.(*maelstrom.RPCError)
	if ok && err != nil && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return StorageValue{}, nil
	} else if err != nil {
		return StorageValue{}, err
	}

	log.Println("Read body:", body)
	bodyMap := body.(map[string]interface{})
	val := bodyMap["value"]
	id := bodyMap["id"].(string)

	if err != nil {
		return StorageValue{}, err
	}

	// Do a cas to get the latest value deterministically
	casCtx, casCancel := context.WithTimeout(context.Background(), timeout)
	defer casCancel()

	oldVal := StorageValue{
		Value: int(val.(float64)),
		Id:    id,
	}

	newVal := StorageValue{
		Value: int(val.(float64)),
		Id:    s.NextOpId(),
	}
	err = s.KV.CompareAndSwap(casCtx, storageKey, oldVal, newVal, true)
	if err != nil {
		return StorageValue{}, err
	}
	return newVal, nil
}

func (s *Server) ReadHandler() {
	s.Node.Handle("read", func(msg maelstrom.Message) error {
		var body Message
		err := json.Unmarshal(msg.Body, &body)

		if err != nil {
			log.Println("Error unmarshalling message:", msg, err)
			return err
		}
		val, err := s.Read()
		if err != nil {
			log.Println("Error reading value:", err)
			return err
		}
		s.Node.Reply(msg, ReadMessageResponse{
			Message: Message{
				Type: "read_ok",
			},
			Value: val.Value,
		})
		return nil
	})
}

func main() {
	server := &Server{
		Node:      maelstrom.NewNode(),
		valueChan: make(chan int),
	}
	server.KV = maelstrom.NewSeqKV(server.Node)
	server.AddHandler()
	server.ReadHandler()
	go server.UpdateValueLoop()
	if err := server.Node.Run(); err != nil {
		log.Fatal(err)
	}
}
