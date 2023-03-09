package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	timeout = 100 * time.Millisecond
)

type DataStore struct {
	data    map[int]int
	dataMut sync.Mutex
}

type Msg struct {
	Type  string        `json:"type"`
	MsgId int           `json:"msg_id"`
	Txn   []interface{} `json:"txn"`
}

type MsgResponse struct {
	Type      string        `json:"type"`
	InReplyTo int           `json:"in_reply_to"`
	Txn       []interface{} `json:"txn"`
}

type KeyVal struct {
	Key   int `json:"key"`
	Value int `json:"value"`
}

func (ds *DataStore) Get(key int) int {
	// ds.dataMut.Lock()
	// defer ds.dataMut.Unlock()
	return ds.data[key]
}

func (ds *DataStore) Set(key, value int) {
	// ds.dataMut.Lock()
	// defer ds.dataMut.Unlock()
	ds.data[key] = value
}

func (ds *DataStore) Update(key, value int) {
	// ds.dataMut.Lock()
	// defer ds.dataMut.Unlock()
	ds.data[key] = value
}

func SendTillSuccessfull(node *maelstrom.Node, data []interface{}, nodeID string) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		msg := Msg{Type: "value_update", MsgId: 0, Txn: data}
		_, err := node.SyncRPC(ctx, nodeID, msg)
		cancel()
		if err == nil {
			log.Println("Sent to", nodeID, data)
			return
		} else {
			log.Println("Failed to send to", nodeID, data, err)
		}
	}
}

func RunSync(node *maelstrom.Node, dataChan chan []interface{}) {
	for node.ID() == "" {
		time.Sleep(10 * time.Millisecond)
	}
	for _, neighbor := range node.NodeIDs() {
		if neighbor != node.ID() {
			log.Println("Starting sync with", neighbor)
			go func(n string) {
				for {
					data := <-dataChan
					log.Println("Sending to", n, data)
					SendTillSuccessfull(node, data, n)
				}
			}(neighbor)
		}
	}
}

func (ds *DataStore) doTransactions(txns []interface{}) []interface{} {
	toReturn := make([]interface{}, 0)
	ds.dataMut.Lock()
	defer ds.dataMut.Unlock()
	for _, op := range txns {
		opType := op.([]interface{})[0].(string)
		switch opType {
		case "r":
			key := int(op.([]interface{})[1].(float64))
			value := ds.Get(key)
			toReturn = append(toReturn, []interface{}{"r", key, value})
		case "w":
			key := int(op.([]interface{})[1].(float64))
			value := int(op.([]interface{})[2].(float64))
			ds.Set(key, value)
			toReturn = append(toReturn, []interface{}{"w", key, value})
		}
	}
	return toReturn
}

func main() {
	dataStore := DataStore{data: make(map[int]int)}
	node := maelstrom.NewNode()
	updateChan := make(chan []interface{})

	node.Handle("txn", func(data maelstrom.Message) error {
		var msg Msg

		err := json.Unmarshal(data.Body, &msg)
		if err != nil {
			return err
		}
		msgRes := MsgResponse{Type: "txn_ok", InReplyTo: msg.MsgId, Txn: []interface{}{}}
		msgRes.Txn = dataStore.doTransactions(msg.Txn)
		go func() {
			updateChan <- msgRes.Txn
		}()
		node.Reply(data, msgRes)
		return nil
	})

	node.Handle("value_update", func(data maelstrom.Message) error {
		var msgs Msg
		err := json.Unmarshal(data.Body, &msgs)
		if err != nil {
			return err
		}
		log.Println("Got update", msgs.Txn)
		dataStore.doTransactions(msgs.Txn)
		log.Println("Updated")
		res := MsgResponse{Type: "value_update_ok", InReplyTo: msgs.MsgId, Txn: []interface{}{}}
		node.Reply(data, res)
		return nil
	})

	go RunSync(node, updateChan)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

}
