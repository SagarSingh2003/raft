package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"

	raft "github.com/SagarSingh2003/Raft-KV/server/raft_proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	op := flag.String("op", "add", "add operation on the state machine")
	ns := flag.String("ns", "default", "namespaces are partitions in the state machine")
	key := flag.String("key", "", "the key")
	value := flag.String("value", "", "value corresponding to the key")

	flag.Parse()

	makeClientRequest(ns, op, key, value, "")
}

func makeClientRequest(ns, op, key, value *string, leaderId string) {
	address := map[string]string{
		"node1": "localhost:3001",
		"node2": "localhost:3002",
		"node3": "localhost:3003",
		"node4": "localhost:3004",
		"node5": "localhost:3005",
	}

	var nodeId = ""
	if leaderId == "" {
		nodeId = fmt.Sprintf("node%d", rand.Intn(5)+1)
	} else {
		nodeId = leaderId
	}

	addr := address[nodeId]
	fmt.Println("address", addr)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	client := raft.NewRaftClient(conn)
	res, err := client.ClientOperation(context.Background(), &raft.ClientOperationRequest{
		RequestId: "sdfkxlkdf",
		Namespace: *ns,
		Operation: *op,
		Key:       *key,
		Value:     *value,
	})

	if err != nil {
		fmt.Println("error :", err.Error())
		return
	}

	if res.Success == false && res.LeaderId != "" && res.LeaderId != nodeId {
		makeClientRequest(ns, op, key, value, res.LeaderId)
	}

	if res.Success {
		fmt.Println(res)
	}
}
