package main

import (
	"flag"

	raft "github.com/SagarSingh2003/Raft-KV/server/node"
)

func main() {
	id := flag.String("id", "node1", "give a unique nodeId to the node")

	flag.Parse()

	server := &raft.Server{}
	raft.StartNode(id, &server, "./server/config.yaml")
}
