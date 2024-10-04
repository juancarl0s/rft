package main

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"

	"github.com/juancarl0s/rft"
	"github.com/juancarl0s/rft/kvapp"
)

var raftServer *rft.RaftLogic

// Example runs (start a raft server [identified by number 1] as a leader ):
// go run example/server/main.go 1 leader
//
// Example runs (start a raft server [identified by number 2] as a follower):
// go run example/server/main.go 2 follower
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <nodename> <role>")
		return
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	nodename := os.Args[1]
	role := os.Args[2]

	raftServer = rft.NewRaftLogic(nodename, role, 1)

	var listener net.Listener

	rft.HandleSignals(listener)

	listener, err := net.Listen("tcp", rft.SERVERS[nodename])
	if err != nil {
		slog.Error("Error binding to port:", "error", err)
		log.Fatal(err)
	}
	defer listener.Close()
	slog.Info("Server is listening on", "server", nodename, "addr", rft.SERVERS[nodename])

	raftServer.Listen(listener, kvapp.NewKVApp())
}
