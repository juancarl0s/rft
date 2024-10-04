package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/juancarl0s/rft"
)

// Example run (connect as a client to a server indentified by the number 3):
// go run 07_leader_election/client/main.go 3
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <nodename>")
		return
	}
	nodename := os.Args[1]

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var conn net.Conn

	rft.HandleSignals(conn)

	// Create a TCP socket and connect to the server.
	conn, err := net.Dial("tcp", rft.SERVERS[nodename])
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		os.Exit(1)
	}
	defer conn.Close()

	for {
		reader := bufio.NewReader(os.Stdin)

		fmt.Print("CMD> ")
		cmdBytes, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Println("Error reading message from input:", err)
			panic(err)
		}
		// Remove trailing new line char.
		cmdBytes = cmdBytes[:len(cmdBytes)-1]

		cmd := string(cmdBytes)
		msg := rft.Message{
			MsgType:              rft.SUBMIT_COMMAND_MSG,
			SubmitCommandRequest: &cmd,
		}
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			fmt.Println("Error marshalling JSON:", err)
			panic(err)
		}

		err = rft.Send(conn, []byte(jsonMsg))
		if err != nil {
			fmt.Println("Error sending message from client:", err)
		}
		response, err := rft.Rcv_msg(conn)
		if err != nil {
			fmt.Println("Error receiving message from server:", err)
		}
		fmt.Println("" + string(response))

	}

}
