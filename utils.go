package rft

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func HandleSignals(funcs ...io.Closer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigs
		for _, f := range funcs {
			f.Close()
		}
		os.Exit(1)
	}()

}

func rcv_exactly(conn net.Conn, nBytes int) ([]byte, error) {
	sizeChunk := make([]byte, 10)
	_, err := conn.Read(sizeChunk)
	if err != nil {
		return nil, err
	}

	// Step 1: Convert the byte slice to a string
	sizeStr := string(sizeChunk)

	// Step 2: Trim any leading  whitespace
	sizeStr = strings.TrimSpace(sizeStr)

	// Step 3: Parse to int
	sizeToRead, err := strconv.Atoi(sizeStr)
	if err != nil {
		fmt.Println("Error converting size to integer:", err)
		return nil, err
	}

	buf := make([]byte, sizeToRead)
	var bytesRead int
	for bytesRead < sizeToRead {
		// Step 4: Read the number of bytes specified by the size until we fill the buffer
		nRead, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("EOF reached:", err)
				break
			}
			fmt.Println("Error reading from connection:", err)
			return nil, err
		}
		bytesRead += nRead
	}

	return buf, nil
}

func Rcv_msg(conn net.Conn) ([]byte, error) {
	return rcv_exactly(conn, 10)
}

func Send(conn net.Conn, msg []byte) error {
	// fmt.Println("len(msg): ", len(msg), "msg: ", string(msg), "|")
	size := []byte(fmt.Sprintf("%10d", len(msg)))
	_, err := conn.Write(append(size, []byte(msg)...))
	if err != nil {
		fmt.Println("Error sending message:", err)
		return err
	}

	return nil
}
