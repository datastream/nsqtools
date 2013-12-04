package main

import (
	"bufio"
	"encoding/json"
	"github.com/bitly/go-nsq"
	"github.com/datastream/logplex"
	"io"
	"log"
	"net"
	"strings"
)

// udp_server
func run_udp_server(port string, w *nsq.Writer, exitchan chan int) {
	udp_addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatal("udp:", err)
	}
	server, err := net.ListenUDP("udp", udp_addr)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	defer server.Close()
	rbuf := bufio.NewReader(server)
	reader := logplex.NewReader(rbuf)
	for {
		select {
		case <-exitchan:
			return
		default:
			msg, err := reader.ReadMsg()
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("read log failed", err)
				continue
			}
			var msg_body []byte
			if b, err := json.Marshal(msg); err != nil {
				msg_body = b
			} else {
				log.Println(err)
				continue
			}
			w.Publish(logTopic, msg_body)
		}
	}
}
