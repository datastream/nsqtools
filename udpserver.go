package main

import (
	"bufio"
	"encoding/json"
	"github.com/bmizerany/logplex"
	"io"
	"log"
	"net"
	"strings"
)

// udp_server
func run_udp_server(port string, logchan chan []byte, exitchan chan int) {
	udp_addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatal("udp:", err)
		return
	}
	server, err := net.ListenUDP("udp", udp_addr)
	defer server.Close()
	if err != nil {
		log.Fatal("server bind failed:", err)
		return
	}
	rbuf := bufio.NewReader(server)
	reader := logplex.NewReader(rbuf)
	go func() {
		for {
			msg, err := reader.ReadMsg()
			if err != nil &&
				strings.Contains(err.Error(),
					"use of closed network connection") {
				break
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("read log failed", err)
				continue
			}
			if msg_json, err := json.Marshal(msg); err == nil {
				logchan <- msg_json
			} else {
				log.Println("json:", err)
			}
		}
	}()
	<-exitchan
}
