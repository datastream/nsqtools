package main

import (
	"bufio"
	"encoding/json"
	"github.com/datastream/logplex"
	"io"
	"log"
	"net"
	"strings"
)

// udp_server
func run_udp_server(port string, w *Writer, exitchan chan int) {
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
			var msg_body []byte
			if *enable_json {
				if b, err := json.Marshal(msg); err != nil {
					msg_body = b
				} else {
					log.Println(err)
					continue
				}
			} else {
				msg_body = msg.Msg
			}
			if len(msg.AppName) > 0 {
				err = w.Write(string(msg.AppName), msg_body)
			} else {
				err = w.Write("misc", msg_body)
			}
			if err != nil {
				log.Println("Write NSQ error", err)
			}
		}
	}()
	<-exitchan
}
