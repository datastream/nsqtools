package main

import (
	"bufio"
	"encoding/json"
	"github.com/datastream/logplex"
	"github.com/datastream/nsq/nsq"
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
			var topic string
			if len(msg.AppName) > 0 {
				topic = string(msg.AppName)
			} else {
				topic = "misc"
			}
			cmd := nsq.Publish(topic, msg_body)
			_, _, err = w.Write(cmd)
			if err != nil {
				w.ConnectToNSQ(*nsq_address)
				log.Println("Write NSQ error", err)
			}
		}
	}()
	<-exitchan
}
