package main

import (
	"bufio"
	"github.com/bitly/go-nsq"
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
	for {
		select {
		case <-exitchan:
			return
		default:
			msg, err := rbuf.ReadString('\n')
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Fatal("read log failed", err)
				continue
			}
			w.Publish(topic, msg)
		}
	}
}
