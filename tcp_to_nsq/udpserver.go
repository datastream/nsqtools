package main

import (
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
	buf := make([]byte, 8192)
	for {
		select {
		case <-exitchan:
			return
		default:
			size, _, err := server.ReadFromUDP(buf)
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Println("read log failed", err)
				continue
			}
			w.Publish(topic, []byte(buf[:size]))
		}
	}
}
