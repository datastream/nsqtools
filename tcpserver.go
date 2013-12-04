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
	"sync"
	"time"
)

// tcp_server
func run_tcp_server(port string, w *nsq.Writer, exitchan chan int) {
	server, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	defer server.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	for {
		select {
		case <-exitchan:
			wg.Done()
			wg.Wait()
		default:
			fd, err := server.Accept()
			if err != nil &&
				strings.Contains(err.Error(),
					"use of closed network connection") {
				break
			}
			if err != nil {
				log.Fatal("accept error", err)
				time.Sleep(time.Second)
			} else {
				go func() {
					wg.Add(1)
					loghandle(fd, w, exitchan)
					wg.Done()
				}()
			}
		}
	}
}

// receive log from tcp socket, encode json and send to msg_chan
func loghandle(fd net.Conn, w *nsq.Writer, exitchan chan int) {
	defer fd.Close()
	rbuf := bufio.NewReader(fd)
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
