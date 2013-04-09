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
	"sync"
	"time"
)

// tcp_server
func run_tcp_server(port string, w *nsq.Writer, exitchan chan int) {
	server, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("server bind failed:", err)
		return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
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
	}()
	_, _ = <-exitchan
	server.Close()
	wg.Done()
	wg.Wait()
}

// receive log from tcp socket, encode json and send to msg_chan
func loghandle(fd net.Conn, w *nsq.Writer, exitchan chan int) {
	defer fd.Close()
	rbuf := bufio.NewReader(fd)
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
				log.Println("Write NSQ error", err)
			}
		}
	}()
	<-exitchan
}
