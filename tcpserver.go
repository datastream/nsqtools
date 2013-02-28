package main

import (
	"bufio"
	"encoding/json"
	"github.com/datastream/logplex"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// tcp_server
func run_tcp_server(port string, logchan chan []byte, exitchan chan int) {
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
					loghandle(fd, logchan, exitchan)
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

// receive log from tcp socket, encode json and send to logchan
func loghandle(fd net.Conn, logchan chan []byte, exitchan chan int) {
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
			if *enable_json {
				if msg_json, err := json.Marshal(msg); err == nil {
					logchan <- msg_json
				} else {
					log.Println("json:", err)
				}
			} else {
				logchan <- msg.Msg
			}
		}
	}()
	<-exitchan
}
