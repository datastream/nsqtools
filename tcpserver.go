package main

import (
	"bufio"
	"github.com/datastream/logplex"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// tcp_server
func run_tcp_server(port string, msg_chan chan *logplex.Msg, exitchan chan int) {
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
					loghandle(fd, msg_chan, exitchan)
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
func loghandle(fd net.Conn, msg_chan chan *logplex.Msg, exitchan chan int) {
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
			msg_chan <- msg
		}
	}()
	<-exitchan
}
