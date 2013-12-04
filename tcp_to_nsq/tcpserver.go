package main

import (
	"bufio"
	"github.com/bitly/go-nsq"
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
			w.Publish(topic, []byte(msg))
		}
	}
}
