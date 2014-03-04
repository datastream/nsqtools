package main

import (
	"bufio"
	"encoding/json"
	"github.com/bitly/go-nsq"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type StreamServer struct {
	*Setting
	exitChan    chan int
	msgChan     chan []byte
	recoverChan chan string
	wg          sync.WaitGroup
}

func (s *StreamServer) Run() {
	for i := 0; i < s.writePoolSize; i++ {
		w := nsq.NewWriter(s.nsqdAddr)
		go s.writeLoop(w)
	}
	go s.readUDP()
	go s.readTCP()
	go s.recoverServer()
}

func (s *StreamServer) recoverServer() {
	for {
		select {
		case sType := <-s.recoverChan:
			time.Sleep(time.Second)
			log.Println(sType, " reconnecting")
			if sType == "tcp" {
				go s.readTCP()
			}
			if sType == "udp" {
				go s.readUDP()
			}
		case <-s.exitChan:
			return
		}
	}
}

func (s *StreamServer) writeLoop(w *nsq.Writer) {
	for {
		select {
		case msg := <-s.msgChan:
			w.Publish(s.topic, msg)
		case <-s.exitChan:
			return
		}
	}
}
func (s *StreamServer) Stop() {
	close(s.exitChan)
	s.wg.Wait()
}

func (s *StreamServer) readUDP() {
	udpAddr, err := net.ResolveUDPAddr("udp", s.udpPort)
	if err != nil {
		log.Fatal("udp:", err)
	}
	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	defer server.Close()
	buf := make([]byte, 8192)
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case <-s.exitChan:
			return
		default:
			size, addr, err := server.ReadFromUDP(buf)
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				go func() { s.recoverChan <- "udp" }()
				return
			}
			if err == io.EOF {
				go func() { s.recoverChan <- "udp" }()
				return
			}
			if err != nil {
				log.Println("read log failed", err)
				continue
			}
			body, err := logToJSON(addr.String(), string(buf[:size]))
			if err != nil {
				log.Println("failed to parser JSON", err)
				continue
			}
			s.msgChan <- body
		}
	}
}
func (s *StreamServer) readTCP() {
	server, err := net.Listen("tcp", s.tcpPort)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	defer server.Close()
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case <-s.exitChan:
			return
		default:
			fd, err := server.Accept()
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				go func() { s.recoverChan <- "tcp" }()
				return
			}
			if err != nil {
				log.Fatal("accept error", err)
				time.Sleep(time.Second)
			} else {
				go s.loghandle(fd)
			}
		}
	}
}

// receive log from tcp socket, encode json and send to msg_chan
func (s *StreamServer) loghandle(fd net.Conn) {
	defer fd.Close()
	rbuf := bufio.NewReader(fd)
	addr := fd.RemoteAddr()
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case <-s.exitChan:
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
				log.Println("read log failed", err)
				continue
			}
			body, err := logToJSON(addr.String(), msg)
			if err != nil {
				log.Println("failed to parser JSON", err)
				continue
			}
			s.msgChan <- body
		}
	}
}

func logToJSON(addr string, msg string) ([]byte, error) {
	r := make(map[string]string)
	r["from"] = addr
	r["raw_msg"] = msg
	return json.Marshal(r)
}
