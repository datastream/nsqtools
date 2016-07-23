package main

import (
	"./logformat"
	"bufio"
	"fmt"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/nsqio/go-nsq"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type message struct {
	topic string
	body  [][]byte
}

type StreamServer struct {
	*Setting
	exitChan chan int
	msgChan  chan [][]byte
	wg       sync.WaitGroup
}

func (s *StreamServer) Run() {
	cfg := nsq.NewConfig()
	hostname, _ := os.Hostname()
	cfg.Set("user_agent", fmt.Sprintf("netstream/%s", hostname))
	cfg.Set("snappy", true)
	for i := 0; i < s.WritePoolSize; i++ {
		w, _ := nsq.NewProducer(s.NsqdAddr, cfg)
		go s.writeLoop(w)
	}
	go s.readUDP()
	go s.readTCP()
}

func (s *StreamServer) writeLoop(w *nsq.Producer) {
	for {
		select {
		case msg := <-s.msgChan:
			w.MultiPublish(s.Topic, msg)
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
	udpAddr, err := net.ResolveUDPAddr("udp", s.UdpPort)
	if err != nil {
		log.Fatal("udp:", err)
	}
	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	defer server.Close()
	buf := make([]byte, 8192*8)
	var bodies [][]byte
	b := flatbuffers.NewBuilder(0)
	for {
		select {
		case <-s.exitChan:
			return
		default:
			size, addr, err := server.ReadFromUDP(buf)
			if err != nil {
				log.Println("read log failed", err)
				continue
			}
			fbuf := MakeLog(b, addr.String(), string(buf[:size]))
			bodies = append(bodies, []byte(fbuf))
			if len(bodies) > 100 {
				s.msgChan <- bodies
				bodies = bodies[:0]
			}
		}
	}
}
func (s *StreamServer) readTCP() {
	server, err := net.Listen("tcp", s.TcpPort)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	defer server.Close()
	for {
		select {
		case <-s.exitChan:
			return
		default:
			fd, err := server.Accept()
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
	scanner := bufio.NewScanner(fd)
	scanner.Split(bufio.ScanLines)
	addr := fd.RemoteAddr()
	s.wg.Add(1)
	defer s.wg.Done()
	var bodies [][]byte
	var err error
	b := flatbuffers.NewBuilder(0)
	for {
		select {
		case <-s.exitChan:
			return
		default:
			if scanner.Scan() == false {
				err = scanner.Err()
			}
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			msg := scanner.Text()
			buf := MakeLog(b, addr.String(), msg)
			bodies = append(bodies, []byte(buf))
			if len(bodies) > 100 {
				s.msgChan <- bodies
				bodies = bodies[:0]
			}

		}
	}
}

func MakeLog(b *flatbuffers.Builder, addr string, msg string) []byte {
	b.Reset()
	addr_postion := b.CreateByteString([]byte(addr))
	logformat.LogMessageStart(b)
	logformat.LogMessageAddFrom(b, addr_postion)
	msg_postion := b.CreateByteString([]byte(msg))
	logformat.LogMessageAddRawMsg(b, msg_postion)
	log_end := logformat.LogMessageEnd(b)
	b.Finish(log_end)
	return b.Bytes[b.Head():]
}
