package main

import (
	"flag"
	"github.com/bitly/nsq/nsq"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	topic            = flag.String("topic", "", "nsq topic")
	channel          = flag.String("channel", "nsq_to_tcp", "nsq channel")
	maxInFlight      = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "127.0.0.1:4161", "lookupd http")
	port             = flag.String("port", ":1514", "log send port")
)

type MsgHandler struct {
	msg_chan chan []byte
}

func (this *MsgHandler) HandleMessage(m *nsq.Message) error {
	this.msg_chan <- m.Body
	return nil
}

func main() {
	flag.Parse()

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)
	msg_handler := MsgHandler{make(chan []byte)}
	r.AddHandler(&msg_handler)
	lookupdlist := strings.Split(*lookupdHTTPAddrs, ",")
	exitchan := make(chan int)
	go tcp_server(*port, msg_handler.msg_chan, exitchan)
	for _, addrString := range lookupdlist {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	select {
	case <-r.ExitChan:
		log.Println("reader exited")
	case <-sigChan:
		r.Stop()
		exitchan <- 1
		log.Println("stop all")
	}
	time.Sleep(time.Second)
}

func tcp_server(port string, logchan chan []byte, exitchan chan int) {
	server, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("server bind failed:", err)
		return
	}
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
				go send_log(fd, logchan)
			}
		}
	}()
	<-exitchan
	server.Close()
	log.Println("tcp server closed")
}

func send_log(fd net.Conn, logchan chan []byte) {
	defer fd.Close()
	var err error
	for {
		msg, ok := <-logchan
		if !ok {
			break
		}
		_, err = fd.Write(msg)
		if err != nil {
			logchan <- msg
			log.Println(err)
			break
		}
	}
}
