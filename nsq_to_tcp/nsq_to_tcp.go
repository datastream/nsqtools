package main

import (
	"encoding/json"
	"flag"
	"github.com/bitly/nsq/go-nsq"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	conf_file = flag.String("conf", "config.json", "config file")
)

type Msg struct {
	Body []byte
	Stat chan error
}
type MsgHandler struct {
	Topic    string
	msg_chan chan Msg
}

func (this *MsgHandler) HandleMessage(m *nsq.Message) error {
	msg := Msg{
		Stat: make(chan error),
	}
	msg.Body = append([]byte(this.Topic+": "), m.Body...)
	this.msg_chan <- msg
	return <-msg.Stat
}

func main() {
	flag.Parse()
	setting, err := ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("fail to read config", err)
	}

	topics := strings.Split(setting["topics"], ",")
	maxInFlight, err := strconv.Atoi(setting["maxinflight"])
	if err != nil {
		maxInFlight = 200
	}
	msg_chan := make(chan Msg)
	var reader_list []*nsq.Reader
	for _, topic := range topics {
		r, err := nsq.NewReader(topic, setting["channel"])
		if err != nil {
			log.Fatalf(err.Error())
		}
		r.SetMaxInFlight(maxInFlight)
		msg_handler := MsgHandler{topic, msg_chan}
		r.AddHandler(&msg_handler)
		lookupdlist := strings.Split(setting["lookupdHTTPAddrs"], ",")
		for _, addrString := range lookupdlist {
			log.Printf("lookupd addr %s", addrString)
			err := r.ConnectToLookupd(addrString)
			if err != nil {
				log.Fatalf(err.Error())
			}
		}
		reader_list = append(reader_list, r)
	}
	exitchan := make(chan int)
	go tcp_server(setting["port"], msg_chan, exitchan)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	<-sigChan
	for _, r := range reader_list {
		r.Stop()
	}
	close(exitchan)
	log.Println("stop all")
	time.Sleep(time.Second)
}

func ReadConfig(file string) (map[string]string, error) {
	var setting map[string]string
	config_file, err := os.Open(file)
	config, err := ioutil.ReadAll(config_file)
	if err != nil {
		return nil, err
	}
	defer config_file.Close()
	if err := json.Unmarshal(config, &setting); err != nil {
		return nil, err
	}
	return setting, nil
}

func tcp_server(port string, msg_chan chan Msg, exitchan chan int) {
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
				go send_log(fd, msg_chan, exitchan)
			}
		}
	}()
	<-exitchan
	server.Close()
	log.Println("tcp server closed")
}

func send_log(fd net.Conn, msg_chan chan Msg, exitchan chan int) {
	defer fd.Close()
	var err error
	for {
		select {
		case <-exitchan:
			return
		case msg := <-msg_chan:
			_, err = fd.Write(msg.Body)
			msg.Stat <- err
			if err != nil {
				log.Println(err)
				return
			}
		}
	}
}
