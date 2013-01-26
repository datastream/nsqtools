package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/bmizerany/logplex"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	port             = flag.String("port", ":1514", "log reciever port")
	topic            = flag.String("topic", "nginx_log", "nsq topic")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "127.0.0.1:4161", "lookupd http")
)

func main() {
	flag.Parse()
	// get nsqd list
	lookupdlist := strings.Split(*lookupdHTTPAddrs, ",")
	// signal
	termchan := make(chan os.Signal, 1)
	exitChan := make(chan int)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-termchan
		wg.Done()
		exitChan <- 1
	}()
	logchan := make(chan []byte)
	// tcp server
	go func() {
		wg.Add(1)
		run_server(*port, logchan, exitChan)
		wg.Done()
	}()
	nsqd_ch := make(chan string)
	go lookupnsqd(lookupdlist, nsqd_ch)
	go push_data(nsqd_ch, *topic, logchan)
	wg.Wait()
	log.Println("Server is going down")
	time.Sleep(time.Second * 2)
}

// run_server, listen tcp
func run_server(port string, logchan chan []byte, exitchan chan int) {
	server, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	defer server.Close()
	client_count := 0
	client_exit := make(chan int)
	stat := true
	go func() {
		<-exitchan
		stat = false
		for i := 0; i < client_count; i++ {
			client_exit <- 1
		}
	}()
	var wg sync.WaitGroup
	for {
		if stat {
			fd, err := server.Accept()
			client_count++
			if err != nil {
				log.Println("accept error", err)
			}
			go func() {
				wg.Add(1)
				loghandle(fd, logchan, client_exit)
				wg.Done()
			}()
		} else {
			break
		}
	}
	wg.Wait()
	log.Println("All connection closed")
}

// receive log from tcp socket, encode json and send to logchan
func loghandle(fd net.Conn, logchan chan []byte, exitchan chan int) {
	defer fd.Close()
	rbuf := bufio.NewReader(fd)
	reader := logplex.NewReader(rbuf)
	stat := true
	go func() {
		<-exitchan
		stat = false
	}()
	for {
		if stat {
			msg, err := reader.ReadMsg()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println("read log failed", err)
				var zero time.Time
				fd.SetReadDeadline(zero)
				continue
			}
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				break
			}
			if msg_json, err := json.Marshal(msg); err == nil {
				logchan <- msg_json
			} else {
				log.Println("json:", err)
			}
		} else {
			break
		}
	}
}

// lookup allo nsqd node, send nsqd node via nsqd_ch
func lookupnsqd(lookupdaddrs []string, nsqd_ch chan string) {
	ticker := time.NewTicker(60 * time.Second)
	for {
		for _, addr := range lookupdaddrs {
			endpoint := fmt.Sprintf("http://%s/nodes", addr)
			log.Printf("LOOKUPD: querying %s", endpoint)
			data, err := nsq.ApiRequest(endpoint)
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				continue
			}
			producers := data.Get("producers")
			producersArray, _ := producers.Array()
			for i, _ := range producersArray {
				producer := producers.GetIndex(i)
				address := producer.Get("address").MustString()
				tcpPort := producer.Get("tcp_port").MustInt()
				port := strconv.Itoa(tcpPort)
				nsqd_ch <- address + ":" + port
				log.Println(address, ":", port)
			}
		}
		<-ticker.C
	}
}

type NsqdServer struct {
	Conn     net.Conn
	NsqdAddr string
}

// connect nsqd, receive nsqd's info from nsqd_ch. create connection
func push_data(nsqd_ch chan string, topic string, logchan chan []byte) {
	nsqd_list := make(map[string]*NsqdServer)
	done := make(chan string)
	for {
		select {
		case nsqd := <-nsqd_ch:
			if _, ok := nsqd_list[nsqd]; ok {
				continue
			}
			conn, err := net.DialTimeout("tcp", nsqd, time.Second)
			if err != nil {
				log.Println("connect failed:", err)
				continue
			}
			n := &NsqdServer{
				Conn:     conn,
				NsqdAddr: nsqd,
			}
			nsqd_list[nsqd] = n
			go message_handler(n, topic, logchan, done)
		case nsq := <-done:
			delete(nsqd_list, nsq)
			log.Println("disconnect:", nsq)
			nsqd_ch <- nsq
		}
	}
}

// send msg to nsqd node
func message_handler(nsqdserver *NsqdServer, topic string, logchan chan []byte, done chan string) {
	defer nsqdserver.Conn.Close()
	nsqdserver.Conn.Write(nsq.MagicV2)
	rwbuf := bufio.NewReadWriter(bufio.NewReader(nsqdserver.Conn), bufio.NewWriter(nsqdserver.Conn))
	for {
		var batch [][]byte
		for i := 0; i < 2; i++ {
			line := <-logchan
			if len(line) > 0 {
				batch = append(batch, line)
			}
		}
		cmd, _ := nsq.MultiPublish(topic, batch)
		err := cmd.Write(rwbuf)
		if err != nil {
			log.Println("write buf error", err)
			done <- nsqdserver.NsqdAddr
			break
		}
		err = rwbuf.Flush()
		if err != nil {
			log.Println("flush buf error", err)
			done <- nsqdserver.NsqdAddr
			break
		}
		resp, err := nsq.ReadResponse(rwbuf)
		if err != nil {
			log.Println("failed to read response", err)
			done <- nsqdserver.NsqdAddr
			break
		}
		_, data, _ := nsq.UnpackResponse(resp)
		if !bytes.Equal(data, []byte("OK")) {
			log.Println("invalid response", err)
			done <- nsqdserver.NsqdAddr
			break
		}
	}
}
