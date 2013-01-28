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
	exitnsq := make(chan int)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-termchan
		wg.Done()
		exitChan <- 1
		exitnsq <- 1
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
	go push_data(nsqd_ch, *topic, logchan, exitnsq)
	wg.Wait()
	log.Println("Server is going down")
	time.Sleep(time.Second * 2)
}

// run_server, listen tcp, it may contain race condtion
func run_server(port string, logchan chan []byte, exitchan chan int) {
	server, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("server bind failed:", err)
	}
	client_count := 0
	client_exit := make(chan int)
	var wg sync.WaitGroup
	go func() {
		for {
			fd, err := server.Accept()
			if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			if err != nil {
				log.Fatal("accept error", err)
				time.Sleep(time.Second)
			} else {
				go func() {
					wg.Add(1)
					client_count++
					loghandle(fd, logchan, client_exit)
					client_count--
					wg.Done()
				}()
			}
		}
	}()
	<-exitchan
	server.Close()
	for i := 0; i < client_count; i++ {
		client_exit <- 1
	}
	wg.Wait()
	log.Println("All connection closed")
}

// receive log from tcp socket, encode json and send to logchan
func loghandle(fd net.Conn, logchan chan []byte, exitchan chan int) {
	defer fd.Close()
	rbuf := bufio.NewReader(fd)
	reader := logplex.NewReader(rbuf)
	go func() {
		for {
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
			if msg_json, err := json.Marshal(msg); err == nil {
				logchan <- msg_json
			} else {
				log.Println("json:", err)
			}
		}
	}()
	<-exitchan
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
	Done     chan int
}

// connect nsqd, receive nsqd's info from nsqd_ch. create connection
func push_data(nsqd_ch chan string, topic string, logchan chan []byte, exitchan chan int) {
	nsqd_list := make(map[string]*NsqdServer)
	nsqd_chan := make(chan string)
	for {
		select {
		case <-exitchan:
			for _, v := range nsqd_list {
				v.Done <- 1
			}
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
				Done:     make(chan int),
			}
			nsqd_list[nsqd] = n
			go message_handler(n, topic, logchan, nsqd_chan)
		case nsq := <-nsqd_chan:
			delete(nsqd_list, nsq)
			log.Println("disconnect:", nsq)
			nsqd_ch <- nsq
		}
	}
}

// send msg to nsqd node
func message_handler(nsqdserver *NsqdServer, topic string, logchan chan []byte, nsqd_chan chan string) {
	defer nsqdserver.Conn.Close()
	nsqdserver.Conn.Write(nsq.MagicV2)
	rwbuf := bufio.NewReadWriter(bufio.NewReader(nsqdserver.Conn), bufio.NewWriter(nsqdserver.Conn))
	var batch [][]byte
	for {
		select {
		case <-nsqdserver.Done:
			cmd, _ := nsq.MultiPublish(topic, batch)
			cmd.Write(rwbuf)
			rwbuf.Flush()
			break
		case line := <-logchan:
			if len(batch) < 20 {
				batch = append(batch, line)
			} else {
				cmd, _ := nsq.MultiPublish(topic, batch)
				err := cmd.Write(rwbuf)
				if err != nil {
					log.Println("write buf error", err)
					nsqd_chan <- nsqdserver.NsqdAddr
					break
				}
				err = rwbuf.Flush()
				if err != nil {
					log.Println("flush buf error", err)
					nsqd_chan <- nsqdserver.NsqdAddr
					break
				}
				resp, err := nsq.ReadResponse(rwbuf)
				if err != nil {
					log.Println("failed to read response", err)
					nsqd_chan <- nsqdserver.NsqdAddr
					break
				}
				_, data, _ := nsq.UnpackResponse(resp)
				if !bytes.Equal(data, []byte("OK")) {
					log.Println("invalid response", err)
					nsqd_chan <- nsqdserver.NsqdAddr
					break
				}
				batch = batch[:0]
			}
		}
	}
}
