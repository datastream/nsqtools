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
	exittcp := make(chan int)
	exitnsq := make(chan int)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-termchan
		wg.Done()
		exittcp <- 1
		exitnsq <- 1
	}()
	logchan := make(chan []byte)
	// tcp server
	go func() {
		wg.Add(1)
		run_server(*port, logchan, exittcp)
		wg.Done()
	}()
	go lookupnsqd(lookupdlist, *topic, logchan, exitnsq)
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
	client_count := 0
	client_exit := make(chan int)
	var client_lock sync.Mutex
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
					client_lock.Lock()
					client_count++
					client_lock.Unlock()
					loghandle(fd, logchan, client_exit)
					client_lock.Lock()
					client_count--
					client_lock.Unlock()
					wg.Done()
				}()
			}
		}
	}()
	<-exitchan
	server.Close()
	client_lock.Lock()
	for i := 0; i < client_count; i++ {
		client_exit <- 1
	}
	client_lock.Unlock()
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

type NsqdServer struct {
	Conn     net.Conn
	NsqdAddr string
	Exitchan chan int
	Done     chan int
}

// lookup allo nsqd node, send nsqd node via nsqd_ch
func lookupnsqd(lookupdaddrs []string, topic string, logchan chan []byte, exitchan chan int) {
	ticker := time.NewTicker(60 * time.Second)
	var list_lock sync.Mutex
	nsqd_list := make(map[string]*NsqdServer)
	go func() {
		for {
			for _, addr := range lookupdaddrs {
				nsqd_servers := get_nsqd(addr)
				for _, nsqd := range nsqd_servers {
					if _, ok := nsqd_list[nsqd]; ok {
						continue
					}
					n := &NsqdServer{
						NsqdAddr: nsqd,
						Exitchan: make(chan int),
						Done:     make(chan int),
					}
					list_lock.Lock()
					nsqd_list[n.NsqdAddr] = n
					list_lock.Unlock()
					go n.message_handler(topic, logchan)
					go func() {
						<-n.Done
						delete(nsqd_list, n.NsqdAddr)
						log.Println("disconnect:", n.NsqdAddr)
					}()
					log.Println("connect", nsqd)
				}
			}
			<-ticker.C
		}
	}()
	<-exitchan
	ticker.Stop()
	list_lock.Lock()
	for _, v := range nsqd_list {
		v.Exitchan <- 1
	}
	list_lock.Unlock()
}
func get_nsqd(lookupaddr string) []string {
	var nsqd_list []string
	endpoint := fmt.Sprintf("http://%s/nodes", lookupaddr)
	log.Printf("LOOKUPD: querying %s", endpoint)
	data, err := nsq.ApiRequest(endpoint)
	if err != nil {
		log.Printf("ERROR: lookupd %s - %s\n", endpoint, err.Error())
	} else {
		producers := data.Get("producers")
		producersArray, _ := producers.Array()
		for i, _ := range producersArray {
			producer := producers.GetIndex(i)
			address := producer.Get("address").MustString()
			tcpPort := producer.Get("tcp_port").MustInt()
			port := strconv.Itoa(tcpPort)
			nsqd_list = append(nsqd_list, address+":"+port)
		}
	}
	return nsqd_list
}

// send msg to nsqd node
func (this *NsqdServer) message_handler(topic string, logchan chan []byte) {
	var err error
	this.Conn, err = net.DialTimeout("tcp", this.NsqdAddr, time.Second)
	if err != nil {
		log.Println("connect failed:", err)
		this.Done <- 1
		return
	}
	defer this.Conn.Close()
	this.Conn.Write(nsq.MagicV2)
	rwbuf := bufio.NewReadWriter(bufio.NewReader(this.Conn), bufio.NewWriter(this.Conn))
	var batch [][]byte
	for {
		select {
		case <-this.Exitchan:
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
					break
				}
				err = rwbuf.Flush()
				if err != nil {
					log.Println("flush buf error", err)
					break
				}
				resp, err := nsq.ReadResponse(rwbuf)
				if err != nil {
					log.Println("failed to read response", err)
					break
				}
				_, data, _ := nsq.UnpackResponse(resp)
				if !bytes.Equal(data, []byte("OK")) {
					log.Println("invalid response", err)
					break
				}
				batch = batch[:0]
			}
		}
	}
	this.Done <- 1
}
