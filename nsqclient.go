package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

type NsqdServer struct {
	Conn     net.Conn
	NsqdAddr string
	Exitchan chan int
	Done     chan int
}

// lookup allo nsqd node, send nsqd node via nsqd_ch
func connect_nsqd_cluster(lookupdaddrs []string, topic string, logchan chan []byte, exitchan chan int) {
	ticker := time.NewTicker(60 * time.Second)
	var list_lock sync.Mutex
	nsqd_list := make(map[string]*NsqdServer)
	go func() {
		for {
			for _, addr := range lookupdaddrs {
				nsqd_servers := get_nsqd_list(addr)
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

//lookup nsqd from lookupd server
func get_nsqd_list(lookupaddr string) []string {
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
				if err := cmd.Write(rwbuf); err != nil {
					log.Println("write buf error", err)
					break
				}
				if err = rwbuf.Flush(); err != nil {
					log.Println("flush buf error", err)
					break
				}
				resp, err := nsq.ReadResponse(rwbuf)
				if err != nil {
					log.Println("failed to read response", err)
					break
				}
				_, data, err := nsq.UnpackResponse(resp)
				if err != nil {
					log.Println("unpack failed", err)
					continue
				}
				if !bytes.Equal(data, []byte("OK")) || !bytes.Equal(data, []byte("_heartbeat_")) {
					log.Println("response not ok",
						string(data))
					continue
				}
				batch = batch[:0]
			}
		}
	}
	this.Done <- 1
}
