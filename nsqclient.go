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

type NsqdClient struct {
	net.Conn
	NsqdAddr string
}

// lookup allo nsqd node, send nsqd node via nsqd_ch
func connect_nsqd_cluster(lookupdaddrs []string, topic string, logchan chan []byte, exitchan chan int) {
	ticker := time.NewTicker(30 * time.Second)
	var list_lock sync.Mutex
	nsqd_list := make(map[string]*NsqdClient)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			for _, addr := range lookupdaddrs {
				nsqd_servers := get_nsqd_list(addr)
				for _, nsqd := range nsqd_servers {
					if _, ok := nsqd_list[nsqd]; ok {
						continue
					}
					n := &NsqdClient{
						NsqdAddr: nsqd,
					}
					list_lock.Lock()
					nsqd_list[n.NsqdAddr] = n
					list_lock.Unlock()
					go func() {
						wg.Add(1)
						for {
							err := n.message_handler(topic, logchan, exitchan)
							if err == nil {
								break
							}
						}
						list_lock.Lock()
						delete(nsqd_list, n.NsqdAddr)
						list_lock.Unlock()
						log.Println("disconnect:", n.NsqdAddr)
						wg.Done()
					}()
					log.Println("connect", nsqd)
				}
			}
			<-ticker.C
		}
	}()
	_, ok := <-exitchan
	if !ok {
		ticker.Stop()
		wg.Done()
		wg.Wait()
	}

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
func (this *NsqdClient) message_handler(topic string, logchan chan []byte, exitchan chan int) error {
	var err error
	this.Conn, err = net.DialTimeout("tcp", this.NsqdAddr, time.Second)
	if err != nil {
		log.Println("connect failed:", err)
		return err
	}
	defer this.Conn.Close()
	this.Conn.Write(nsq.MagicV2)
	rwbuf := bufio.NewReadWriter(bufio.NewReader(this.Conn), bufio.NewWriter(this.Conn))
	var batch [][]byte
	for {
		select {
		case <-exitchan:
			cmd, _ := nsq.MultiPublish(topic, batch)
			cmd.Write(rwbuf)
			rwbuf.Flush()
			return nil
		case line := <-logchan:
			if len(batch) < 20 {
				batch = append(batch, line)
			} else {
				cmd, _ := nsq.MultiPublish(topic, batch)
				if err := cmd.Write(rwbuf); err != nil {
					log.Println("write buf error", err)
					return err
				}
				if err = rwbuf.Flush(); err != nil {
					log.Println("flush buf error", err)
					return err
				}
				resp, err := nsq.ReadResponse(rwbuf)
				if err != nil {
					log.Println("failed to read response", err)
					return err
				}
				_, data, err := nsq.UnpackResponse(resp)
				if err != nil {
					log.Println("unpack failed", err)
					continue
				}
				if !bytes.Equal(data, []byte("OK")) && !bytes.Equal(data, []byte("_heartbeat_")) {
					log.Println("response not ok",
						string(data))
					continue
				}
				batch = batch[:0]
			}
		}
	}
	return nil
}
