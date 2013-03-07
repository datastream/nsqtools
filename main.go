package main

import (
	"flag"
	"github.com/datastream/logplex"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

var (
	port             = flag.String("port", ":1514", "log reciever port")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "127.0.0.1:4161", "lookupd http")
	enable_json      = flag.Bool("enable_json", true, "json encode")
)

func main() {
	flag.Parse()
	// signal
	termchan := make(chan os.Signal, 1)
	stop_accept := make(chan int)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-termchan
		wg.Done()
		close(stop_accept)
	}()
	msg_chan := make(chan *logplex.Msg)
	// tcp server
	go func() {
		wg.Add(1)
		log.Println("Start tcp server at", *port)
		run_tcp_server(*port, msg_chan, stop_accept)
		log.Println("Stop tcp server")
		wg.Done()
	}()
	// udp server
	go func() {
		wg.Add(1)
		log.Println("Start udp server at", *port)
		run_udp_server(*port, msg_chan, stop_accept)
		log.Println("Stop udp server")
		wg.Done()
	}()
	// get lookupd server list
	lookupdlist := strings.Split(*lookupdHTTPAddrs, ",")
	var wg2 sync.WaitGroup
	stop_nsq := make(chan int)
	go func() {
		wg2.Add(1)
		log.Println("start nsqd client")
		connect_nsqd_cluster(lookupdlist, msg_chan, stop_nsq)
		log.Println("cleanup nsqd client")
		wg2.Done()
	}()
	wg.Wait()
	close(stop_nsq)
	close(msg_chan)
	wg2.Wait()
}
