package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

var (
	port             = flag.String("port", ":1514", "log reciever port")
	topic            = flag.String("topic", "nginx_log", "nsq topic")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "127.0.0.1:4161", "lookupd http")
)

func main() {
	flag.Parse()
	// signal
	termchan := make(chan os.Signal, 1)
	exittcp := make(chan int)
	exitudp := make(chan int)
	exitnsq := make(chan int)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-termchan
		wg.Done()
		exittcp <- 1
		exitudp <- 1
		exitnsq <- 1
	}()
	logchan := make(chan []byte)
	// tcp server
	go func() {
		wg.Add(1)
		log.Println("Start tcp server at", *port)
		run_tcp_server(*port, logchan, exittcp)
		log.Println("Stop tcp server")
		wg.Done()
	}()
	// udp server
	go func() {
		wg.Add(1)
		log.Println("Start udp server at", *port)
		run_udp_server(*port, logchan, exitudp)
		log.Println("Stop udp server")
		wg.Done()
	}()
	// get lookupd server list
	lookupdlist := strings.Split(*lookupdHTTPAddrs, ",")
	go func() {
		wg.Add(1)
		log.Println("start nsqd client")
		connect_nsqd_cluster(lookupdlist, *topic, logchan, exitnsq)
		log.Println("cleanup nsqd client")
		wg.Done()
	}()
	wg.Wait()
}
