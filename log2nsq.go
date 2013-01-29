package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
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
		run_tcp_server(*port, logchan, exittcp)
		wg.Done()
	}()
	go lookupnsqd(lookupdlist, *topic, logchan, exitnsq)
	wg.Wait()
	log.Println("Server is going down")
	time.Sleep(time.Second * 2)
}
