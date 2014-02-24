package main

import (
	"flag"
	"github.com/datastream/nsq/nsq"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	port        = flag.String("port", ":1514", "log reciever port")
	nsq_address = flag.String("nsq_address", "127.0.0.1:4150", "nsq")
	enable_json = flag.Bool("enable_json", true, "json encode")
)

func main() {
	flag.Parse()
	// signal
	termchan := make(chan os.Signal, 1)
	stop_accept := make(chan int)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	// tcp server
	w := nsq.NewWriter()
	err := w.ConnectToNSQ(*nsq_address)
	if err != nil {
		log.Fatal("nsq error", err)
	}
	defer w.Stop()
	go run_tcp_server(*port, w, stop_accept)
	// udp server
	go run_udp_server(*port, w, stop_accept)
	<-termchan
	close(stop_accept)
}