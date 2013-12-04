package main

import (
	"flag"
	"github.com/bitly/go-nsq"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("conf", "config.json", "syslog2nsq config file")
)

var logTopic string

func main() {
	flag.Parse()
	// signal
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	nsqdAddr, _ := c["nsqd_addr"]
	logTopic, _ = c["log_topic"]
	tcpPort, _ := c["tcp_port"]
	udpPort, _ := c["udp_port"]
	termchan := make(chan os.Signal, 1)
	stop_accept := make(chan int)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	// tcp server
	w := nsq.NewWriter(nsqdAddr)
	if err != nil {
		log.Fatal("nsq error", err)
	}
	defer w.Stop()
	go run_tcp_server(tcpPort, w, stop_accept)
	// udp server
	go run_udp_server(udpPort, w, stop_accept)
	<-termchan
	close(stop_accept)
}
