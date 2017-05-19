package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("c", "netstreamtonsq.json", "syslog2nsq config file")
)

func main() {
	flag.Parse()
	// signal
	setting, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	s := &StreamServer{
		exitChan: make(chan int),
		msgChan:  make(chan *LogFormat),
	}
	s.Setting = setting
	go s.Run()
	defer s.Stop()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
}
