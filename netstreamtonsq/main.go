package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("conf", "config.json", "syslog2nsq config file")
)

func main() {
	flag.Parse()
	// signal
	setting, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	s := &StreamServer{
		exitChan:    make(chan int),
		recoverChan: make(chan string),
		msgChan:     make(chan []byte),
	}
	s.Setting = setting
	s.Run()
	defer s.Stop()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
}
