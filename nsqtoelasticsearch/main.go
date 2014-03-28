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
	m := &Builder{
		Setting:     setting,
		dataChannel: make(chan []byte),
		exitChannel: make(chan int),
	}
	if err = m.Run(); err != nil {
		log.Fatal("elastic search builder failed", err)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	m.Stop()
}
