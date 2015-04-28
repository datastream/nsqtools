package main

import (
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"gopkg.in/fsnotify.v1"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	fileDir    = flag.String("dir", "/tmp", "watch files")
	nsqTopic   = flag.String("topic", "test#test", "nsq topic")
	nsqAddress = flag.String("nsq_address", "127.0.0.1:4150", "nsq")
)

func main() {
	flag.Parse()
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal("fail to get hostname", err)
	}
	cfg.Set("user_agent", fmt.Sprintf("file_to_nsq/%s", hostname))
	cfg.Set("snappy", true)
	w, _ := nsq.NewProducer(*nsqAddress, cfg)
	exitchan := make(chan int)
	go watchFiles(*fileDir, *nsqTopic, w, exitchan)
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	close(exitchan)
}

func watchFiles(fileDir string, topic string, w *nsq.Producer, exitchan chan int) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()
	var f FileList
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if f.Update(event) {
					go f.ReadLog(event.Name, topic, w, exitchan)
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			}
		}
	}()
	err = watcher.Add(fileDir)
	if err != nil {
		log.Fatal(err)
	}
}
