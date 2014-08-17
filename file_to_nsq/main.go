package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bitly/go-nsq"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	conf_file   = flag.String("conf", "config.json", "config file")
	nsq_address = flag.String("nsq_address", "127.0.0.1:4150", "nsq")
)

func main() {
	flag.Parse()
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	cfg.Set("user_agent", fmt.Sprintf("file_to_nsq/%s", hostname))
	cfg.Set("snappy", true)
	w, _ := nsq.NewProducer(*nsq_address, cfg)
	setting, err := ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("fail to read config", err)
	}
	exitchan := make(chan int)
	for k, v := range setting {
		go readLog(v, k, w, exitchan)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	close(exitchan)
}

func ReadConfig(file string) (map[string]string, error) {
	var setting map[string]string
	config_file, err := os.Open(file)
	config, err := ioutil.ReadAll(config_file)
	if err != nil {
		return nil, err
	}
	defer config_file.Close()
	if err := json.Unmarshal(config, &setting); err != nil {
		return nil, err
	}
	return setting, nil
}

func readLog(file string, topic string, w *nsq.Producer, exitchan chan int) {
	fd, err := os.Open(file)
	if err != nil {
		log.Println(err)
		return
	}
	defer fd.Close()
	_, err = fd.Seek(0, 2)
	if err != nil {
		return
	}
	reader := bufio.NewReader(fd)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			time.Sleep(time.Second * 10)
			line, err = reader.ReadString('\n')
		}
		if err == io.EOF {
			log.Println("READ EOF")
			size0, err := fd.Seek(0, 1)
			if err != nil {
				return
			}
			fd, err = os.Open(file)
			if err != nil {
				log.Println("open failed", err)
				return
			}
			size1, err := fd.Seek(0, 2)
			if err != nil {
				log.Println(err)
			}
			if size1 < size0 {
				fd.Seek(0, 0)
			} else {
				fd.Seek(size0, 0)
			}
			reader = bufio.NewReader(fd)
			continue
		}
		if err != nil {
			log.Println(err)
			return
		}
		err = w.Publish(topic, []byte(line))
		if err != nil {
			log.Println("NSQ writer", err)
		}
		select {
		case <-exitchan:
			return
		default:
		}
	}
}
