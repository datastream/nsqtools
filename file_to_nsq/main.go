package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"github.com/datastream/nsq/nsq"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	conf_file   = flag.String("conf", "config.json", "config file")
	nsq_address = flag.String("nsq_address", "127.0.0.1:4150", "nsq")
)

func main() {
	flag.Parse()
	w := nsq.NewWriter()
	err := w.ConnectToNSQ(*nsq_address)
	if err != nil {
		log.Fatal("fail to connect nsq", err)
	}
	setting, err := ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("fail to read config", err)
	}
	exitchan := make(chan int)
	for k, v := range setting {
		go read_log(v, k, w, exitchan)
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

func read_log(file string, topic string, w *nsq.Writer, exitchan chan int) {
	fd, err := os.Open(file)
	if err != nil {
		log.Println(err)
		return
	}
	defer fd.Close()
	reader := bufio.NewReader(fd)
	for {
		select {
		case <-exitchan:
			return
		default:
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				size0, er := fd.Seek(0, 2)
				if er != nil {
					log.Println(er)
					return
				}
				fd, err = os.Open(file)
				if err != nil {
					log.Println(err)
					return
				}
				size1, er := fd.Seek(0, 2)
				if err != nil {
					log.Println(err)
					return
				}
				if size1 != size0 {
					fd.Seek(0, 0)
					reader = bufio.NewReader(fd)
				}
				continue
			}
			if err != nil {
				log.Println(err)
				return
			}
			cmd := nsq.Publish(topic, []byte(line))
			_, _, err = w.Write(cmd)
			if err != nil {
				log.Println("NSQ writer", err)
				w.ConnectToNSQ(*nsq_address)
			}
		}
	}
}
