package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/datastream/nsq/nsq"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
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
	offset := read_stat(setting)
	for k, v := range setting {
		go read_log(v, offset[k], k, w, exitchan)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	close(exitchan)
}

func read_stat(setting map[string]string) map[string]int64 {
	stat := make(map[string]int64)
	for k, _ := range setting {
		stat_file, err := os.Open(k)
		if err != nil {
			stat[k] = 0
			continue
		}
		s, err := ioutil.ReadAll(stat_file)
		if err != nil {
			stat[k] = 0
			continue
		}
		i, err := strconv.ParseInt(string(s), 10, 64)
		stat[k] = i
	}
	return stat
}

func sync_stat(stat string, value int64) {
	fd, err := os.Create(stat)
	if err != nil {
		log.Println("fail to create ", stat, err)
	}
	defer fd.Close()
	fd.WriteString(fmt.Sprintf("%d", value))
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

func read_log(file string, offset int64, topic string, w *nsq.Writer, exitchan chan int) {
	fd, err := os.Open(file)
	if err != nil {
		log.Println(err)
		return
	}
	defer fd.Close()
	size, err := fd.Seek(0, 2)
	if err != nil {
		return
	}
	if size < offset {
		fd.Seek(0, 0)
	} else {
		fd.Seek(offset, 0)
	}
	reader := bufio.NewReader(fd)
	tick := time.Tick(time.Second * 10)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			time.Sleep(time.Second*10)
			log.Println("retry")
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
		cmd := nsq.Publish(topic, []byte(line))
		_, _, err = w.Write(cmd)
		if err != nil {
			log.Println("NSQ writer", err)
			w.ConnectToNSQ(*nsq_address)
		}
		select {
		case <-tick:
			size, _ := fd.Seek(0, 1)
			sync_stat(topic, size)
		case <-exitchan:
			size, _ := fd.Seek(0, 1)
			sync_stat(topic, size)
			return
		default:
		}
	}
}
