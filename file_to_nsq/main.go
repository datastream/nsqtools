package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	conf_file   = flag.String("conf", "config.json", "config file")
	nsq_address = flag.String("nsq_address", "127.0.0.1:4150", "nsq")
)

func main() {
	flag.Parse()
	setting, err := ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("fail to read config", err)
	}
	exitchan := make(chan int)
	offset := read_stat(setting)
	for k, v := range setting {
		w := nsq.NewWriter(0)
		err := w.ConnectToNSQ(*nsq_address)
		if err != nil {
			log.Fatal("can't connect nsqd")
		}
		go read_log(v, offset[v], k, w, *nsq_address, exitchan)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	close(exitchan)
	time.Sleep(time.Second * 2)
}

func read_stat(setting map[string]string) map[string]int64 {
	stat := make(map[string]int64)
	for _, v := range setting {
		stat_file, err := os.Open(strings.Replace(v, "/", "_", -1))
		if err != nil {
			stat[v] = 0
			continue
		}
		s, err := ioutil.ReadAll(stat_file)
		if err != nil {
			stat[v] = 0
			continue
		}
		i, _ := strconv.ParseInt(string(s), 10, 64)
		stat[v] = i
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

func read_log(file string, offset int64, topic string, w *nsq.Writer, nsqd_addr string, exitchan chan int) {
	defer w.Stop()
	log.Println("read logfile:", file)
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
	tick := time.Tick(time.Second)
	lock_file := strings.Replace(file, "/", "_", -1)
	var body [][]byte
	for {
		select {
		case <-tick:
			size, _ := fd.Seek(0, 1)
			sync_stat(lock_file, size)
		case <-exitchan:
			size, _ := fd.Seek(0, 1)
			sync_stat(lock_file, size)
			return
		default:
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				time.Sleep(time.Second)
				line, err = reader.ReadString('\n')
			}
			if err == io.EOF {
				size0, err := fd.Seek(0, 1)
				if err != nil {
					return
				}
				fd.Close()
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
					log.Println("switch log file", file)
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
			body = append(body, []byte(line))
			if len(body) > 100 {
				_, _, err := w.MultiPublish(topic, body)
				if err != nil {
					log.Println("write failed")
					w.ConnectToNSQ(nsqd_addr)
				}
			}
		}
	}
}
