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
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	conf_file   = flag.String("conf", "config.json", "config file")
	nsq_address = flag.String("nsq_address", "127.0.0.1:4150", "nsq")
	nsq_conns   = flag.Int("max", 5, "nsq connections")
)

func main() {
	flag.Parse()
	setting, err := ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("fail to read config", err)
	}
	exitchan := make(chan int)
	offset := read_stat(setting)
	var wg sync.WaitGroup
	cmdchan := make(chan *nsq.Command)
	for k, v := range setting {
		go read_log(v, offset[v], k, cmdchan, exitchan)
		wg.Add(1)
	}
	for i := 0; i < *nsq_conns; i++ {
		w := nsq.NewWriter()
		err := w.ConnectToNSQ(*nsq_address)
		if err != nil {
			log.Fatal("fail to connect nsq", err)
		}
		go writerloop(cmdchan, w, exitchan)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	close(exitchan)
	wg.Done()
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

func read_log(file string, offset int64, topic string, cmdchan chan *nsq.Command, exitchan chan int) {
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
			body = append(body, []byte(line))
			if len(body) > 100 {
				cmd, _ := nsq.MultiPublish(topic, body)
				cmdchan <- cmd
				body = body[:0]
			}
		}
	}
}
func writerloop(cmdchan chan *nsq.Command, w *nsq.Writer, exitchan chan int) {
	for {
		select {
		case cmd := <-cmdchan:
			_, _, err := w.Write(cmd)
			if err != nil {
				w.ConnectToNSQ(*nsq_address)
			}
		case <-exitchan:
			return
		}
	}
}
