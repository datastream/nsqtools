package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nsqio/go-nsq"
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
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	cfg.Set("user_agent", fmt.Sprintf("file_to_nsq/%s", hostname))
	cfg.Set("snappy", true)
	m := &LogTask{}
	m.Writer, _ = nsq.NewProducer(*nsq_address, cfg)
	m.Setting, err = ReadConfig(*conf_file)
	m.LogStat = make(map[string]chan int)
	if err != nil {
		log.Fatal("fail to read config", err)
	}
	m.Run()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	m.Stop()
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
