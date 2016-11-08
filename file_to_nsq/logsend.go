package main

import (
	"bufio"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/nsqio/go-nsq"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type message struct {
	topic string
	body  [][]byte
}

type LogTask struct {
	Writer        *nsq.Producer
	LogStat       map[string]chan int
	CurrentConfig map[string]string
	Setting       map[string]string
	msgChan       chan *message
	client        *api.Client
	exitChan      chan int
}

func (m *LogTask) Run() {
	m.exitChan = make(chan int)
	m.msgChan = make(chan *message)
	ticker := time.Tick(time.Second * 600)
	config := api.DefaultConfig()
	config.Address = m.Setting["consul_address"]
	config.Datacenter = m.Setting["datacenter"]
	config.Token = m.Setting["consul_token"]
	var err error
	m.client, err = api.NewClient(config)
	if err != nil {
		fmt.Println("reload consul setting failed", err)
	}
	err = m.CheckReload()
	if err != nil {
		fmt.Println("reload consul setting failed", err)
	}
	for {
		select {
		case <-ticker:
			err = m.CheckReload()
			if err != nil {
				fmt.Println("reload consul setting failed", err)
			}
		case <-m.exitChan:
			return
		}
	}
}
func (m *LogTask) Stop() {
	close(m.exitChan)
	for _, v := range m.LogStat {
		close(v)
	}
	m.Writer.Stop()
}
func (m *LogTask) ReadConfigFromConsul() (map[string]string, error) {
	consulSetting := make(map[string]string)
	kv := m.client.KV()
	pairs, _, err := kv.List(m.Setting["cluster"], nil)
	if err != nil {
		return consulSetting, err
	}
	size := len(m.Setting["cluster"]) + 1
	for _, value := range pairs {
		if len(value.Key) > size {
			consulSetting[value.Key[size:]] = string(value.Value)
		}
	}
	return consulSetting, err

}
func (m *LogTask) CheckReload() error {
	newConf, err := m.ReadConfigFromConsul()
	if err != nil {
		return err
	}
	for k, _ := range newConf {
		if m.CurrentConfig[k] != newConf[k] {
			if len(m.CurrentConfig[k]) > 0 {
				close(m.LogStat[k])
				delete(m.LogStat, k)
				delete(m.CurrentConfig, k)
			}
			if len(newConf[k]) > 0 {
				fileNames := strings.Split(newConf[k], ",")
				m.LogStat[k] = make(chan int)
				for _, fileName := range fileNames {
					go m.WriteLoop(m.LogStat[k])
					go m.ReadLog(fileName, k, m.LogStat[k])
				}
			}
		}
	}
	for k, _ := range m.CurrentConfig {
		if m.CurrentConfig[k] != newConf[k] {
			if len(newConf[k]) == 0 {
				close(m.LogStat[k])
				delete(m.LogStat, k)
			}
		}
	}
	m.CurrentConfig = newConf
	return nil
}

func (m *LogTask) ReadLog(file string, topic string, exitchan chan int) {
	fd, err := os.Open(file)
	if err != nil {
		log.Println(err)
		return
	}
	defer fd.Close()
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	if len(m.Setting["read_all"]) == 0 {
		_, err = fd.Seek(0, io.SeekEnd)
		if err != nil {
			return
		}
		log.Println("reading from EOF")
	}
	log.Println("reading ", file)
	reader := bufio.NewReader(fd)
	var body [][]byte
	for {
		select {
		case <-exitchan:
			return
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				time.Sleep(time.Second)
				line, err = reader.ReadString('\n')
			}
			if err == io.EOF {
				log.Println(file, "READ EOF")
				size0, err := fd.Seek(0, io.SeekCurrent)
				if err != nil {
					return
				}
				fd, err = os.Open(file)
				if err != nil {
					log.Println("open failed", err)
					return
				}
				size1, err := fd.Seek(0, io.SeekEnd)
				if err != nil {
					log.Println(err)
				}
				if size1 < size0 {
					fd.Seek(0, io.SeekCurrent)
				} else {
					fd.Seek(size0, io.SeekStart)
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
				msg := &message{
					topic: topic,
					body:  body,
				}
				m.msgChan <- msg
				body = body[:0]
			}
		}
	}
}

func (m *LogTask) WriteLoop(exitchan chan int) {
	for {
		select {
		case <-m.exitChan:
			return
		case <-exitchan:
			return
		case msg := <-m.msgChan:
			m.Writer.MultiPublish(msg.topic, msg.body)
		}
	}
}
