package main

import (
	"fmt"
	"github.com/mattbaird/elastigo/lib"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"time"
)

type Builder struct {
	*Setting
	consumer    *nsq.Consumer
	dataChannel chan []byte
	exitChannel chan int
}

func (m *Builder) Run() error {
	var err error
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	cfg.Set("user_agent", fmt.Sprintf("metric_processor/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.consumer, err = nsq.NewConsumer(m.Topic, m.Channel, cfg)
	if err != nil {
		log.Println(m.Topic, err)
		return err
	}
	go m.elasticSearchBuildIndex()
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	if err != nil {
		return err
	}
	return err
}

func (m *Builder) HandleMessage(msg *nsq.Message) error {
	m.dataChannel <- msg.Body
	return nil
}
func (m *Builder) elasticSearchBuildIndex() {
	c := elastigo.NewConn()
	c.Domain = m.ElasticSearchHost
	indexor := c.NewBulkIndexerErrors(10, 60)
	indexor.Start()
	defer indexor.Stop()
	for {
		select {
		case errBuf := <-indexor.ErrorChannel:
			log.Println(errBuf.Err)
		case body := <-m.dataChannel:
			timestamp := time.Now()
			indexor.Index(m.ElasticsearchIndex, m.Topic, "", "", m.ElasticsearchIndexTTL, &timestamp, body)
		case <-m.exitChannel:
			break
		}
	}
}

func (m *Builder) Stop() {
	m.consumer.Stop()
	close(m.exitChannel)
}
