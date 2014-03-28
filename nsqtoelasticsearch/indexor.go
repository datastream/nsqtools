package main

import (
	"github.com/bitly/go-nsq"
	"github.com/mattbaird/elastigo/api"
	"github.com/mattbaird/elastigo/core"
	"log"
	"time"
)

type Builder struct {
	*Setting
	reader      *nsq.Reader
	dataChannel chan []byte
	exitChannel chan int
}

func (m *Builder) Run() error {
	var err error
	m.reader, err = nsq.NewReader(m.Topic, m.Channel)
	if err != nil {
		log.Println(m.Topic, err)
		return err
	}
	go m.elasticSearchBuildIndex()
	m.reader.SetMaxInFlight(m.MaxInFlight)
	for i := 0; i < m.MaxInFlight; i++ {
		m.reader.AddHandler(m)
	}
	for _, addr := range m.LookupdAddresses {
		err = m.reader.ConnectToLookupd(addr)
		if err != nil {
			break
		}
	}
	return err
}

func (m *Builder) HandleMessage(msg *nsq.Message) error {
	m.dataChannel <- msg.Body
	return nil
}
func (m *Builder) elasticSearchBuildIndex() {
	api.Domain = m.ElasticSearchHost
	api.Port = m.ElasticSearchPort
	indexor := core.NewBulkIndexorErrors(10, 60)
	done := make(chan bool)
	indexor.Run(done)
	for {
		select {
		case errBuf := <-indexor.ErrorChannel:
			log.Println(errBuf.Err)
		case body := <-m.dataChannel:
			timestamp := time.Now()
			indexor.Index(m.ElasticsearchIndex, m.Topic, "", m.ElasticsearchIndexTTL, &timestamp, body)
		case <-m.exitChannel:
			break
		}
	}
	done <- true
}

func (m *Builder) Stop() {
	m.reader.Stop()
	close(m.exitChannel)
}
