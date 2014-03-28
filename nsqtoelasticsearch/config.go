package main

import (
	"encoding/json"
	"github.com/goinggo/mapstructure"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	LookupdAddresses      []string `jpath:"lookupd_addresses"`
	Topic                 string   `jpath:"topic"`
	Channel               string   `jpath:"channel"`
	ElasticSearchHost     string   `jpath:"elasticsearch_host"`
	ElasticSearchPort     string   `jpath:"elasticsearch_port"`
	ElasticsearchIndex    string   `jpath:"elasticsearch_index"`
	ElasticsearchIndexTTL string   `jpath:"elasticsearch_index_ttl"`
	MaxInFlight           int      `jpath:"maxinflight"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	docMap := make(map[string]interface{})
	if err := json.Unmarshal(config, &docMap); err != nil {
		return nil, err
	}
	setting := &Setting{}
	err = mapstructure.DecodePath(docMap, setting)
	return setting, err
}
