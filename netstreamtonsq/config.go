package main

import (
	"encoding/json"
	"github.com/goinggo/mapstructure"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	NsqdAddr      string `jpath:"nsqd_addr"`
	Topic         string `jpath:"topic"`
	TcpPort       string `jpath:"tcp_listen_address"`
	UdpPort       string `jpath:"udp_listen_address"`
	WritePoolSize int    `jpath:"write_pool_size"`
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
