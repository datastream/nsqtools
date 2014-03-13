package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	nsqdAddr      string `json:"nsqd_addr"`
	topic         string `json:"topic"`
	tcpPort       string `json:"tcp_listen_address"`
	udpPort       string `json:"udp_listen_address"`
	writePoolSize int    `json:"write_pool_size"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	setting := &Setting{}
	if err := json.Unmarshal(config, setting); err != nil {
		return nil, err
	}
	return setting, err
}
