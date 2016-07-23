package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	NsqdAddr      string `json:"nsqd_addr"`
	Topic         string `json:"topic"`
	TcpPort       string `json:"tcp_listen_address"`
	UdpPort       string `json:"udp_listen_address"`
	ConsulAddress string `json:"consul_address"`
	Datacenter    string `json:"datacenter"`
	Token         string `json:"consul_token"`
	ConsulKey     string `json:"consul_key"`
	WritePoolSize int    `json:"write_pool_size"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	var s Setting
	if err != nil {
		return &s, err
	}
	defer configFile.Close()
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(config, &s)
	return &s, err
}
