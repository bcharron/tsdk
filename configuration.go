package main

import (
	"encoding/json"
	"github.com/golang/glog"
	"os"
)

type Configuration struct {
	ListenAddr           string
	Brokers              []string
	Topic                string
	ReceiveBufferSize    int
	MemoryQueueSize      int
	FlushPeriodMS        int
	Senders              int
	SendBatchSize        int
	DiskBatchSize        int
	DiskQueuePath        string
	DiskQueueBuffer      int
	DiskMaxSize          int64
	NetworkReadTimeoutMS int
	CompressionCodec     string
	Tags                 map[string]string
	MaxTags              int
	KafkaVersion         string
	KafkaBatchSize       int
}

func (c *Configuration) loadDefaults() {
	c.ListenAddr = ":4242"
	c.Brokers = make([]string, 0)
	c.Topic = "tsdb"
	c.ReceiveBufferSize = 10000
	c.MemoryQueueSize = 100000
	c.FlushPeriodMS = 5000
	c.Senders = 5
	c.SendBatchSize = 1000
	c.DiskBatchSize = 1000
	c.DiskQueuePath = "dirq"
	c.DiskQueueBuffer = 10000
	c.DiskMaxSize = 10000000
	c.NetworkReadTimeoutMS = 30000
	c.CompressionCodec = "gzip"
	c.Tags = make(map[string]string)
	c.MaxTags = 8
	c.KafkaVersion = "2.3.0"
	c.KafkaBatchSize = 1000
}

func loadConfig(filename string) *Configuration {
	config := new(Configuration)
	config.loadDefaults()

	file, err := os.Open(filename)
	if err != nil {
		glog.Fatalf("Unable to open config file: %v", err)
		os.Exit(1)
	}

	defer file.Close()
	decoder := json.NewDecoder(file)

	err = decoder.Decode(config)
	if err != nil {
		glog.Fatal("error:", err)
		os.Exit(1)
	}

	hostname, _ := os.Hostname()

	_, ok := config.Tags["host"]
	if !ok {
		config.Tags["host"] = hostname
	}

	return (config)
}
