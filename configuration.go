package main

import(
    "os"
    "github.com/golang/glog"
    "encoding/json"
)

type Configuration struct {
    ListenAddr string
    Brokers []string
    Topic string
    ReceiveBuffer int
    MemoryQueueSize int
    FlushPeriod int
    Senders int
    SendBatchSize int
    DiskBatchSize int
    DiskQueuePath string
    Tags map[string]string
}

func (c *Configuration) loadDefaults() {
    c.ListenAddr = ":4242"
    c.Brokers = make([]string, 0)
    c.Topic = "tsdb"
    c.ReceiveBuffer = 10000
    c.MemoryQueueSize = 100000
    c.FlushPeriod = 5
    c.Senders = 5
    c.SendBatchSize = 1000
    c.DiskBatchSize = 1000
    c.DiskQueuePath = "dirq"
    c.Tags = make(map[string]string)
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
    if ! ok {
        config.Tags["host"] = hostname
    }

    return(config)
}

