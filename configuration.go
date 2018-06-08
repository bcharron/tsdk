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
    FlushPeriodMS int
    Senders int
    SendBatchSize int
    DiskBatchSize int
    DiskQueuePath string
    DiskMaxSize int64
    NetworkReadTimeoutMS int
    CompressionCodec string
    Tags map[string]string
}

func (c *Configuration) loadDefaults() {
    c.ListenAddr = ":4242"
    c.Brokers = make([]string, 0)
    c.Topic = "tsdb"
    c.ReceiveBuffer = 10000
    c.MemoryQueueSize = 100000
    c.FlushPeriodMS = 5000
    c.Senders = 5
    c.SendBatchSize = 1000
    c.DiskBatchSize = 1000
    c.DiskQueuePath = "dirq"
    c.DiskMaxSize = 10000000
    c.NetworkReadTimeoutMS = 30000
    c.CompressionCodec = "gzip"
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

