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
    Tags map[string]string
}

func loadConfig(filename string) *Configuration {
    config := new(Configuration)

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

