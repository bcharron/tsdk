// Copyright 2018 Benjamin Charron. All rights reserved.
// Use of this source code is governed by the GPL 3.0
// license that can be found in the LICENSE file.

package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "github.com/golang/glog"
    "os"
    "os/signal"
    "syscall"
    "sync"
    "time"
)

const VERSION string = "0.1"

var configuration Configuration

type Configuration struct {
    ListenPort int
    Brokers []string
    ReceiveBuffer int
    MemoryQueueSize int
    Senders int
    SendBatchSize int
    DiskBatchSize int
}

func sendStats(prioq chan Metric, qmgr *QueueManager) {
    hostname, _ := os.Hostname()

    for {
        tags := map[string]string{"host": hostname}
        metric := Metric{Metric:"tsdk.memq.count", Value:float64(qmgr.CountMem()), Timestamp: uint64(time.Now().Unix()), Tags: tags}
        prioq <- metric
        <-time.After(time.Second)
    }
}

func showStats(recvq chan []Metric, qmgr *QueueManager) {
    var last_received int
    var last_sent int

    for {
        <-time.After(time.Second)
        received := qmgr.received
        sent := qmgr.sent

        diff_received := received - last_received
        diff_sent := sent - last_sent

        glog.Infof("stats: recvq: %v/%v  memq: %v/%v  diskq: %v/?  recv rate: %v/s  send rate: %v/s  drops: %v  idle senders: %v\n", len(recvq), cap(recvq), qmgr.CountMem(), qmgr.max, qmgr.CountDisk(), diff_received, diff_sent, qmgr.Drops(), len(qmgr.requests_queue))

        last_received = received
        last_sent = sent
    }
}

func loadConfig(filename string) {
    file, err := os.Open(filename);
    if err != nil {
        glog.Fatalf("Unable to open config file: %v", err)
        os.Exit(1)
    }

    defer file.Close()
    decoder := json.NewDecoder(file)

    err = decoder.Decode(&configuration)
    if err != nil {
        glog.Fatal("error:", err)
        os.Exit(1)
    }
}

func main() {
    flag.Parse()

    loadConfig("config.json")

    fmt.Println("Brokers:", configuration.Brokers)
    fmt.Println("ListenPort:", configuration.ListenPort)

    // Senders need to be shutdown before queue managers.
    senders_wg := new(sync.WaitGroup)

    glog.Info("Starting")

    qmgr := new(QueueManager)
    qmgr.Init(configuration.MemoryQueueSize, "qdir")

    nb_senders := 5

    recvq := make(chan []Metric, configuration.ReceiveBuffer)
    prioq := make(chan Metric, 1000)

    var r Receiver
    r.recvq = recvq

    shutdown_server := make(chan bool, 1)
    go r.server(shutdown_server)
    go showStats(recvq, qmgr)
    go sendStats(prioq, qmgr)

    qmgr_chan := make(chan QMessage)

    shutdown_channels := make([]chan bool, 0, nb_senders)

    for x := 0; x < nb_senders; x++ {
        name := fmt.Sprintf("sender-%v", x)
        c := make(chan Batch, 1)
        done := make(chan bool)
        shutdown_channels = append(shutdown_channels, done)
        go sender(name, qmgr_chan, c, done, senders_wg)
    }

    shutdown_qmgr := make(chan bool)
    go qmgr.queueManager(1000, recvq, prioq, qmgr_chan, shutdown_qmgr)

    c := make(chan os.Signal, 2)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)

    // XXX: Handle senders dying

    sig := <-c
    glog.Infof("main: Received signal: %v", sig)

    shutdown_server <- true

    glog.Infof("main: Stopping all senders")
    for _, sc := range shutdown_channels {
        go func(c chan bool) {
            c <- true
        }(sc)
    }

    senders_wg.Wait()

    glog.Infof("main: Stopping queue manager")
    shutdown_qmgr <- true

    glog.Infof("Waiting for queue manager.")
    <-shutdown_qmgr

    glog.Infof("All routines finished. Exiting.")
}
