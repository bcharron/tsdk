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

var live_senders int32 = 0

type Configuration struct {
    ListenPort int
    Brokers []string
    ReceiveBuffer int
    MemoryQueueSize int
    FlushPeriod int
    Senders int
    SendBatchSize int
    DiskBatchSize int
    Tags map[string]string
}

func sendStats(recvq chan []Metric, prioq chan Metric, qmgr *QueueManager, dqmgr *DiskQueueManager) {
    for {
        now := uint64(time.Now().Unix())

        metrics := make([]Metric, 0, 10)
        metrics = append(metrics, Metric{Metric:"tsdk.memq.count", Value:float64(qmgr.CountMem()), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.memq.limit", Value:float64(qmgr.max), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.metrics.received", Value:float64(qmgr.CountReceived()), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.metrics.sent", Value:float64(qmgr.CountSent()), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.metrics.dropped", Value:float64(qmgr.CountDrops()), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.recvq.count", Value:float64(len(recvq)), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.recvq.limit", Value:float64(cap(recvq)), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.diskq.count", Value:float64(dqmgr.Count()), Timestamp: now, Tags: configuration.Tags})
        metrics = append(metrics, Metric{Metric:"tsdk.senders.count", Value:float64(live_senders), Timestamp: now, Tags: configuration.Tags})

        for _, metric := range metrics {
            prioq <- metric
        }

        <-time.After(time.Second)
    }
}

func showStats(recvq chan []Metric, qmgr *QueueManager, dqmgr *DiskQueueManager) {
    var last_received uint64
    var last_sent uint64

    for {
        <-time.After(time.Second)
        received := qmgr.CountReceived()
        sent := qmgr.CountSent()

        diff_received := received - last_received
        diff_sent := sent - last_sent

        glog.Infof("stats: recvq: %v/%v  memq: %v/%v  diskq: %v/?  recv rate: %v/s  send rate: %v/s  drops: %v  idle senders: %v\n", len(recvq), cap(recvq), qmgr.CountMem(), qmgr.max, dqmgr.Count(), diff_received, diff_sent, qmgr.CountDrops(), len(qmgr.requests_queue))

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

    hostname, _ := os.Hostname()

    _, ok := configuration.Tags["host"]
    if ! ok {
        configuration.Tags["host"] = hostname
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

    disk_enqueue := make(chan []Metric, 100)
    disk_dequeue := make(chan []Metric)

    shutdown_qmgr := make(chan bool)
    qmgr := new(QueueManager)
    qmgr.Init(configuration.MemoryQueueSize, disk_enqueue, disk_dequeue)

    shutdown_dqmgr := make(chan bool)
    dqmgr := new(DiskQueueManager)
    dqmgr.Init("dirq", disk_enqueue, disk_dequeue, shutdown_dqmgr)

    nb_senders := 5

    recvq := make(chan []Metric, configuration.ReceiveBuffer)
    prioq := make(chan Metric, 1000)

    var r Receiver
    r.recvq = recvq

    shutdown_server := make(chan bool, 1)
    go r.server(shutdown_server)
    go showStats(recvq, qmgr, dqmgr)
    go sendStats(recvq, prioq, qmgr, dqmgr)

    qmgr_chan := make(chan QMessage)

    shutdown_channels := make([]chan bool, 0, nb_senders)

    for x := 0; x < nb_senders; x++ {
        name := fmt.Sprintf("sender-%v", x)
        c := make(chan Batch, 1)
        done := make(chan bool)
        shutdown_channels = append(shutdown_channels, done)
        go sender(name, qmgr_chan, c, done, senders_wg)
    }

    go qmgr.queueManager(recvq, prioq, qmgr_chan, shutdown_qmgr)
    go dqmgr.diskQueueManager()

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

    glog.Infof("main: Waiting for queue manager.")
    <-shutdown_qmgr

    glog.Infof("main: Stopping disk queue manager")
    shutdown_dqmgr <- true
    glog.Infof("main: Waiting for disk queue manager.")
    <-shutdown_dqmgr

    glog.Infof("main: All routines finished. Exiting.")
}
