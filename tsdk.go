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
    "strconv"
    "syscall"
    "sync"
    "time"
)

const VERSION string = "0.2.1"

var configuration *Configuration

var live_senders int32 = 0

func makeMetric(name string, value interface{}, t uint64) Metric {
    var s string

    switch value.(type) {
        case int32: s = strconv.FormatInt(int64(value.(int32)), 10)
        case int: s = strconv.FormatInt(int64(value.(int)), 10)
        case int64: s = strconv.FormatInt(value.(int64), 10)
        case uint64: s = strconv.FormatUint(value.(uint64), 10)
    }

    n := json.Number(s)

    metric := Metric{Metric:name, Value:n, Timestamp: t, Tags: configuration.Tags}

    return metric
}

func sendStats(recvq chan MetricList, prioq chan *Metric, qmgr *QueueManager, dqmgr *DiskQueueManager, counters *Counters) {
    for {
        now := uint64(time.Now().Unix())

        metrics := make([]Metric, 0, 10)
        metrics = append(metrics, makeMetric("tsdk.memq.count", qmgr.CountMem(), now))
        metrics = append(metrics, makeMetric("tsdk.memq.limit", qmgr.max, now))
        metrics = append(metrics, makeMetric("tsdk.metrics.received", counters.received, now))
        metrics = append(metrics, makeMetric("tsdk.metrics.sent", counters.sent, now))
        metrics = append(metrics, makeMetric("tsdk.metrics.dropped", counters.dropped, now))
        metrics = append(metrics, makeMetric("tsdk.metrics.dropped_disk_full", counters.droppedDiskFull, now))
        metrics = append(metrics, makeMetric("tsdk.metrics.invalid", counters.invalid, now))
        metrics = append(metrics, makeMetric("tsdk.http_errors", counters.http_errors, now))
        metrics = append(metrics, makeMetric("tsdk.recvq.count", len(recvq), now))
        metrics = append(metrics, makeMetric("tsdk.recvq.limit", cap(recvq), now))
        metrics = append(metrics, makeMetric("tsdk.diskq.count", dqmgr.Count(), now))
        metrics = append(metrics, makeMetric("tsdk.diskq.usage", dqmgr.GetDiskUsage(), now))
        metrics = append(metrics, makeMetric("tsdk.senders.count", live_senders, now))
        metrics = append(metrics, makeMetric("tsdk.send.failed", counters.sendFailed, now))
        metrics = append(metrics, makeMetric("tsdk.send.serializationError", counters.serializationError, now))

        for idx, _ := range metrics {
            prioq <- &metrics[idx]
        }

        <-time.After(time.Second)
    }
}

func showStats(recvq chan MetricList, qmgr *QueueManager, dqmgr *DiskQueueManager, counters *Counters) {
    var last_received uint64
    var last_sent uint64

    for {
        <-time.After(time.Second)

        received := counters.received
        sent := counters.sent

        diff_received := received - last_received
        diff_sent := sent - last_sent

        glog.Infof("stats: recvq: %v/%v  memq: %v/%v  diskq: %v  recv rate: %v/s  send rate: %v/s  drops: %v  idle senders: %v\n", len(recvq), cap(recvq), qmgr.CountMem(), qmgr.max, dqmgr.Count(), diff_received, diff_sent, counters.dropped + counters.droppedDiskFull, len(qmgr.requests_queue))

        last_received = received
        last_sent = sent
    }
}

func waitForSenders(wg *sync.WaitGroup, done chan bool) {
    // Give a chance for the senders to start.
    <-time.After(1 * time.Second)

    wg.Wait()
    done <- true
}

func main() {
    config_filename := flag.String("c", "config.json", "Path to the config JSON file")
    show_version := flag.Bool("version", false, "Show version")
    flag.Parse()

    if *show_version {
        fmt.Printf("tsdk version %v\n", VERSION)
        return
    }

    configuration = loadConfig(*config_filename)
    counters := new(Counters)

    glog.Infof("Brokers: %v", configuration.Brokers)
    glog.Infof("ListenAddr: %v", configuration.ListenAddr)

    // Senders need to be shutdown before queue managers.
    senders_wg := new(sync.WaitGroup)

    glog.Infof("Starting tsdk version %v", VERSION)

    disk_enqueue := make(chan MetricList, 100)
    disk_dequeue := make(chan MetricList)

    shutdown_qmgr := make(chan bool)
    qmgr := new(QueueManager)
    qmgr.Init(configuration, disk_enqueue, disk_dequeue, counters)

    shutdown_dqmgr := make(chan bool)
    dqmgr := new(DiskQueueManager)
    dqmgr.Init(configuration, disk_enqueue, disk_dequeue, shutdown_dqmgr, counters)

    nb_senders := 5

    recvq := make(chan MetricList, configuration.ReceiveBuffer)
    prioq := make(chan *Metric, 1000)

    r := new(Receiver)
    r.Init(recvq, counters)

    shutdown_server := make(chan bool, 1)
    go r.server(shutdown_server)
    go showStats(recvq, qmgr, dqmgr, counters)
    go sendStats(recvq, prioq, qmgr, dqmgr, counters)

    qmgr_chan := make(chan QMessage)

    shutdown_channels := make([]chan bool, 0, nb_senders)

    for x := 0; x < nb_senders; x++ {
        name := fmt.Sprintf("sender-%v", x)
        c := make(chan Batch, 1)
        done := make(chan bool)
        shutdown_channels = append(shutdown_channels, done)
        go sender(name, qmgr_chan, c, done, senders_wg, counters)
    }

    go qmgr.queueManager(recvq, prioq, qmgr_chan, shutdown_qmgr)
    go dqmgr.diskQueueManager()

    c := make(chan os.Signal, 2)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)

    senders_done := make(chan bool)

    go waitForSenders(senders_wg, senders_done)

    done := false

    for !done {
        select {
            case sig := <-c:
                glog.Infof("main: Received signal: %v", sig)
                done = true

            case <-senders_done:
                glog.Errorf("main: All senders are dead?!? Terminating!")
                done = true
        }
    }

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
    glog.Flush()
}
