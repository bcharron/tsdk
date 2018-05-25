package main

import "encoding/json"
import "testing"
import "github.com/golang/glog"


func TestDiskIntegrity(t *testing.T) {
    qsize := 10

    config := new(Configuration)
    config.MemoryQueueSize = qsize
    config.DiskBatchSize = 10
    config.DiskQueuePath = "testq"

    to_disk := make(chan MetricList)
    from_disk := make(chan MetricList)
    done_channel := make(chan bool)
    counters := new(Counters)

    dqmgr := new(DiskQueueManager)
    dqmgr.Init(config, to_disk, from_disk, done_channel, counters)

    go dqmgr.diskQueueManager()

    glog.Infof("Queueing")

    buf := make(MetricList, 0)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:uint64(x), Value:json.Number(x), Tags:nil}
        buf = append(buf, &m)
    }

    to_disk <- buf
    done_channel <- true

    glog.Infof("Waiting for dqmgr to shutdown and persist metrics to disk")

    <-done_channel
    glog.Infof("Persisting done.")

    glog.Infof("Spawning new dqmgr")
    dqmgr2 := new(DiskQueueManager)
    dqmgr2.Init(config, to_disk, from_disk, done_channel, counters)
    go dqmgr2.diskQueueManager()

    glog.Infof("Reloading metrics from disk")
    obuf := <-from_disk
    glog.Infof("obuf len:%v", len(obuf))

    if len(obuf) != qsize {
        t.Errorf("Expected dequeued size to be %v but got %v", qsize, len(obuf))
    }

    for x, metric := range obuf {
        if metric.Timestamp != uint64(x) {
            t.Errorf("Expected metric[%v] timestamp to be %v but got %v", x, x, metric.Timestamp)
        }
    }

    done_channel <- true

    <-done_channel
}
