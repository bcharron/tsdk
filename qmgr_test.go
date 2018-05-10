package main

import "testing"

func TestQueue(t *testing.T) {
    fake_disk_send := make(chan []Metric, 100)
    fake_disk_from := make(chan []Metric)
    m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}
    qmgr := new(QueueManager)
    qmgr.Init(5000, fake_disk_send, fake_disk_from)
    count := qmgr.CountMem()
    if count != 0 {
        t.Error("Expected 0 metric in memq, got", count)
    }

    qmgr.add_mem(m)

    count = qmgr.CountMem()
    if count != 1 {
        t.Error("Expected 1 metric in memq, got", count)
    }

    b := qmgr.take(1)
    if len(b.metrics) != 1 {
        t.Error("Expected 1 metric in batch, got", len(b.metrics))
    }
}
