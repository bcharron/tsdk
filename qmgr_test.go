package main

import "testing"
import "github.com/golang/glog"

func TestCommit(t *testing.T) {
    qsize := 10

    m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}

    qmgr := new(QueueManager)
    qmgr.Init(qsize, nil, nil)
    count := qmgr.CountMem()
    if count != 0 {
        t.Error("Expected 0 metric in memq, got", count)
    }

    for x := 0; x < qsize; x++ {
        qmgr.add_mem(m)
    }

    // Test commit
    b := qmgr.take(100, "trx1")
    if len(b.metrics) != qsize {
        t.Error("Expected %v metric in batch, got", qsize, len(b.metrics))
    }

    if qmgr.CountMem() != qsize {
        t.Error("After starting a transaction, expected count:%v but got %v", qsize, qmgr.CountMem())
    }

    qmgr.commit("trx1")

    if qmgr.CountMem() != 0 {
        t.Error("After committing a transaction, expected count:0 but got %v", qmgr.CountMem())
    }
}

func TestRollback(t *testing.T) {
    qsize := 10

    m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}

    qmgr := new(QueueManager)
    qmgr.Init(qsize, nil, nil)
    count := qmgr.CountMem()
    if count != 0 {
        t.Error("Expected 0 metric in memq, got", count)
    }

    for x := 0; x < qsize; x++ {
        qmgr.add_mem(m)
    }

    b := qmgr.take(100, "trx1")
    if len(b.metrics) != qsize {
        t.Error("Expected %v metrics in batch, got", qsize, len(b.metrics))
    }

    if qmgr.CountMem() != qsize {
        t.Error("After starting a transaction, expected count:%v but got %v", qsize, qmgr.CountMem())
    }

    qmgr.rollback("trx1")

    if qmgr.CountMem() != qsize {
        t.Error("After rolling back a transaction, expected count:%v but got %v", qsize, qmgr.CountMem())
    }
}

func TestInternalQueue(t *testing.T) {
    qsize := 10

    m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}
    qmgr := new(QueueManager)
    qmgr.Init(qsize, nil, nil)
    count := qmgr.CountMem()
    if count != 0 {
        t.Error("Expected 0 metric in memq, got", count)
    }

    qmgr.add_mem(m)

    count = qmgr.CountMem()
    if count != 1 {
        t.Error("Expected 1 metric in memq, got", count)
    }

    qmgr.take(10, "trx1")
    qmgr.commit("trx1")

    metrics := make([]Metric, 0, qsize)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}
        metrics = append(metrics, m)
    }

    qmgr.add(metrics, false)
    if qmgr.CountMem() != qsize {
        t.Errorf("Expected %v metrics in memq but got %v", qsize, qmgr.CountMem())
    }
}

func TestQueueDiscard(t *testing.T) {
    qsize := 10

    fake_disk_send := make(chan []Metric)
    fake_disk_from := make(chan []Metric)

    qmgr := new(QueueManager)
    qmgr.Init(qsize, fake_disk_send, fake_disk_from)

    metrics := make([]Metric, 0, qsize)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}
        metrics = append(metrics, m)
    }

    qmgr.add(metrics, false)

    if qmgr.CountMem() != qsize {
        t.Errorf("Expected queue to have count=%v but found count=%v", qsize, qmgr.CountMem())
    }

    if qmgr.CountDrops() != 0 {
        t.Errorf("Expected queue to have no drops but found %v drops", qmgr.CountDrops())
    }

    qmgr.add(metrics, false)
    if qmgr.CountDrops() != len(metrics) {
        t.Errorf("Expected queue to have %v drops but found %v drops", len(metrics), qmgr.CountDrops())
    }
}

func TestQueueSendDisk(t *testing.T) {
    qsize := 10

    fake_disk_send := make(chan []Metric, 100)
    fake_disk_from := make(chan []Metric)

    qmgr := new(QueueManager)
    qmgr.Init(qsize, fake_disk_send, fake_disk_from)

    metrics := make([]Metric, 0, qsize)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}
        metrics = append(metrics, m)
    }

    // fill-up the queue
    qmgr.add(metrics, false)

    // overflow the queue
    qmgr.add(metrics, false)

    if len(fake_disk_send) != 1 {
        t.Errorf("Expected 1 item in the disk queue. Got %v", len(fake_disk_send))
    }

    disk_metrics := <-fake_disk_send

    if len(disk_metrics) != qsize {
        t.Errorf("Expected %v items in the disk queue, but got %v", qsize, len(disk_metrics))
    }
}

func TestShutdown(t *testing.T) {
    qsize := 10

    fake_disk_send := make(chan []Metric, 100)
    fake_disk_from := make(chan []Metric)

    qmgr := new(QueueManager)
    qmgr.Init(qsize, fake_disk_send, fake_disk_from)

    val := 0
    metrics := make([]Metric, 0, qsize)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:uint64(x), Value:0.0, Tags:nil}
        metrics = append(metrics, m)
        val += x
    }

    // 1
    qmgr.add(metrics, false)

    // 2
    qmgr.take(2, "trx1")

    // 3
    qmgr.take(2, "trx2")

    m := Metric{Metric:"fake", Timestamp:99, Value:0.0, Tags:nil}
    val += int(m.Timestamp)

    // 4
    qmgr.add_prio(m)

    qmgr.shutdown()

    if len(fake_disk_send) != 4 {
        t.Errorf("Expected 4 items in the send_queue, but found %v instead", len(fake_disk_send))
    }

    n := 0
    done := false
    received := make([]Metric, 0, qsize + 1)

    for !done {
        select {
            case r := <-fake_disk_send:
                n += len(r)
                received = append(received, r...)

            default:
                done = true
        }
    }

    if n != qsize + 1 {
        t.Errorf("Expected %v metrics but received %v", qsize + 1, n)
    }

    sum := uint64(0)
    for i, m := range received {
        sum += m.Timestamp
        glog.Infof("m[%v]: %v", i, m.Timestamp)
    }

    if sum != uint64(val) {
        t.Errorf("Expected a sum of %v but got %v", val, sum)
    }
}

