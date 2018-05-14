package main

import "testing"
import "github.com/golang/glog"


func TestCommit(t *testing.T) {
    qsize := 10

    var counters *Counters = new(Counters)
    config := new(Configuration)
    config.MemoryQueueSize = qsize

    m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}

    qmgr := new(QueueManager)
    qmgr.Init(config, nil, nil, counters)
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
        t.Errorf("Expected %v metric in batch, got %v", qsize, len(b.metrics))
    }

    if qmgr.CountMem() != qsize {
        t.Errorf("After starting a transaction, expected count:%v but got %v", qsize, qmgr.CountMem())
    }

    qmgr.commit("trx1")

    if qmgr.CountMem() != 0 {
        t.Errorf("After committing a transaction, expected count:0 but got %v", qmgr.CountMem())
    }
}

func TestRollback(t *testing.T) {
    qsize := 10

    counters := new(Counters)
    config := new(Configuration)
    config.MemoryQueueSize = qsize

    m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}

    qmgr := new(QueueManager)
    qmgr.Init(config, nil, nil, counters)
    count := qmgr.CountMem()
    if count != 0 {
        t.Error("Expected 0 metric in memq, got", count)
    }

    for x := 0; x < qsize; x++ {
        qmgr.add_mem(m)
    }

    b := qmgr.take(100, "trx1")
    if len(b.metrics) != qsize {
        t.Errorf("Expected %v metrics in batch, got %v", qsize, len(b.metrics))
    }

    if qmgr.CountMem() != qsize {
        t.Errorf("After starting a transaction, expected count:%v but got %v", qsize, qmgr.CountMem())
    }

    qmgr.rollback("trx1")

    if qmgr.CountMem() != qsize {
        t.Errorf("After rolling back a transaction, expected count:%v but got %v", qsize, qmgr.CountMem())
    }
}

func TestInternalQueue(t *testing.T) {
    qsize := 10

    counters := new(Counters)
    config := new(Configuration)
    config.MemoryQueueSize = qsize

    m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}
    qmgr := new(QueueManager)
    qmgr.Init(config, nil, nil, counters)
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

    counters := new(Counters)
    config := new(Configuration)
    config.MemoryQueueSize = qsize

    fake_disk_send := make(chan []Metric)
    fake_disk_from := make(chan []Metric)

    qmgr := new(QueueManager)
    qmgr.Init(config, fake_disk_send, fake_disk_from, counters)

    metrics := make([]Metric, 0, qsize)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:1, Value:9.0, Tags:nil}
        metrics = append(metrics, m)
    }

    qmgr.add(metrics, false)

    if qmgr.CountMem() != qsize {
        t.Errorf("Expected queue to have count=%v but found count=%v", qsize, qmgr.CountMem())
    }

    if counters.dropped != 0 {
        t.Errorf("Expected queue to have no drops but found %v drops", counters.dropped)
    }

    qmgr.add(metrics, false)
    if counters.dropped != uint64(len(metrics)) {
        t.Errorf("Expected queue to have %v drops but found %v drops", len(metrics), counters.dropped)
    }
}

func TestQueueSendDisk(t *testing.T) {
    qsize := 10

    config := new(Configuration)
    config.MemoryQueueSize = qsize

    fake_disk_send := make(chan []Metric, 100)
    fake_disk_from := make(chan []Metric)

    counters := new(Counters)
    qmgr := new(QueueManager)
    qmgr.Init(config, fake_disk_send, fake_disk_from, counters)

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

    counters := new(Counters)
    config := new(Configuration)
    config.MemoryQueueSize = qsize

    fake_disk_send := make(chan []Metric, 100)
    fake_disk_from := make(chan []Metric)

    qmgr := new(QueueManager)
    qmgr.Init(config, fake_disk_send, fake_disk_from, counters)

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

func TestTake(t *testing.T) {
    qsize := 10

    counters := new(Counters)
    config := new(Configuration)
    config.MemoryQueueSize = qsize

    fake_disk_send := make(chan []Metric, 100)
    fake_disk_from := make(chan []Metric)

    qmgr := new(QueueManager)
    qmgr.Init(config, fake_disk_send, fake_disk_from, counters)

    val := 0
    metrics := make([]Metric, 0, qsize)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:uint64(x), Value:0.0, Tags:nil}
        metrics = append(metrics, m)
        val += x
    }

    qmgr.add(metrics, false)

    m := Metric{Metric:"fake", Timestamp:99, Value:0.0, Tags:nil}
    val += int(m.Timestamp)

    qmgr.add_prio(m)

    if qmgr.CountMem() != qsize + 1 {
        t.Errorf("Expected CountMem to be %v but got %v", qsize + 1, qmgr.CountMem())
    }

    b := qmgr.take(100, "blah")

    glog.Infof("batch len: %v", len(b.metrics))

    if qmgr.CountMem() != qsize + 1 {
        t.Errorf("Expected CountMem to be %v but got %v", qsize + 1, qmgr.CountMem())
    }
}

func BenchmarkMem(b *testing.B) {
    qsize := 10

    counters := new(Counters)
    config := new(Configuration)
    config.MemoryQueueSize = qsize

    // fake_disk_send := make(chan []Metric, 100)
    // fake_disk_from := make(chan []Metric)

    qmgr := new(QueueManager)
    qmgr.Init(config, nil, nil, counters)

    val := 0
    metrics := make([]Metric, 0, qsize)
    for x := 0; x < qsize; x++ {
        m := Metric{Metric:"fake", Timestamp:uint64(x), Value:0.0, Tags:nil}
        metrics = append(metrics, m)
        val += x
    }

    //b.ResetTimer()

    for i := 0; i < b.N; i++ {
        qmgr.add(metrics, false)
        b.StopTimer()
        qmgr.ClearMemQueue()
        b.StartTimer()
    }
}

