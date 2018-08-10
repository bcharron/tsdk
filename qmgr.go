package main;

import(
    "github.com/golang/glog"
    "time"
)

type QueueManager struct {
    // Main queue for metrics, in memory
    memq MetricList

    // Priority queue for tsdk's own metrics
    prioq MetricList

    trx map[string]Batch
    trx_count int

    // Max size of the memory queue
    max int

    // Number of metrics to send at once
    batch_size int

    // How often to flush queues with less than batch_size metrics
    flush_period_ms time.Duration

    // Senders waiting for batches
    requests_queue []QMessage

    counters *Counters

    dqm *DiskQueueManager

    from_disk chan MetricList
    to_disk chan MetricList
}

func (q *QueueManager) Init(config *Configuration, to_disk chan MetricList, from_disk chan MetricList, counters *Counters) {
    q.batch_size = config.SendBatchSize
    q.requests_queue = make ([]QMessage, 0, 100)
    q.max = config.MemoryQueueSize
    q.memq = make(MetricList, 0, q.max)
    q.prioq = make(MetricList, 0, 1000)
    q.trx = make(map[string]Batch)
    q.trx_count = 0
    q.from_disk = from_disk
    q.to_disk = to_disk
    q.counters = counters
    q.flush_period_ms = time.Duration(config.FlushPeriodMS) * time.Millisecond
}

func (q *QueueManager) ClearMemQueue() {
    q.memq = make(MetricList, 0, q.max)
}

// Take up to 'n' metrics from the queue, and starts a transaction.
func (q *QueueManager) take(n int, trx_name string) Batch {
    var b Batch

    qsize := len(q.prioq) + len(q.memq)
    if n > qsize {
        n = qsize
    }

    b.metrics = make(MetricList, 0, n)

    // Take metrics from the priority queue, if any
    if len(q.prioq) > 0 {
        pn := n
        if pn > len(q.prioq) {
            pn = len(q.prioq)
        }

        b.metrics = append(b.metrics, q.prioq[:pn]...)
        q.prioq = q.prioq[pn:]
        n -= pn
    }

    if n > len(q.memq) {
        n = len(q.memq)
    }

    if n > 0 {
        b.metrics = append(b.metrics, q.memq[:n]...)

/*
        // allow GC to collect from underlying array
        for x := 0; x < n; x++ {
            q.memq[x] = nil
        }
*/

        q.memq = q.memq[n:]
    }

    if len(b.metrics) > 0 {
        q.trx[trx_name] = b
        q.updateTrxCount()
    }

    return b
}

// Avoid concurrent read/write access to the map by pre-computing its size
// every time it's updated.
func (q *QueueManager) updateTrxCount() {
    x := 0

    for _, batch := range q.trx {
        x += len(batch.metrics)
    }

    q.trx_count = x
}

func (q *QueueManager) CountMem() int {
    return(len(q.memq) + len(q.prioq) + q.trx_count)
}

func (q *QueueManager) add_mem(metric *Metric) {
    q.memq = append(q.memq, metric)
}

func (q *QueueManager) add_prio(metric *Metric) {
    q.prioq = append(q.prioq, metric)
}

func (q *QueueManager) add(metrics MetricList, force bool) {
    overflow := make(MetricList, 0, len(metrics))

    for _, metric := range metrics {
        if q.CountMem() < q.max || force {
            q.add_mem(metric)
        } else {
            // queue to disk
            overflow = append(overflow, metric)
        }
    }

    q.send_to_disk(overflow, false)
}

func (q *QueueManager) send_to_disk(metrics MetricList, wait bool) {
    if len(metrics) > 0 {
        if wait {
            glog.Infof("qmgr: Trying to flush %v metrics to disk..", len(metrics))
            select {
                case q.to_disk <- metrics:
                    glog.Infof("qmgr: Sent %v metrics to disk", len(metrics))

                case <-time.After(time.Second * 60):
                    glog.Infof("qmgr: Timed out waiting to empty queue. %v metrics were dropped.", len(metrics))
            }
        } else {
            select {
                case q.to_disk <- metrics:
                    glog.V(4).Infof("qmgr: Sent %v metrics to disk", len(metrics))

                default:
                    q.counters.inc_dropped(uint64(len(metrics)))

                    if q.counters.dropped % 1000 == 0 || q.counters.dropped == 1 {
                        glog.Warningf("Queue is full. Dropped %v messages since starting.", q.counters.dropped)
                    }
            }
        }
    }
}

func (q *QueueManager) dispatch() {
    req := q.requests_queue[0]
    b := q.take(q.batch_size, req.name)
    glog.V(3).Infof("qmgr: Sending a batch of %v metrics to %s", len(b.metrics), req.name)
    req.sender_channel <- b
    q.requests_queue = q.requests_queue[1:]
 }

func (q *QueueManager) rollback(name string) {
    b, ok := q.trx[name]
    if ok {
        q.add(b.metrics, true)
        delete(q.trx, name)
    }

    q.updateTrxCount()
}

func (q *QueueManager) commit(name string) {
    b, ok := q.trx[name]
    if ok {
        q.counters.inc_sent(uint64(len(b.metrics)))
        delete(q.trx, name)
    }

    q.updateTrxCount()
}

// Takes incoming metrics from recvq, queue them in memory or disk, and offer
// them to sendq.
func (q *QueueManager) queueManager(recvq chan MetricList, prioc chan *Metric, qmgr chan QMessage, done chan bool) {
    var metrics MetricList
    var b Batch
    var from_diskq chan MetricList

    timer := time.After(q.flush_period_ms)

    alive := true
    for alive {
        // Load messages from disk if memq is at 33% or less
        if len(q.memq) <= q.max / 3 {
            from_diskq = q.from_disk
        } else {
            from_diskq = nil
        }

        // Receive new metrics and/or requests
        select {
            case metrics = <-recvq:
                glog.V(4).Infof("qmgr: Received %v metrics from the recvq", len(metrics))
                q.add(metrics, false)
                q.counters.inc_received(uint64(len(metrics)))

                if len(q.requests_queue) > 0 && len(q.memq) + len(q.prioq) >= q.batch_size {
                    q.dispatch()
                }

            case metric := <-prioc:
                glog.V(4).Infof("qmgr: Received metric from the prioq")
                q.add_prio(metric)
                q.counters.inc_received(1)

            case metrics = <-from_diskq:
                glog.V(4).Infof("qmgr: Received %v metrics from the diskq", len(metrics))
                q.add(metrics, false)

            case req := <-qmgr:
                if req.msg == "TAKE" {
                   if len(q.memq) > q.batch_size {
                       glog.V(3).Infof("qmgr: TAKE request from %s", req.name)
                       b = q.take(q.batch_size, req.name)
                       req.sender_channel <- b
                    } else {
                        q.requests_queue = append(q.requests_queue, req)
                    }
                } else if req.msg == "COMMIT" {
                    glog.V(3).Infof("qmgr: COMMIT request from %s", req.name)
                    q.commit(req.name)
                } else if req.msg == "ROLLBACK" {
                    glog.V(3).Infof("qmgr: ROLLBACK request from %s", req.name)
                    q.rollback(req.name)
                } else {
                    glog.Warningf("qmgr: Unknown message from %s: %v", req.name, req.msg)
                }

            case <-timer:
                glog.V(4).Infof("qmgr: Timer hit. Flushing memq (metrics: %v)", len(q.memq))
                for len(q.requests_queue) > 0 && len(q.memq) + len(q.prioq) > 0 {
                    q.dispatch()
                }
                timer = time.After(q.flush_period_ms)
                glog.V(4).Infof("qmgr: Done flushing timed memq.")

            case <-done:
                glog.Infof("qmgr: Received shutdown request.")
                alive = false
                break
        }
    }

    q.shutdown()
    done <- true
}

func (q *QueueManager) shutdown() {
    glog.Infof("qmgr: Shutting down.")
    q.send_to_disk(q.memq, true)
    q.send_to_disk(q.prioq, true)

    if len(q.trx) > 0 {
        glog.Infof("qmgr: Sending incomplete transactions to disk.")

        for name, batch := range q.trx {
            glog.Infof("qmgr: Rolling back transaction of %v metrics from [%s]", len(batch.metrics), name)
            q.send_to_disk(batch.metrics, true)
        }

        q.updateTrxCount()
    }

    glog.Infof("qmgr: Asking disk queue to shutdown.")
}
