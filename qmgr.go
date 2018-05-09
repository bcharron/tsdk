package main;

import(
    "github.com/golang/glog"
    "time"
)

type QueueManager struct {
    // Main queue for metrics, in memory
    memq []Metric

    // Priority queue for tsdk's own metrics
    prioq []Metric

    trx map[string]Batch

    // Max size of the memory queue
    max int

    // Number of metrics received
    received int

    // Number of metrics sent
    sent int

    // Number of dropped/discarded metrics
    drops int

    // Number of metrics to send at once
    batch_size int

    // Senders waiting for batches
    requests_queue []QMessage

    dqm *DiskQueueManager

    from_disk chan []Metric
    to_disk chan []Metric

    // Used to signal the diskq to shutdown
    diskq_done chan bool
}

func (q *QueueManager) Init(size int, dir string) {
    q.batch_size = configuration.SendBatchSize
    q.requests_queue = make ([]QMessage, 0, 100)
    q.max = size
    q.memq = make([]Metric, 0, size)
    q.prioq = make([]Metric, 0, 1000)
    q.trx = make(map[string]Batch)
    q.dqm = new(DiskQueueManager)
    q.to_disk = make(chan []Metric, 100)
    q.from_disk = make(chan []Metric)
    q.diskq_done = make(chan bool)
    q.dqm.Init(dir, q.to_disk, q.from_disk, q.diskq_done)

    go q.dqm.diskQueueManager()
}

func (q *QueueManager) take(n int) Batch {
    var b Batch

    qsize := len(q.prioq) + len(q.memq) 
    if n > qsize {
        n = qsize
    }

    b.metrics = make([]Metric, n)

    // Take metrics from the priority queue, if any
    if len(q.prioq) > 0 {
        pn := n
        if pn > len(q.prioq) {
            pn = len(q.prioq)
        }

        copy(b.metrics, q.prioq)
        q.prioq = q.prioq[pn:]
        n -= pn
    }

    if n > len(q.memq) {
        n = len(q.memq)
    }

    if n > 0 {
        // glog.Infof("Queue size before: %v", len(q.memq))
        copy(b.metrics, q.memq)
        q.memq = q.memq[n:]
        // glog.Infof("Queue size after : %v", len(q.memq))
    }

    return b
}

func (q *QueueManager) Count() int {
    return(q.CountMem() + q.CountDisk())
}

func (q *QueueManager) CountMem() int {
    x := len(q.memq) + len(q.prioq)

    for _, batch := range q.trx {
        x += len(batch.metrics)
    }

    return(x)
}

func (q *QueueManager) CountDisk() int {
    // return(q.dqm.Count() * q.batch_size)
    return(q.dqm.Count())
}

func (q *QueueManager) Drops() int {
    return(q.drops)
}

func (q *QueueManager) add_mem(metric Metric) {
    q.memq = append(q.memq, metric)
}

func (q *QueueManager) add_prio(metric Metric) {
    q.prioq = append(q.prioq, metric)
}

func (q *QueueManager) add(metrics []Metric, force bool) {
    overflow := make([]Metric, 0, len(metrics))

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

func (q *QueueManager) send_to_disk(metrics []Metric, wait bool) {
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
                    glog.Infof("qmgr: Sent %v metrics to disk", len(metrics))

                default:
                    if q.drops % 1000 == 0 {
                        glog.Warningf("Queue is full. Dropped %v messages since starting.", q.drops)
                    }

                    q.drops++
            }
        }
    }
}

func (q *QueueManager) dispatch() {
    req := q.requests_queue[0]
    b := q.take(q.batch_size)
    glog.V(3).Infof("qmgr: Sending a batch of %v metrics to %s", len(b.metrics), req.name)
    req.sender_channel <- b
    q.trx[req.name] = b
    q.requests_queue = q.requests_queue[1:]
 }

// Takes incoming metrics from recvq, queue them in memory or disk, and offer
// them to sendq.
func (q *QueueManager) queueManager(size int, recvq chan []Metric, prioc chan Metric, qmgr chan QMessage, done chan bool) {
    var metrics []Metric
    var b Batch
    var from_diskq chan []Metric

    timer := time.After(time.Second * 5)

    alive := true
    for alive {
        // Load messages from disk if memq is at 33% or less
        if len(q.memq) <= cap(q.memq) / 3 {
            from_diskq = q.from_disk
        } else {
            from_diskq = nil
        }

        // Receive new metrics and/or requests
        select {
            case metrics = <-recvq:
                glog.V(4).Infof("qmgr: Received %v metrics from the recvq", len(metrics))
                q.add(metrics, false)
                q.received += len(metrics)

                if len(q.requests_queue) > 0 && len(q.memq) + len(q.prioq) >= q.batch_size {
                    q.dispatch()
                }

            case metric := <-prioc:
                glog.V(4).Infof("qmgr: Received metric from the prioq")
                q.add_prio(metric)
                q.received++

            case metrics = <-from_diskq:
                glog.V(4).Infof("qmgr: Received %v metrics from the diskq", len(metrics))
                q.add(metrics, false)

            case req := <-qmgr:
                if req.msg == "TAKE" {
                   if len(q.memq) > q.batch_size {
                       glog.V(3).Infof("qmgr: TAKE request from %s", req.name)
                       b = q.take(q.batch_size)
                       req.sender_channel <- b
                       q.trx[req.name] = b
                    } else {
                        q.requests_queue = append(q.requests_queue, req)
                    }
                } else if req.msg == "COMMIT" {
                    glog.V(3).Infof("qmgr: COMMIT request from %s", req.name)
                    q.sent += len(q.trx[req.name].metrics)
                    delete(q.trx, req.name)
                } else if req.msg == "ROLLBACK" {
                    glog.V(3).Infof("qmgr: ROLLBACK request from %s", req.name)
                    b := q.trx[req.name]
                    q.add(b.metrics, true)
                    // q.memq = append(q.memq, b.metrics...)
                    delete(q.trx, req.name)
                } else {
                    glog.Warningf("qmgr: Unknown message from %s: %v", req.name, req.msg)
                }

            case <-timer:
                glog.V(4).Infof("qmgr: Timer hit. Flushing memq (metrics: %v)", len(q.memq))
                for len(q.requests_queue) > 0 && len(q.memq) + len(q.prioq) > 0 {
                    q.dispatch()
                }
                timer = time.After(time.Second * 5)
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
    }

    glog.Infof("qmgr: Asking disk queue to shutdown.")
    q.diskq_done <- true

    // wait for dqm to finish persisting data
    <-q.diskq_done
}
