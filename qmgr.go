package main;

import(
    "context"
    "github.com/golang/glog"
    "time"
)

type QueueManager struct {
    // Main queue for metrics, in memory
    memq chan *Metric

    // Metrics that failed to be sent and need to be retried
    retryq chan *Metric

    // Number of metrics to send at once
    batch_size int

    // How often to flush queues with less than batch_size metrics
    flush_period_ms time.Duration

    // Senders waiting for batches
    requests_queue []QMessage

    counters *Counters

    dqm *DiskQueueManager

    from_disk chan *Metric
    to_disk chan *Metric
}

func (q *QueueManager) CountMem() int {
    return(len(q.memq))
}

func (q *QueueManager) Init(config *Configuration, to_disk chan *Metric, from_disk chan *Metric, memq chan *Metric, retryq chan *Metric, counters *Counters) {
    q.batch_size = config.SendBatchSize
    q.requests_queue = make ([]QMessage, 0, 100)
    q.memq = memq
    q.retryq = retryq
    q.from_disk = from_disk
    q.to_disk = to_disk
    q.counters = counters
    q.flush_period_ms = time.Duration(config.FlushPeriodMS) * time.Millisecond
}

func (q *QueueManager) add(metrics MetricList) {
    for _, metric := range metrics {
        select {
            case q.memq <- metric:
                // ok
            default:
                q.to_disk <- metric
        }
    }
}

// Takes incoming metrics from recvq, queue them in memory or disk.
func (q *QueueManager) queueManager(ctx context.Context, recvq chan MetricList, done chan bool) {
    var metrics MetricList

    alive := true
    for alive {
        // Receive new metrics and/or requests
        select {
            case metrics = <-recvq:
                glog.V(4).Infof("qmgr: Received %v metrics from the recvq", len(metrics))
                q.add(metrics)
                q.counters.inc_received(uint64(len(metrics)))

            case <-ctx.Done():
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

    glog.Infof("qmgr: Sending %v metrics from retry-queue to disk..", len(q.retryq))
    for metric := range q.retryq {
           q.to_disk <- metric
    }

    glog.Infof("qmgr: Sending up to %v metrics from memory queue to disk..", len(q.memq))
    for metric := range q.memq {
            q.to_disk <- metric
    }

    close(q.to_disk)

    glog.Infof("qmgr: Finished shutdown.")
}
