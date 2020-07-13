package main;

import(
    "context"
    "github.com/golang/glog"
    "sync"
)

type QueueManager struct {
    // Main queue for metrics, in memory
    memq chan *Metric

    // Metrics that failed to be sent and need to be retried
    retryq chan *Metric

    requests_queue []QMessage

    dqm *DiskQueueManager

    from_disk chan *Metric
    to_disk chan *Metric
}

func (q *QueueManager) CountMem() int {
    return(len(q.memq))
}

func NewQueueManager(config *Configuration, to_disk chan *Metric, memq chan *Metric, retryq chan *Metric) (*QueueManager) {
    q := &QueueManager{}
    q.requests_queue = make ([]QMessage, 0, 100)
    q.memq = memq
    q.retryq = retryq
    q.to_disk = to_disk

    return(q)
}

// Try to add to memq, otherwise send to diskq
func (q *QueueManager) add(metric *Metric) {
    select {
        case q.memq <- metric:
            // ok
        default:
            q.to_disk <- metric
    }
}

// Takes incoming metrics from recvq, queue them in memory or disk.
func (q *QueueManager) queueManager(ctx context.Context, recvq chan *Metric, wg *sync.WaitGroup) {
    wg.Add(1)

    defer wg.Done()

    alive := true
    for alive {
        // Receive new metrics and/or requests
        select {
            // case metric := <-q.from_disk:
            //     glog.V(4).Infof("qmgr: Received metric from disk")
            //     q.add(metric)

            case metric := <-recvq:
                glog.V(4).Infof("qmgr: Received metric from the recvq", metric)
                q.add(metric)

            case <-ctx.Done():
                glog.Infof("qmgr: Received shutdown request.")
                alive = false
                break
        }
    }

    q.shutdown()
}

func (q *QueueManager) shutdown() {
    glog.Infof("qmgr: Shutting down.")

    glog.Infof("qmgr: Sending retry-queue to disk..", len(q.retryq))
    for metric := range q.retryq {
           q.to_disk <- metric
    }

    close(q.memq)

    glog.Infof("qmgr: Sending up to %v metrics from memory queue to disk..", len(q.memq))
    for metric := range q.memq {
            q.to_disk <- metric
    }

    close(q.to_disk)

    glog.Infof("qmgr: Finished shutdown.")
}
