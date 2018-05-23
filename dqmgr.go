package main;

import(
    "bytes"
    "encoding/gob"
    "github.com/beeker1121/goque"
    "github.com/golang/glog"
    "os"
    "path"
    "time"
)

type DiskQueueManager struct {
    // buffer of metrics that will be sent to disk soon
    diskbuf []Metric
    tosend []*Metric

    // Receive from memq, send to memq
    recvq chan []*Metric
    sendq chan []*Metric

    // How much disk space is being used
    diskUsage int64

    batch_size int

    // metrics queued on disk
    diskq *goque.Queue

    counters *Counters

    done chan bool

    config *Configuration
}

func (q *DiskQueueManager) Init(config *Configuration, recvq chan []*Metric, sendq chan []*Metric, done chan bool, counters *Counters) (bool, error) {
    var err error

    q.config = config
    q.recvq = recvq
    q.sendq = sendq
    q.batch_size = q.config.DiskBatchSize
    q.diskbuf = make([]Metric, 0, q.batch_size)
    q.tosend = make([]*Metric, 0, q.batch_size)
    q.done = done
    q.counters = counters

    q.diskq, err = goque.OpenQueue(q.config.DiskQueuePath)
    if err != nil {
        glog.Fatalf("Error trying to open %s: %v", q.config.DiskQueuePath, err)
        return false, err
    }

    glog.Infof("dqmgr: Found approximately %v metrics on disk.", int(q.diskq.Length()) * q.batch_size)

    return true, nil
}

func (q *DiskQueueManager) Count() int {
    return(int(q.diskq.Length()) * q.batch_size + len(q.diskbuf) + len(q.tosend))
}

func (q *DiskQueueManager) GetDiskUsage() int64 {
    return(q.diskUsage)
}

func (q *DiskQueueManager) queue_to_disk(metrics []*Metric, force bool) error {
    for _, metric := range metrics {
        q.diskbuf = append(q.diskbuf, *metric)

        if len(q.diskbuf) >= q.batch_size {
            if err := q.flush_disk(force); err != nil {
                return(err)
            }
        }
    }

    return(nil)
}

func (q *DiskQueueManager) flush_disk(force bool) error {
    if len(q.diskbuf) > 0 {
        glog.V(3).Infof("Flushing %v metrics to disk.", len(q.diskbuf))
        if _, err := q.diskq.EnqueueObject(q.diskbuf); err != nil {
            glog.Error("Failed to queue %v metrics to disk: %v\n", len(q.diskbuf), err)
            return(err)
        }

        q.diskbuf = make([]Metric, 0, q.batch_size)

        glog.V(3).Infof("Number of objects on disk: %v", q.diskq.Length())
    }

    return(nil)
}

func (q *DiskQueueManager) dequeue_from_disk() bool {
    item, err := q.diskq.Dequeue()
    if err != nil {
        glog.Errorf("Fucked up trying to get data from disk: %v", err)
        return false
    }

    // fmt.Printf("item: %+v\n", item)
    buf := bytes.NewBuffer(item.Value)
    dec := gob.NewDecoder(buf)

    var metrics []Metric
    err = dec.Decode(&metrics)
    if err != nil {
        glog.Fatalf("Error decoding metric from disk: %v", err)
        return false
    }

    glog.V(4).Infof("Decoded %v metrics from disk", len(metrics))

    for idx, _ := range metrics {
        // q.add_mem(metric)
        q.tosend = append(q.tosend, &metrics[idx])
    }

    return true
}

func (q *DiskQueueManager) shutdown() {
    // Persist all queues to disk.

    glog.Infof("dqmgr: Shutting down.")

    // Flush "diskbuf" (items pending to be flushed to disk)
    glog.Infof("dqmgr: Flushing diskbuf")
    q.flush_disk(true)

    // Flush "tosend" (items pending to be sent back to memq)
    glog.Infof("dqmgr: Flushing tosend")
    for _, metric := range q.tosend {
        q.diskbuf = append(q.diskbuf, *metric)
    }
    q.flush_disk(true)

    done := false

    glog.Infof("dqmgr: Draining memq")
    for !done {
        select {
            case metrics := <-q.recvq:
                q.queue_to_disk(metrics, true)

            default:
                done = true
                // All done
        }
    }

    glog.Infof("dqmgr: Flushing last metrics to disk")
    q.flush_disk(true)

    q.diskq.Close()
    glog.Infof("dqmgr: Done.")

    q.done <- true
}

func (q *DiskQueueManager) diskQueueManager() {
    var sendq chan []*Metric
    alive := true
    acceptingMetrics := true
    duChannel := make(chan int64)

    go updateDiskUsage(q.config.DiskQueuePath, duChannel)

    for alive {
        if q.diskq.Length() > 0 || len(q.tosend) > 0 {
            if len(q.tosend) == 0 {
                q.dequeue_from_disk()
            }

            sendq = q.sendq
        } else {
            sendq = nil
        }

        select {
            case metrics := <-q.recvq:
                glog.V(3).Infof("dqmgr: Received %v metrics", len(metrics))
                if acceptingMetrics {
                    q.queue_to_disk(metrics, true)
                } else {
                    glog.Warningf("dqmgr: Dropping metrics")
                    q.counters.inc_droppedDiskFull(uint64(len(metrics)))
                }

            case sendq <- q.tosend:
                glog.V(3).Infof("dqmgr: Sent %v metrics back to memq", len(q.tosend))
                q.tosend = make([]*Metric, 0, q.batch_size)

            case queueDiskUsage := <-duChannel:
                q.diskUsage = queueDiskUsage
                glog.V(3).Infof("dqmgr: Disk usage: %v bytes", q.diskUsage)
                if queueDiskUsage < q.config.DiskMaxSize {
                    acceptingMetrics = true
                } else {
                    acceptingMetrics = false
                }

            case <-q.done:
                glog.Infof("dqmgr: Received shutdown request.")
                alive = false
                break
        }
    }

    q.shutdown()
}

func updateDiskUsage(dirPath string, c chan int64) {
    for {
        size := du(dirPath)
        c <- size

        // Check every 5 seconds
        <-time.After(time.Second * 5)
    }
}

// Recursively returns the number of bytes used by files in a directory.
// The output may probably be different from the OS' own `du` because we don't
// round to the nearest block size
func du(dirPath string) int64 {
    var total int64 = 0

    dir, err := os.Open(dirPath)
    if err != nil {
        glog.Errorf("Unable to open %s to count disk space: %v", dirPath, err)
        return(-1)
    }

    defer dir.Close()

    files, err := dir.Readdir(0)
    if err != nil {
        glog.Errorf("Unable to open read files in %s: %v", dirPath, err)
        return(-1)
    }

    for _, file := range files {
        if file.IsDir() {
            subPath := path.Join(dirPath, file.Name())
            total += du(subPath)
        } else {
            total += file.Size()
        }
    }

    return total
}
