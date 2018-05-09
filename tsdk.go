// Copyright 2018 Benjamin Charron. All rights reserved.
// Use of this source code is governed by the GPL 3.0
// license that can be found in the LICENSE file.

package main

import (
    "io"
    "bytes"
    "bufio"
    "encoding/gob"
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "github.com/beeker1121/goque"
    "github.com/golang/glog"
    "github.com/Shopify/sarama"
    "time"
    "net"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    // "net/http/httputil"
    "strconv"
    "strings"
    "sync"
)

const VERSION string = "0.1"

var configuration Configuration

type Configuration struct {
    ListenPort int
    Brokers []string
    ReceiveBuffer int
    MemoryQueueSize int
    Senders int
    BatchSize int
}

type Metric struct {
	Metric string `json:"metric"`
	Timestamp uint64 `json:"timestamp"`
        Value float64 `json:"value"`
	Tags map[string]string `json:"tags"`
}

func (m *Metric) dump() {
    fmt.Printf("%+v\n", m)
}

func (m *Metric) isValid() (bool, string) {
    if len(m.Metric) < 3 {
        return false, "Metric name is too short"
    } else if m.Timestamp == 0 {
        return false, "Timestamp is invalid"
    } else if len(m.Tags) > 8 {
        return false, "Too any tags"
    }

    return true, ""
}

func sender(name string, qmgr chan QMessage, myqueue chan Batch, done chan bool, wg *sync.WaitGroup) {
    sconfig := sarama.NewConfig()
    sconfig.Producer.Return.Successes = true

    wg.Add(1)
    defer wg.Done()

    producer, err := sarama.NewSyncProducer(configuration.Brokers, sconfig)
    if err != nil {
        glog.Errorf("Unable to instantiate kafka producer: %v", err)
        return
    }

    defer producer.Close()

    alive := true

    for alive {
        glog.V(3).Infof("[%s] Asking the manager for a batch.", name)
        select {
            case qmgr <- QMessage{"TAKE", name, myqueue}:
            case <-done:
                glog.Infof("[%s] Received 'done'.", name)
                alive = false
                continue
        }

        var batch Batch

        select {
            case batch = <-myqueue:
                // yay
            case <-done:
                glog.Infof("[%s] Received 'done'.", name)
                alive = false
                continue
        }

        if len(batch.metrics) == 0 {
            glog.Infof("[%s] Received empty batch from qmgr.", name)
            continue
        }

        glog.V(3).Infof("[%s] Sending %v metrics.", name, len(batch.metrics))

        key := sarama.StringEncoder(batch.metrics[0].Metric)
        json_output, err := json.Marshal(batch.metrics)
        if err != nil {
            glog.Errorf("[%s] Unable to convert to JSON: %v", name, err)
            // Discard batch, it won't be better next time.
            qmgr <- QMessage{"COMMIT", name, myqueue}
        } else {
            value := sarama.StringEncoder(json_output)

            kafkaMessage := sarama.ProducerMessage{
                Topic: "tsdb_test",
                Key: key,
                Value: value,
                Headers: []sarama.RecordHeader{},
                Metadata: nil }

            _, _, err = producer.SendMessage(&kafkaMessage)
            if err != nil {
                glog.Errorf("[%s] Producer failed: %v", name, err)
                qmgr <- QMessage{"ROLLBACK", name, myqueue}
            } else {
                glog.V(3).Infof("[%s] Committing %v metrics.\n", name, len(batch.metrics))
                qmgr <- QMessage{"COMMIT", name, myqueue}
            }

            // For testing
            // <-time.After(time.Second)
        }
    }

    // Rollback any pending transaction
    select {
        case <-myqueue:
            glog.Infof("[%s] Rolling back incomplete transaction", name)
            qmgr <- QMessage{"ROLLBACK", name, myqueue}
        default:
            // Nothing to do
    }

    glog.Infof("[%s] Terminating.", name)
}

func showStats(recvq chan []Metric, qmgr *QueueManager) {
    var last_received int
    var last_sent int

    for {
        <-time.After(time.Second)
        received := qmgr.received
        sent := qmgr.sent

        diff_received := received - last_received
        diff_sent := sent - last_sent

        glog.Infof("stats: recvq: %v/%v  memq: %v/%v  diskq: %v/?  recv rate: %v/s  send rate: %v/s  drops: %v  idle senders: %v\n", len(recvq), cap(recvq), qmgr.CountMem(), qmgr.max, qmgr.CountDisk(), diff_received, diff_sent, qmgr.Drops(), len(qmgr.requests_queue))

        last_received = received
        last_sent = sent
    }
}

type Batch struct {
    metrics []Metric
}

type QMessage struct {
    msg string
    name string
    sender_channel chan Batch
}

type QueueManager struct {
    memq []Metric

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
    q.batch_size = 1000
    q.requests_queue = make ([]QMessage, 0, 100)
    q.max = size
    q.memq = make([]Metric, 0, size)
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

    if len(q.memq) < n {
        n = len(q.memq)
    }

    // glog.Infof("Queue size before: %v", len(q.memq))
    b.metrics = make([]Metric, n)
    copy(b.metrics, q.memq)
    q.memq = q.memq[n:]
    // glog.Infof("Queue size after : %v", len(q.memq))

    return b
}

func (q *QueueManager) Count() int {
    return(q.CountMem() + q.CountDisk())
}

func (q *QueueManager) CountMem() int {
    x := len(q.memq)

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

// Takes incoming metrics from recvq, queue them in memory or disk, and offer
// them to sendq.
func (q *QueueManager) queueManager(size int, recvq chan []Metric, qmgr chan QMessage, done chan bool) {
    var metrics []Metric
    var b Batch
    var from_diskq chan []Metric

    // wg.Add(1)
    // defer wg.Done()

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

            case metrics = <-from_diskq:
                glog.V(4).Infof("qmgr: Received %v metrics from the diskq", len(metrics))
                q.add(metrics, false)

            case req := <-qmgr:
                if req.msg == "TAKE" {
                   if len(q.memq) > 0 {
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

            case <-done:
                glog.Infof("qmgr: Received shutdown request.")
                alive = false
                break
        }

        // Send batches if there are any waiting senders
        for len(q.requests_queue) > 0 && len(q.memq) > 0 && alive {
            req := q.requests_queue[0]
            glog.V(3).Infof("qmgr: Sending a batch to %s", req.name)
            // glog.V(3).Infof("qmgr: %v remaining in memq", len(q.memq))
            b = q.take(q.batch_size)
            req.sender_channel <- b
            q.trx[req.name] = b
            q.requests_queue = q.requests_queue[1:]
        }
    }

    q.shutdown()
    done <- true
}

func (q *QueueManager) shutdown() {
    glog.Infof("qmgr: Shutting down.")
    q.send_to_disk(q.memq, true)

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

type DiskQueueManager struct {
    // buffer of metrics that will be sent to disk soon
    diskbuf []Metric
    tosend []Metric

    // Receive from memq, send to memq
    recvq chan []Metric
    sendq chan []Metric

    batch_size int

    // metrics queued on disk
    diskq *goque.Queue

    done chan bool
}

func (q *DiskQueueManager) Init(dir string, recvq chan []Metric, sendq chan []Metric, done chan bool) (bool, error) {
    var err error

    q.recvq = recvq
    q.sendq = sendq
    q.diskbuf = make([]Metric, 0, q.batch_size)
    q.tosend = make([]Metric, 0, q.batch_size)
    q.batch_size = 1000
    q.done = done

    q.diskq, err = goque.OpenQueue(dir)
    if err != nil {
        glog.Fatalf("Error trying to open %s: %v", dir, err)
        return false, err
    }

    glog.Infof("dqm: Found approximately %v metrics on disk.", int(q.diskq.Length()) * q.batch_size)

    return true, nil
}

func (q *DiskQueueManager) Count() int {
    return(int(q.diskq.Length()) * q.batch_size)
}

func (q *DiskQueueManager) queue_to_disk(metrics []Metric, force bool) error {
    for _, metric := range metrics {
        q.diskbuf = append(q.diskbuf, metric)

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

    for _, metric := range metrics {
        // q.add_mem(metric)
        q.tosend = append(q.tosend, metric)
    }

    return true
}

func (q *DiskQueueManager) shutdown() {
    // Persist all queues to disk.

    glog.Infof("dqm: Shutting down.")

    // Flush "diskbuf" (items pending to be flushed to disk)
    glog.Infof("dqm: Flushing diskbuf")
    q.flush_disk(true)

    // Flush "tosend" (items pending to be sent back to memq)
    glog.Infof("dqm: Flushing tosend")
    q.diskbuf = q.tosend

    done := false

    glog.Infof("dqm: Draining memq")
    for !done {
        select {
            case metrics := <-q.recvq:
                q.queue_to_disk(metrics, true)

            default:
                done = true
                // All done
        }
    }

    glog.Infof("dqm: Flushing last metrics to disk")
    q.flush_disk(true)

    glog.Infof("dqm: Done.")

    q.done <- true
}

func (q *DiskQueueManager) diskQueueManager() {
    var sendq chan []Metric
    alive := true

    for alive {
        if q.diskq.Length() > 0 {
            if len(q.tosend) == 0 {
                q.dequeue_from_disk()
            }

            sendq = q.sendq
        } else {
            sendq = nil
        }

        select {
            case metrics := <-q.recvq:
                glog.V(3).Infof("dqm: Received %v metrics", len(metrics))
                q.queue_to_disk(metrics, true)

            case sendq <- q.tosend:
                glog.V(3).Infof("dqm: Sent %v metrics back to memq", len(q.tosend))
                q.tosend = make([]Metric, 0, q.batch_size)

            case <-q.done:
                glog.Infof("dqm: Received shutdown request.")
                alive = false
                break
        }
    }

    q.shutdown()
}

type FakeConn struct {
    reader *bufio.Reader
    realConn net.Conn
}

func (c *FakeConn) init(reader *bufio.Reader, conn net.Conn) {
    c.realConn = conn
    c.reader = reader
}

func (c *FakeConn) Read(b []byte) (n int, err error) { return c.reader.Read(b) }
func (c *FakeConn) Write(b []byte) (n int, err error) { return c.realConn.Write(b) }
func (c *FakeConn) Close() error { return c.realConn.Close() }
func (c *FakeConn) LocalAddr() net.Addr { return c.realConn.LocalAddr()  }
func (c *FakeConn) RemoteAddr() net.Addr { return c.realConn.RemoteAddr()  }
func (c *FakeConn) SetDeadline(t time.Time) error { return c.realConn.SetDeadline(t) }
func (c *FakeConn) SetReadDeadline(t time.Time) error { return c.realConn.SetReadDeadline(t) }
func (c *FakeConn) SetWriteDeadline(t time.Time) error { return c.realConn.SetWriteDeadline(t) }

type FakeListener struct {
    connections chan net.Conn
    myaddr net.Addr
}

func (l *FakeListener) Accept() (net.Conn, error) {
    // fmt.Println("Waiting for connections!")
    conn := <-l.connections
    // fmt.Println("Accept!")
    return conn, nil
}

func (l *FakeListener) Close() error { return nil }

func (l *FakeListener) Addr() net.Addr { return l.myaddr }

type Receiver struct {
    recvq chan []Metric
}

func (r *Receiver) HandleHttpPut(w http.ResponseWriter, req *http.Request) {
    var err error
    var ok bool

    w.Header().Set(
        "Content-Type",
        "text/html",
    )

    // XXX: Check method
    // XXX: Check for ?details

    body, err := ioutil.ReadAll(req.Body)
    if err != nil {
        glog.Info("httphandler: ERROR Reading Request Body:", err)
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    ok = false
    single := true

    // Figure out if we got an array or a single element
    for x := 0; x < len(body); x++ {
        if body[x] == '{' {
            single = true
            ok = true
            break
        } else if body[x] == '[' {
            single = false
            ok = true
            break
        }
    }

    if !ok {
        // c.Write([]byte("Not JSON"))
        glog.Warningf("httphandler: Body received from %v doesn't look like JSON.", req.RemoteAddr)
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    var metrics []Metric

    if single {
        var m Metric
        err = json.Unmarshal(body, &m)
        metrics = append(metrics, m)
    } else {
        err = json.Unmarshal(body, &metrics)
    }

    if err != nil {
        if err != nil {
            glog.Warning("httphandler: Failed to decode JSON: ", err)
            w.WriteHeader(http.StatusBadRequest)
            return
        }
    }

    glog.V(3).Infof("httphandler: Received %v metrics from %v", len(metrics), req.RemoteAddr)

    valid_metrics := make([]Metric, 0, len(metrics))
    for _, m := range metrics {
        if ok, errmsg := m.isValid(); ok {
            valid_metrics = append(valid_metrics, m)
        } else {
            glog.Infof("httphandler: Discarding bad metric %v=%v: %v\n", m.Metric, m.Value, errmsg)
        }
    }

    if len(valid_metrics) > 0 {
        r.recvq <- valid_metrics
    }

    // XXX: Respond with number failed and stuff
    io.WriteString(w, "Thanks for the metrics.")
}

func (r *Receiver) handleTelnet(reader *bufio.Reader, c net.Conn) {
    s := bufio.NewScanner(reader)

    defer c.Close()

    // fmt.Println("TEXT: [" + s.Text() + "]")

    for ok := s.Scan(); ok; ok = s.Scan()  {
        line := s.Text()
        //fmt.Println("handleTelnet: Read line [" + line + "]")

        fields := strings.Fields(line)

        if len(fields) == 0 {
            continue
        }

        switch(strings.ToUpper(fields[0])) {
            case "PUT": r.handleTelnetPut(c, line, fields)
            case "VERSION": r.handleTelnetVersion(c, line)
            default: c.Write([]byte("ERROR: Command not understood\n"))
                    fmt.Println("bad command: ", fields[0])
        }
    }
}

func (r *Receiver) handleTelnetVersion(c net.Conn, line string) {
    s := fmt.Sprintf("tsdk version %v\n", VERSION)
    c.Write([]byte(s))
}

func (r *Receiver) handleTelnetPut(c net.Conn, line string, fields []string) {
    var m Metric
    m.Tags = make(map[string]string)
    var err error

    if len(fields) < 5 {
        c.Write([]byte("ERROR: Bad PUT line: not enough fields.\n"))
        return
    }

    if fields[0] != "put" {
        glog.Infof("Garbage from %v:\"%v\"", c.RemoteAddr(), line)
        c.Write([]byte("ERROR: Bad line. Should start with 'put'\n"))
        return
    }

    m.Metric = fields[1]
    if len(m.Metric) > 256 {
        glog.Infof("Metric name too long from %v: \"%v\"", c.RemoteAddr(), len(m.Metric))
        c.Write([]byte("ERROR: Metric name is too long\n"))
        return
    }

    m.Timestamp, err = strconv.ParseUint(fields[2], 10, 64)
    if err != nil {
        glog.Infof("Invalid timestamp in PUT from %v: \"%v\"", c.RemoteAddr(), fields[2])
        c.Write([]byte("ERROR: Invalid timestamp\n"))
        return
    }

    m.Value, err = strconv.ParseFloat(fields[3], 64)
    if err != nil {
        glog.Infof("Invalid value in PUT from %v: \"%v\"", c.RemoteAddr(), fields[3])
        c.Write([]byte("ERROR: Invalid value. Expected float.\n"))
        return
    }

    tags := fields[4:]
    for x := 0; x < len(tags); x++ {
        t := strings.Split(tags[x], "=")

        if len(t) == 2 {
            m.Tags[t[0]] = t[1]
        } else {
            glog.Infof("Invalid tags from %v: \"%v\"", c.RemoteAddr(), tags[x])
            c.Write([]byte("ERROR: Invalid tags\n"))
            return
        }
    }

    if ok, err := m.isValid(); ok {
        metrics := make([]Metric, 1, 1)
        metrics[0] = m
        // r.recvq <- m
        r.recvq <- metrics
        c.Write([]byte("ok\n"))
    } else {
        c.Write([]byte(err))
    }
}

func (r *Receiver) handleConnection(c net.Conn, fakeChannel chan net.Conn) {
    reader := bufio.NewReader(c)
    buf, err := reader.Peek(7)
    if err != nil {
        fmt.Println(err)
    } else {
        bufstr := string(buf)

        fields := strings.Split(bufstr, " ")
        switch(strings.ToUpper(fields[0])) {
            case "GET":
                glog.V(3).Info("Got an HTTP GET here")
                fc := new(FakeConn)
                fc.init(reader, c)
                fakeChannel <- fc
            case "POST":
                glog.V(3).Info("Got an HTTP POST here")
                fc := new(FakeConn)
                fc.init(reader, c)
                fakeChannel <- fc
            case "PUT":
                glog.V(3).Info("Got a TELNET PUT here")
                r.handleTelnet(reader, c)
            case "VERSION":
                glog.V(3).Info("Got a TELNET VERSION here")
                r.handleTelnet(reader, c)
            default:
                glog.Infof("Received garbage from %v. Closing connection.", c.RemoteAddr())
                c.Write([]byte("Error: You don't speak any language I know of.\n"))
                c.Close()
        }
    }
}

func (r *Receiver) server(done chan bool) {
    ln, err := net.Listen("tcp", ":4242")
    if err != nil {
        fmt.Println(err)
        return
    }

    fakeListener := new(FakeListener)
    fakeListener.connections = make(chan net.Conn)
    fakeListener.myaddr = ln.Addr()

    http.HandleFunc("/api/put", r.HandleHttpPut)

    go http.Serve(fakeListener, nil)

    alive := true

    for alive {
        c, err := ln.Accept()
        if err != nil {
            fmt.Println(err)
                continue
        }

        select {
            case <-done:
                alive = false
                continue
            default:
        }

        go r.handleConnection(c, fakeListener.connections)
    }
}

func loadConfig(filename string) {
    file, err := os.Open(filename);
    if err != nil {
        glog.Fatalf("Unable to open config file: %v", err)
        os.Exit(1)
    }

    defer file.Close()
    decoder := json.NewDecoder(file)

    err = decoder.Decode(&configuration)
    if err != nil {
        glog.Fatal("error:", err)
        os.Exit(1)
    }
}

func main() {
    flag.Parse()

    loadConfig("config.json")

    fmt.Println("Brokers:", configuration.Brokers)
    fmt.Println("ListenPort:", configuration.ListenPort)

    // Senders need to be shutdown before queue managers.
    senders_wg := new(sync.WaitGroup)

    glog.Info("Starting")

    qmgr := new(QueueManager)
    qmgr.Init(configuration.MemoryQueueSize, "qdir")

    nb_senders := 5

    recvq := make(chan []Metric, configuration.ReceiveBuffer)
    // sendq := make(chan Metric, 10000)

    var r Receiver
    r.recvq = recvq

    shutdown_server := make(chan bool, 1)
    go r.server(shutdown_server)
    go showStats(recvq, qmgr)

    qmgr_chan := make(chan QMessage)

    shutdown_channels := make([]chan bool, 0, nb_senders)

    for x := 0; x < nb_senders; x++ {
        name := fmt.Sprintf("sender-%v", x)
        c := make(chan Batch, 1)
        done := make(chan bool)
        shutdown_channels = append(shutdown_channels, done)
        go sender(name, qmgr_chan, c, done, senders_wg)
    }

    shutdown_qmgr := make(chan bool)
    go qmgr.queueManager(1000, recvq, qmgr_chan, shutdown_qmgr)

    c := make(chan os.Signal, 2)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)

    // XXX: Handle senders dying

    sig := <-c
    glog.Infof("main: Received signal: %v", sig)

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

    glog.Infof("Waiting for queue manager.")
    <-shutdown_qmgr

    glog.Infof("All routines finished. Exiting.")
}
