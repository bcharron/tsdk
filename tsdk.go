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
    // "net/http/httputil"
    "strconv"
    "strings"
)

const VERSION string = "0.1"

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

func sender(name string, qmgr chan QMessage, myqueue chan Batch) {
    // buf := make([]Metric, 0, 10)
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    brokers := []string{"localhost:9092"}

    producer, err := sarama.NewSyncProducer(brokers, config)
    if err != nil {
        glog.Fatalf("Unable to instantiate kafka producer: %v", err)
        return
    }

    defer producer.Close()

    for {
        glog.V(3).Infof("[%s] Asking the manager for a batch.\n", name)
        qmgr <- QMessage{"TAKE", name, myqueue}
        batch := <-myqueue

        glog.V(3).Infof("[%s]: Sending %v metrics.\n", name, len(batch.metrics))

        key := sarama.StringEncoder(batch.metrics[0].Metric)
        json_output, err := json.Marshal(batch.metrics)
        if err != nil {
            glog.Errorf("[%s]: Unable to convert to JSON: %v", name, err)
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
                glog.Errorf("Producer failed: %v", err)
                qmgr <- QMessage{"ROLLBACK", name, myqueue}
            } else {
                glog.V(3).Infof("[%s]: Committing %v metrics.\n", name, len(batch.metrics))
                qmgr <- QMessage{"COMMIT", name, myqueue}
            }

            // For testing
            // <-time.After(time.Second)
        }
    }
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

        glog.Infof("stats: recvq: %v/%v  memq: %v/%v  diskq: %v/?  recv rate: %v/s  send rate: %v/s  idle senders: %v\n", len(recvq), cap(recvq), qmgr.count(), qmgr.max, qmgr.disk_count(), diff_received, diff_sent, len(qmgr.requests_queue))

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

    // metrics queued on disk
    diskq *goque.Queue

    // buffer of metrics that will be sent to disk soon
    diskbuf []Metric

    trx map[string]Batch
    max int

    // Number of metrics received
    received int

    // Number of metrics sent
    sent int

    // Number of dropped/discarded metrics
    drops int

    batch_size int

    // Senders waiting for batches
    requests_queue []QMessage
}

func (q *QueueManager) Init(size int, dir string) {
    var err error

    q.batch_size = 1000
    q.requests_queue = make ([]QMessage, 0, 100)
    q.diskbuf = make([]Metric, 0, q.batch_size)
    q.max = size
    q.memq = make([]Metric, 0, size)
    q.trx = make(map[string]Batch)

    q.diskq, err = goque.OpenQueue(dir)
    if err != nil {
        glog.Fatalf("Error trying to open %s: %v", dir, err)
    }
}

func (q *QueueManager) take(n int) Batch {
    var b Batch

    if len(q.memq) < n {
        n = len(q.memq)
    }

   b.metrics = make([]Metric, n)
   copy(b.metrics, q.memq)
   q.memq = q.memq[n:]

   return b
}

func (q *QueueManager) count() int {
    x := len(q.memq)

    for _, batch := range q.trx {
        x += len(batch.metrics)
    }

    return(x)
}

func (q *QueueManager) add_mem(metric Metric) {
    q.memq = append(q.memq, metric)
}

func (q *QueueManager) add(metrics []Metric, force bool) {
    for _, metric := range metrics {
        if q.count() < q.max || force {
            q.add_mem(metric)
            q.received++
        } else {
            // queue to disk
            if !q.queue_to_disk(metric, force) {
                if q.drops % 1000 == 0 {
                    glog.Warningf("Queue is full. Dropped %v messages.", q.drops)
                }

                q.drops++
            }
        }
    }
}

func (q *QueueManager) disk_count() int {
    return(int(q.diskq.Length()) * q.batch_size)
}

func (q *QueueManager) queue_to_disk(metric Metric, force bool) bool {
    q.diskbuf = append(q.diskbuf, metric)

    if len(q.diskbuf) >= q.batch_size {
        return(q.flush_disk(force))
    }

    return(true)
}

func (q *QueueManager) flush_disk(force bool) bool {
    if _, err := q.diskq.EnqueueObject(q.diskbuf); err != nil {
        glog.Error("Failed to queue %v metrics to disk: %v\n", len(q.diskbuf), err)
        return(false)
    }

    q.diskbuf = make([]Metric, 0, q.batch_size)

    return(true)
}

func (q *QueueManager) dequeue_from_disk() bool {
    item, err := q.diskq.Dequeue()
    if err != nil {
        glog.Errorf("Fucked up trying to get data from disk: %v")
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
        q.add_mem(metric)
    }

    return true
}

// Takes incoming metrics from recvq, queue them in memory or disk, and offer
// them to sendq.
func (q *QueueManager) queueManager(size int, recvq chan []Metric, qmgr chan QMessage) {
    var metrics []Metric
    var b Batch

    for {
        // Receive new metrics and/or requests
        select {
            case metrics = <-recvq:
                glog.V(4).Infof("qmgr: Received %v metrics from the recvq", len(metrics))
                q.add(metrics, false)

            /*
            case metrics = <-diskq:
                glog.V(4).Infof("qmgr: Received %v metrics from the recvq", len(metrics))
                q.add(m, false)
            */

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

                case <-time.After(200 * time.Millisecond):
                    // do nothing
        }

        // Send batches if there are any waiting senders
        for len(q.requests_queue) > 0 && len(q.memq) > 0 {
            req := q.requests_queue[0]
            glog.V(3).Infof("qmgr: Sending a batch to %s", req.name)
            b = q.take(q.batch_size)
            req.sender_channel <- b
            q.trx[req.name] = b
            q.requests_queue = q.requests_queue[1:]
        }

        // Dequeue from disk if there is room in the memq
        for len(q.memq) + q.batch_size < q.max && q.diskq.Length() > 0 {
            if ! q.dequeue_from_disk() {
                // something wrong happened. leave it for now.
                break
            }
        }
    }
}

type DiskQueueManager struct {
}

func (q *DiskQueueManager) diskQueueManager() {
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

func (r *Receiver) server() {
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

    for {
        c, err := ln.Accept()
        if err != nil {
            fmt.Println(err)
                continue
        }

        go r.handleConnection(c, fakeListener.connections)
    }
}

func main() {
    qmgr := new(QueueManager)
    qmgr.Init(5000000, "qdir")

    nb_senders := 5

    flag.Parse()

    recvq := make(chan []Metric, 100000)
    // sendq := make(chan Metric, 10000)

    glog.Info("Starting")

    var r Receiver
    r.recvq = recvq

    go r.server()
    go showStats(recvq, qmgr)

    qmgr_chan := make(chan QMessage)

    for x := 0; x < nb_senders; x++ {
        name := fmt.Sprintf("sender-%v", x)
        c := make(chan Batch)
        go sender(name, qmgr_chan, c)
    }

    go qmgr.queueManager(1000, recvq, qmgr_chan)

    for {
       <-time.After(time.Second * 60)
    }

}
