package main

import (
    "io"
    "bufio"
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "github.com/beeker1121/goque"
    "github.com/golang/glog"
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

    for {
        // timeout := false
        // timeout := time.After(time.Second * 5)
        // timed_out := false

        /*
        for len(buf) < cap(buf) && !timed_out {
            select {
                case metric := <-q:
                    glog.Info("sender: Received metric ", metric.Metric, "=", metric.Value)
                    buf = append(buf, metric)
                case <- timeout:
                    fmt.Println("Timeout")
                    timed_out = true
                    break
            }
        }
        */

        glog.V(3).Infof("[%s] Asking the manager for a batch.\n", name)
        msg := QMessage{"TAKE", name, myqueue}
        qmgr <- msg
        batch := <-myqueue

        glog.V(3).Infof("[%s]: Sending %v metrics.\n", name, len(batch.metrics))
        <-time.After(time.Second * 4)

        glog.V(3).Infof("[%s]: Committing %v metrics.\n", name, len(batch.metrics))
        msg = QMessage{"COMMIT", name, myqueue}
        qmgr <- msg
    }
}

func showStats(recvq chan Metric, qmgr *QueueManager) {
    for {
        <-time.After(time.Second)
        glog.Infof("stats: recvq: %v/%v  memq: %v/%v  diskq: ?/?\n", len(recvq), cap(recvq), qmgr.count(), qmgr.max)
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
    diskq *goque.Queue
    trx map[string]Batch
    max int
}

func (q *QueueManager) Init(size int, dir string) {
    var err error

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

func (q *QueueManager) add(metric Metric, force bool) {
    if q.count() < q.max || force {
        q.memq = append(q.memq, metric)
    } else {
        // queue to disk
        glog.Warning("Queue is full. Dropping message.")
    }
}

// Takes incoming metrics from recvq, queue them in memory or disk, and offer
// them to sendq.
func (q *QueueManager) queueManager(size int, recvq chan Metric, qmgr chan QMessage) {
    // memq := list.New()
    var m Metric
    var b Batch
    requests_queue := make ([]QMessage, 0, 100)

    for {
        select {
            case m = <-recvq:
                glog.V(3).Info("qmgr: Received a metric from the recvq")
                q.add(m, false)
            case req := <-qmgr:
                if req.msg == "TAKE" {
                   if len(q.memq) > 0 {
                       glog.V(3).Infof("mgr: TAKE request from %s", req.name)
                       b = q.take(1000)
                       req.sender_channel <- b
                       q.trx[req.name] = b
                    } else {
                        requests_queue = append(requests_queue, req)
                    }
                } else if req.msg == "COMMIT" {
                    glog.V(3).Infof("mgr: COMMIT request from %s", req.name)
                    delete(q.trx, req.name)
                } else if req.msg == "ROLLBACK" {
                    glog.V(3).Infof("mgr: ROLLBACK request from %s", req.name)
                    b := q.trx[req.name]
                    for _, m := range b.metrics {
                        q.add(m, true)
                    }
                    // q.memq = append(q.memq, b.metrics...)
                    delete(q.trx, req.name)
                }
        }

        for len(requests_queue) > 0 && len(q.memq) > 0 {
            req := requests_queue[0]
            glog.V(3).Infof("mgr: Sending a batch to %s", req.name)
            b = q.take(1000)
            req.sender_channel <- b
            q.trx[req.name] = b
            requests_queue = requests_queue[1:]
        }
    }
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
    recvq chan Metric
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

    for x := 0; x < len(metrics); x++ {
        m := metrics[x]
        if ok, errmsg := m.isValid(); ok {
            r.recvq <- m
        } else {
            glog.Infof("httphandler: Discarding bad metric %v=%v: %v\n", m.Metric, m.Value, errmsg)
        }
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
        r.recvq <- m
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
    ln, err := net.Listen("tcp", ":9999")
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
    qmgr.Init(5000, "qdir")

    nb_senders := 5

    flag.Parse()

    recvq := make(chan Metric, 100000)
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
