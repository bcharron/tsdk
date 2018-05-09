package main;

import(
    "bufio"
    "encoding/json"
    "fmt"
    "io"
    "io/ioutil"
    "github.com/golang/glog"
    "net"
    "net/http"
    "strconv"
    "strings"
)

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
    io.WriteString(w, "Thanks for the metrics.\n")
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

