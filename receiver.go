package main;

import(
    "bufio"
    "compress/zlib"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "io/ioutil"
    "github.com/golang/glog"
    // "github.com/prometheus/client_golang/prometheus"
    "compress/gzip"
    "net"
    "net/http"
    "strconv"
    "strings"
    "time"
)

type Receiver struct {
    recvq chan *Metric
    // diskq chan MetricList
    config *Configuration
}

func NewReceiver(recvq chan *Metric, config *Configuration) (*Receiver) {
    r := &Receiver{}

    r.recvq = recvq
    r.config = config

    return(r)
}

func (r *Receiver) HandleHttpPut(w http.ResponseWriter, req *http.Request) {
    var err error
    var ok bool

    w.Header().Set(
        "Content-Type",
        "application/json",
    )

    if req.Method != "POST" {
        w.WriteHeader(http.StatusMethodNotAllowed)
    }

    // XXX: Check for ?details

    glog.V(5).Infof("Query: %v", req.URL.RawQuery)

    var reader io.ReadCloser

    switch req.Header.Get("Content-Encoding") {
        case "gzip":
            reader, err = gzip.NewReader(req.Body)
            if err != nil {
                glog.Warningf("Error reading gzipd body: %v", err)
                w.WriteHeader(http.StatusBadRequest)
                // r.counters.inc_http_errors(1)
                HttpErrors.Inc()

                return
            }

            defer reader.Close()

        case "deflate":
            reader, err = zlib.NewReader(req.Body)
            if err != nil {
                glog.Warningf("Error reading deflated body: %v", err)
                w.WriteHeader(http.StatusBadRequest)
                // r.counters.inc_http_errors(1)
                HttpErrors.Inc()
                return
            }

            defer reader.Close()

        default:
            reader = req.Body
    }

    body, err := ioutil.ReadAll(reader)
    if err != nil {
        glog.Info("httphandler: ERROR Reading Request Body:", err)
        w.WriteHeader(http.StatusBadRequest)
        // r.counters.inc_http_errors(1)
        HttpErrors.Inc()
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
        io.WriteString(w, "That was not JSON.\n")
        glog.Warningf("httphandler: Body received from %v doesn't look like JSON.", req.RemoteAddr)
        w.WriteHeader(http.StatusBadRequest)
        // r.counters.inc_invalid(1)
        MetricsDroppedInvalid.Inc()
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
            // r.counters.inc_invalid(1)
            MetricsDroppedInvalid.Inc()
            return
        }
    }

    glog.V(4).Infof("httphandler: Received %v metrics from %v", len(metrics), req.RemoteAddr)

    errors := make([]string, 0)
    valid_metrics := make(MetricList, 0, len(metrics))
    for idx, _ := range metrics {
        m := &metrics[idx]
        if ok, errmsg := m.isValid(r.config.MaxTags); ok {
            valid_metrics = append(valid_metrics, m)
        } else {
            glog.V(4).Infof("httphandler: Discarding bad metric %v=%v: %v\n", m.Metric, m.Value, errmsg)
            errmsg2 := fmt.Sprintf("%v: %v", m.Metric, errmsg)
            errors = append(errors, errmsg2)
            // r.counters.inc_invalid(1)
            MetricsDroppedInvalid.Inc()
        }
    }

    if len(valid_metrics) > 0 {
        MetricsReceivedHTTP.Add(float64(len(valid_metrics)))

        for _, metric := range(valid_metrics) {
            r.recvq <- metric
        }
    }

    out := new(HTTPOutputMessage)
    out.Success = len(valid_metrics)
    out.Failed = len(errors)
    out.Errors = errors

    json_output, err := json.Marshal(out)
    if err != nil {
        glog.Warningf("Unable to marshal HTTPOutputMessage: %v", err)
        return
    }

    w.WriteHeader(http.StatusOK)
    io.WriteString(w, string(json_output))
}

type HTTPOutputMessage struct {
    Success int `json:"success"`
    Failed int `json:"failed"`
    Errors []string `json:"errors"`
}

func (r *Receiver) HandleHttpVersion(w http.ResponseWriter, req *http.Request) {
    io.WriteString(w, `{"short_revision":"","repo":"","host":"", "version":"2.3.0", "full_revision": "", "repo_status":"MODIFIED", "user":"root", "branch":"", "timestamp":"1526337710"}`)
}

func (r *Receiver) handleTelnet(reader *bufio.Reader, c net.Conn) {
    timeout_duration := time.Duration(r.config.NetworkReadTimeoutMS) * time.Millisecond
    c.SetReadDeadline(time.Now().Add(timeout_duration))

    s := bufio.NewScanner(reader)

    defer c.Close()

    // fmt.Println("TEXT: [" + s.Text() + "]")

    for ok := s.Scan(); ok; ok = s.Scan()  {
        c.SetReadDeadline(time.Time{})

        line := s.Text()
        // glog.V(5).Infof("handleTelnet: Read line [%v]", line)

        fields := strings.Fields(line)

        if len(fields) == 0 {
            continue
        }

        switch(strings.ToUpper(fields[0])) {
            case "PUT": r.handleTelnetPut(c, line, fields)
            case "VERSION": r.handleTelnetVersion(c, line)
            default: c.Write([]byte("ERROR: Command not understood\n"))
                    glog.Infof("bad command \"%s\" from %s: ", fields[0], c.RemoteAddr().String())
        }

        c.SetReadDeadline(time.Now().Add(timeout_duration))
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
        c.Write([]byte("put: Bad PUT line: not enough fields.\n"))
        MetricsDroppedInvalid.Inc()
        return
    }

    if strings.ToUpper(fields[0]) != "PUT" {
        glog.Infof("Garbage from %v:\"%v\"", c.RemoteAddr(), line)
        c.Write([]byte("put: Bad line. Should start with 'put'\n"))
        MetricsDroppedInvalid.Inc()
        return
    }

    m.Metric = fields[1]
    if len(m.Metric) > 256 {
        glog.Infof("Metric name too long from %v: \"%v\"", c.RemoteAddr(), len(m.Metric))
        MetricsDroppedInvalid.Inc()
        c.Write([]byte("put: Metric name is too long\n"))
        return
    }

    m.Timestamp, err = strconv.ParseUint(fields[2], 10, 64)
    if err != nil {
        glog.Infof("Invalid timestamp in PUT from %v: \"%v\"", c.RemoteAddr(), fields[2])
        MetricsDroppedInvalid.Inc()
        c.Write([]byte("put: Invalid timestamp\n"))
        return
    }

    m.Value = json.Number(fields[3])

    tags := fields[4:]
    for x := 0; x < len(tags); x++ {
        t := strings.Split(tags[x], "=")

        if len(t) == 2 {
            m.Tags[t[0]] = t[1]
        } else {
            glog.Infof("Invalid tags from %v: \"%v\"", c.RemoteAddr(), tags[x])
            MetricsDroppedInvalid.Inc()
            c.Write([]byte("put: Invalid tags\n"))
            return
        }
    }

    if ok, err := m.isValid(r.config.MaxTags); ok {
        MetricsReceivedTelnet.Inc()
        r.recvq <- &m
    } else {
        glog.V(3).Infof("Invalid metric from %s", c.RemoteAddr().String())
        MetricsDroppedInvalid.Inc()
        c.Write([]byte(fmt.Sprintf("put: %v\n", err)))
    }
}

func (r *Receiver) handleConnection(c net.Conn, fakeChannel chan net.Conn) {
    duration := time.Duration(r.config.NetworkReadTimeoutMS) * time.Millisecond
    c.SetReadDeadline(time.Now().Add(duration))

    reader := bufio.NewReader(c)
    buf, err := reader.Peek(7)
    if err != nil {
        glog.Infof("handleConnection: %v", err)
        c.Close()
    } else {
        bufstr := string(buf)

        fields := strings.Split(bufstr, " ")
        switch(strings.ToUpper(fields[0])) {
            case "GET":
                glog.V(4).Info("Got an HTTP GET here")
                fc := NewFakeConn(reader, c)
                fakeChannel <- fc
            case "POST":
                glog.V(4).Info("Got an HTTP POST here")
                fc := NewFakeConn(reader, c)
                fakeChannel <- fc
            case "PUT":
                glog.V(4).Info("Got a TELNET PUT here")
                r.handleTelnet(reader, c)
            case "VERSION":
                glog.V(4).Info("Got a TELNET VERSION here")
                r.handleTelnet(reader, c)
            default:
                glog.Infof("Received garbage from %v. Closing connection.", c.RemoteAddr())
                c.Write([]byte("Error: You don't speak any language I know of.\n"))
                c.Close()
        }
    }
}

func (r *Receiver) server(ctx context.Context) {
    ln, err := net.Listen("tcp", r.config.ListenAddr)
    if err != nil {
        panic(err)
    }

    fakeListener := new(FakeListener)
    fakeListener.connections = make(chan net.Conn)
    fakeListener.myaddr = ln.Addr()

    http.HandleFunc("/api/put", r.HandleHttpPut)
    http.HandleFunc("/api/version", r.HandleHttpVersion)

    go http.Serve(fakeListener, nil)

    alive := true

    for alive {
        c, err := ln.Accept()
        if err != nil {
            glog.Warningf("receiver: Failed to accept(): %v", err)
            continue
        }

        select {
            case <-ctx.Done():
                alive = false
                continue
            default:
        }

        go r.handleConnection(c, fakeListener.connections)
    }
}

