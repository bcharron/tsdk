package main

import "testing"
import "bufio"
import "github.com/golang/glog"
import "net/http/httptest"
import "net"
import "strings"

type TestData struct {
    config *Configuration
    recvq chan MetricList
    counters *Counters
    r *Receiver
}

func (d *TestData) init () {
    d.config = new(Configuration)
    d.config.loadDefaults()
    d.config.DiskBatchSize = 10
    d.config.DiskQueuePath = "testq"

    d.recvq = make(chan MetricList, 1000)
    d.counters = new(Counters)

    d.r = new(Receiver)
    d.r.Init(d.recvq, d.counters, d.config)
}

func TestHttpReceiver(t *testing.T) {
    data := new(TestData)
    data.init()

    expectedMetrics := []int{2, 1};

    testStrings := []string{`[ { "metric": "fake.metric", "value": 99, "timestamp": 1, "tags": { "host": "myhost.example.com", "env": "prod" }}, { "metric": "fake.metric.2", "value": 99, "timestamp": 2, "tags": { "host": "myhost.example.com", "env": "prod" }} ]`, `{ "metric": "fake.metric", "value": 99, "timestamp": 1, "tags": { "host": "myhost.example.com", "env": "prod" }}`}

    for loop, s := range testStrings {
        response := httptest.NewRecorder()
        reader := strings.NewReader(s)
        req := httptest.NewRequest("POST", "/api/put?details", reader)

        glog.Infof("Sending request to handler")
        data.r.HandleHttpPut(response, req)

        result := response.Result()
        glog.Infof("TestHttpReceiver: TestMulti status code: %v", result.StatusCode)

        if result.StatusCode != 200 {
            t.Errorf("Expected status code 200 but got %v", result.StatusCode)
        }

        metrics := <-data.recvq
        if len(metrics) != expectedMetrics[loop] {
            t.Errorf("Expected to receive 2 metrics, but got %v", len(metrics))
        }

        for idx, m := range metrics {
            if m.Value != "99" {
                t.Errorf("Expected metric value to be 99, but %v", m.Value)
            }

            if m.Timestamp != uint64(idx) + 1 {
                t.Errorf("Expected timestamp %v, but %v", idx, m.Timestamp)
            }

            if m.Tags["host"] != "myhost.example.com" {
                t.Errorf("Expected tag 'host' to be 'myhost.example.com', but got %v", m.Tags["host"])
            }

            if m.Tags["env"] != "prod" {
                t.Errorf("Expected tag 'env' to be 'prod', but got %v", m.Tags["env"])
            }
        }
    }
}

func TestTelnetReceiver(t *testing.T) {
    data := new(TestData)
    data.init()

    me, handler := net.Pipe()

    reader := bufio.NewReader(handler)
    go data.r.handleTelnet(reader, handler)

    glog.Infof("Sending 1 metric via put")
    me.Write([]byte("put fake.metric 1 1234 host=myhost.example.com env=prod\n"))

    metrics := <-data.recvq

    glog.Infof("Got back %v metrics", len(metrics))
    if len(metrics) != 1 {
        t.Errorf("Expected to receive 1 metric, but got %v", len(metrics))
    }

    for idx, m := range metrics {
        if m.Value != "1234" {
            t.Errorf("Expected metric value to be 1234, but got %v", m.Value)
        }

        if m.Timestamp != uint64(idx) + 1 {
            t.Errorf("Expected timestamp %v, but %v", idx, m.Timestamp)
        }

        if m.Tags["host"] != "myhost.example.com" {
            t.Errorf("Expected tag 'host' to be myhost.example.com, but got %v", m.Tags["host"])
        }
    }
}
