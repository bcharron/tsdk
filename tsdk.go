// Copyright 2020 Benjamin Charron. All rights reserved.
// Use of this source code is governed by the GPL 3.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/deathowl/go-metrics-prometheus"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const VERSION string = "0.9.1"

var configuration *Configuration

var live_senders int32 = 0

// Rather than create
func updateCounters(updateFrequencyMS int64, recvq chan *Metric, retryq chan *Metric, qmgr *QueueManager, dqmgr *DiskQueueManager) {
	for {
		QueueSizeMem.Set(float64(qmgr.CountMem()))
		QueueSizeToDisk.Set(float64(dqmgr.CountMem()))
		QueueSizeOnDisk.Set(float64(dqmgr.CountDisk()))
		QueueSizeRecv.Set(float64(len(recvq)))
		QueueSizeRetry.Set(float64(len(retryq)))

		<-time.After(time.Millisecond * time.Duration(updateFrequencyMS))
	}
}

func makeMetric(name string, value interface{}, t uint64) Metric {
	var s string

	switch value.(type) {
	case int32:
		s = strconv.FormatInt(int64(value.(int32)), 10)
	case int:
		s = strconv.FormatInt(int64(value.(int)), 10)
	case int64:
		s = strconv.FormatInt(value.(int64), 10)
	case uint64:
		s = strconv.FormatUint(value.(uint64), 10)
	case float64:
		s = strconv.FormatFloat(value.(float64), 'f', -1, 64)
	}

	n := json.Number(s)

	metric := Metric{Metric: name, Value: n, Timestamp: t, Tags: configuration.Tags}

	return metric
}

func sendStats(ctx context.Context, recvq chan *Metric, sendq chan *Metric, qmgr *QueueManager, dqmgr *DiskQueueManager) {
	for {
		now := uint64(time.Now().Unix())

		metrics := make([]Metric, 0, 10)
		metrics = append(metrics, makeMetric("tsdk.memq.count", qmgr.CountMem(), now))
		// metrics = append(metrics, makeMetric("tsdk.memq.limit", qmgr.max, now))
		metrics = append(metrics, makeMetric("tsdk.metrics.received", ReadCounter(MetricsReceivedHTTP)+ReadCounter(MetricsReceivedTelnet), now))
		metrics = append(metrics, makeMetric("tsdk.metrics.sent", ReadCounter(MetricsSent), now))
		metrics = append(metrics, makeMetric("tsdk.metrics.dropped", ReadCounter(MetricsDroppedRecvqFull), now))
		metrics = append(metrics, makeMetric("tsdk.metrics.dropped_disk_full", ReadCounter(MetricsDroppedDiskqFull), now))
		metrics = append(metrics, makeMetric("tsdk.metrics.invalid", ReadCounter(MetricsDroppedInvalid), now))
		metrics = append(metrics, makeMetric("tsdk.http_errors", ReadCounter(HttpErrors), now))
		metrics = append(metrics, makeMetric("tsdk.recvq.count", len(recvq), now))
		metrics = append(metrics, makeMetric("tsdk.recvq.limit", cap(recvq), now))
		metrics = append(metrics, makeMetric("tsdk.diskq.count", dqmgr.Count(), now))
		metrics = append(metrics, makeMetric("tsdk.diskq.usage", dqmgr.GetDiskUsage(), now))
		metrics = append(metrics, makeMetric("tsdk.senders.count", live_senders, now))
		metrics = append(metrics, makeMetric("tsdk.send.failed", ReadCounter(KafkaSendFailed), now))
		metrics = append(metrics, makeMetric("tsdk.send.serializationError", ReadCounter(MetricsDroppedSerializationErrors), now))

		for idx, _ := range metrics {
			select {
			case sendq <- &metrics[idx]:
				// ok

			case <-ctx.Done():
				// We're done.
				return
			}
		}

		<-time.After(time.Second)
	}
}

func showStats(recvq chan *Metric, memq chan *Metric, retryq chan *Metric, qmgr *QueueManager, dqmgr *DiskQueueManager) {
	var last_received float64
	var last_sent float64

	for {
		<-time.After(time.Second)

		received := ReadCounter(MetricsReceivedHTTP) + ReadCounter(MetricsReceivedTelnet)
		sent := ReadCounter(MetricsSent)

		diff_received := received - last_received
		diff_sent := sent - last_sent

		glog.Infof("stats: recvq: %v/%v  memq: %v/%v  retryq: %v/%v  diskq: %v  ondisk: %v  recv rate: %v/s  send rate: %v/s  idle senders: %v\n", len(recvq), cap(recvq), len(memq), cap(memq), len(retryq), cap(retryq), dqmgr.CountMem(), dqmgr.CountDisk(), diff_received, diff_sent, len(qmgr.requests_queue))

		last_received = received
		last_sent = sent
	}
}

func newKafkaProducer(config *Configuration) sarama.AsyncProducer {
	var err error

	sconfig := sarama.NewConfig()
	sconfig.ClientID = "tsdk"
	sconfig.Producer.Flush.Messages = config.KafkaBatchSize
	sconfig.Producer.Flush.Frequency = time.Millisecond * 1000
	sconfig.Producer.Return.Successes = true
	sconfig.Producer.Retry.Max = 0 // don't take an eternity during shutdown if kafka is down
	sconfig.Producer.RequiredAcks = sarama.WaitForLocal
	sconfig.Producer.Compression = GetCompressionCodec(configuration.CompressionCodec)
	sconfig.MetricRegistry = metrics.DefaultRegistry
	if sconfig.Version, err = sarama.ParseKafkaVersion(config.KafkaVersion); err != nil {
		glog.Fatalf("ParseKafkaVersion: %v", err)
	}

	producer, err := sarama.NewAsyncProducer(configuration.Brokers, sconfig)
	if err != nil {
		glog.Fatalf("Unable to instantiate kafka producer: %v", err)
		return (nil)
	}

	return (producer)
}

func main() {
	config_filename := flag.String("c", "config.json", "Path to the config JSON file")
	show_version := flag.Bool("version", false, "Show version")
	flag.Parse()

	if *show_version {
		fmt.Printf("tsdk version %v\n", VERSION)
		return
	}

	configuration = loadConfig(*config_filename)
	// counters := new(Counters)

	// Prometheus
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	glog.Infof("Brokers: %v", configuration.Brokers)
	glog.Infof("ListenAddr: %v", configuration.ListenAddr)

	// Senders need to be shutdown before queue managers.
	senders_wg := new(sync.WaitGroup)

	glog.Infof("Starting tsdk version %v", VERSION)

	// Metrics to be sent to disk
	disk_enqueue := make(chan *Metric, configuration.DiskQueueBuffer)

	// Metrics received from clients
	recvq := make(chan *Metric, configuration.ReceiveBufferSize)

	// Metrics ready to be sent to kafka
	memq := make(chan *Metric, configuration.MemoryQueueSize)

	// Metrics that failed to be sent
	retryq := make(chan *Metric, 10000)

	qmgr := NewQueueManager(configuration, disk_enqueue, memq, retryq)

	dqmgr := NewDiskQueueManager(configuration, disk_enqueue, memq)

	nb_senders := configuration.Senders

	receiver := NewReceiver(recvq, configuration)

	ctx, cancel := context.WithCancel(context.Background())

	go receiver.server(ctx)
	go showStats(recvq, memq, retryq, qmgr, dqmgr)
	go sendStats(ctx, recvq, memq, qmgr, dqmgr)
	go updateCounters(1000, recvq, retryq, qmgr, dqmgr)

	prometheusClient := prometheusmetrics.NewPrometheusProvider(metrics.DefaultRegistry, "tsdk", "sarama", prometheus.DefaultRegisterer, 1*time.Second)
	go prometheusClient.UpdatePrometheusMetrics()

	producer := newKafkaProducer(configuration)
	defer producer.Close()

	for x := 0; x < nb_senders; x++ {
		name := fmt.Sprintf("sender-%v", x)
		sender := &Sender{
			name:     name,
			memq:     memq,
			retryq:   retryq,
			producer: producer,
		}
		go sender.loop(ctx, senders_wg)
		senders_wg.Add(1)
	}

	queue_managers_wg := &sync.WaitGroup{}

	go qmgr.queueManager(ctx, recvq, queue_managers_wg)
	go dqmgr.diskQueueManager(ctx, queue_managers_wg)

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	done := false

	async_handler_wg := &sync.WaitGroup{}

	go func() {
		async_handler_wg.Add(1)
		defer async_handler_wg.Done()

		for {
			error := <-producer.Errors()
			if error != nil {
				glog.Warningf("async_error_handler: Failed to send to kafka: %v", error.Err)
				// counters.inc_sendFailed(1)
				KafkaSendFailed.Inc()
				retryq <- error.Msg.Metadata.(*Metric)
			} else {
				glog.Infof("async_error_handler: Done.")
				return
			}
		}
	}()

	go func() {
		async_handler_wg.Add(1)
		defer async_handler_wg.Done()

		for {
			msg := <-producer.Successes()
			if msg != nil {
				// counters.inc_sent(1)
				MetricsSent.Inc()
			} else {
				glog.Infof("async_success_handler: Done.")
				return
			}
		}
	}()

	for !done {
		select {
		case sig := <-c:
			glog.Infof("main: Received signal: %v", sig)
			done = true
		}
	}

	glog.Infof("main: Stopping all threads")
	cancel()

	glog.Infof("main: Waiting for kafka producer to shutdown.")
	producer.Close()

	senders_wg.Wait()
	glog.Infof("main: All senders done.")

	glog.Infof("main: Waiting for async handlers.")
	async_handler_wg.Wait()
	glog.Infof("main: Async handlers done.")

	close(retryq)
	// close(memq)

	glog.Infof("main: Waiting for queue managers.")
	queue_managers_wg.Wait()

	glog.Infof("main: All routines finished. Exiting.")
	glog.Flush()
}
