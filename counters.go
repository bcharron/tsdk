package main

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
dto "github.com/prometheus/client_model/go"
)

var (
        ErrorCounter = promauto.NewCounterVec(prometheus.CounterOpts{
                Namespace: "tsdk",
                Name: "errors",
                Help: "Total number of errors"},
                []string{"type"},
        )

        HttpErrors = ErrorCounter.With(prometheus.Labels{"type": "http"})
        KafkaSendFailed = ErrorCounter.With(prometheus.Labels{"type": "kafka_send"})

        MetricsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
                Namespace: "tsdk",
                Name: "metrics_received",
                Help: "Total number of metrics received"},
                []string{"input"},
        )

        MetricsReceivedHTTP = MetricsReceived.With(prometheus.Labels{"input": "http"})
        MetricsReceivedTelnet = MetricsReceived.With(prometheus.Labels{"input": "telnet"})

        MetricsSent = promauto.NewCounter(prometheus.CounterOpts{
                Namespace: "tsdk",
                Name: "metrics_sent",
                Help: "Total number of metrics sent",
        })

        MetricsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
                Namespace: "tsdk",
                Name: "metrics_dropped",
                Help: "Total number of metrics dropped",
            },
            []string{"cause"},
        )

        MetricsDroppedInvalid = MetricsDropped.With(prometheus.Labels{"cause": "invalid"})
        MetricsDroppedDiskqFull = MetricsDropped.With(prometheus.Labels{"cause": "diskq_full"})
        MetricsDroppedRecvqFull = MetricsDropped.With(prometheus.Labels{"cause": "recvq_full"})
        MetricsDroppedSerializationErrors = MetricsDropped.With(prometheus.Labels{"cause": "serialization_error"})

        QueueSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
                Namespace: "tsdk",
                Name: "queue_size",
                Help: "Current number of items in queue",
            },
            []string{"queue"},
        )

        QueueSizeMem = QueueSize.With(prometheus.Labels{"queue": "mem"})
        QueueSizeDisk = QueueSize.With(prometheus.Labels{"queue": "disk"})
        QueueSizeRecv = QueueSize.With(prometheus.Labels{"queue": "recv"})
        QueueSizeRetry = QueueSize.With(prometheus.Labels{"queue": "retry"})
        QueueSizeOnDisk = QueueSize.With(prometheus.Labels{"queue": "ondisk"})
        QueueSizeToDisk = QueueSize.With(prometheus.Labels{"queue": "todisk"})
)

// https://github.com/prometheus/client_golang/issues/58
func ReadCounter(m prometheus.Counter) float64 {
    pb := &dto.Metric{}
    m.Write(pb)
    return pb.GetCounter().GetValue()
}
