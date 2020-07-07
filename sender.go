package main

import(
    "encoding/json"
    "github.com/golang/glog"
    "github.com/Shopify/sarama"
    "sort"
    "strings"
    "sync"
    "sync/atomic"
    //"time"
)

var send_arrays = false

func (metric *Metric) makeKafkaKey() string {
	fields := make([]string, 0, len(metric.Tags) * 2 + 1)
	fields = append(fields, metric.Metric)

	keys := make([]string, 0, len(metric.Tags) + 1)
	for k := range metric.Tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		fields = append(fields, k, metric.Tags[k])
	}

	ret := strings.Join(fields, ":")

	return(ret)
}

func GetCompressionCodec(name string) sarama.CompressionCodec {
    codecs := map[string]sarama.CompressionCodec {
        "none" : sarama.CompressionNone,
        "gzip" : sarama.CompressionGZIP,
        "snappy" : sarama.CompressionSnappy,
        "lz4" : sarama.CompressionLZ4,
    }

    codec, ok := codecs[name]
    if ! ok {
        glog.Warningf("Failed to lookup compression codec '%v'. Valid choices are: none, gzip, snappy, lz4. Defaulting to none.", name)
        codec = sarama.CompressionNone
    }

    return codec
}


func sender(name string, qmgr chan QMessage, myqueue chan Batch, done chan bool, wg *sync.WaitGroup, counters *Counters, producer sarama.AsyncProducer) {
    wg.Add(1)
    defer wg.Done()

    atomic.AddInt32(&live_senders, 1)
    defer atomic.AddInt32(&live_senders, -1)

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

        qmgr <- QMessage{"COMMIT", name, myqueue}

        glog.V(3).Infof("[%s] Sending %v metrics.", name, len(batch.metrics))

        for _, metric := range batch.metrics {
            json_output, err := json.Marshal(metric)
            if err != nil {
                glog.Errorf("[%s] Unable to convert to JSON: %v", name, err)
                counters.inc_serializationError(uint64(len(batch.metrics)));
            } else {
                key := sarama.StringEncoder(metric.makeKafkaKey())
                value := sarama.StringEncoder(json_output)

                kafkaMessage := &sarama.ProducerMessage{
                    Topic: configuration.Topic,
                    Key: key,
                    Value: value,
                    Headers: []sarama.RecordHeader{},
                    Metadata: metric,
                }

                producer.Input() <- kafkaMessage
            }
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
