package main

import(
	"context"
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

type Sender struct {
	producer sarama.AsyncProducer
	memq chan *Metric
	retryq chan *Metric
	name string
	counters *Counters
}

/*
func (s *Sender) Init(name string, memq chan *Metric, retryq chan *Metric, counters *Counters, producer sarama.AsyncProducer) {
	s.name = name
	s.memq = memq
	s.retryq = retryq
	s.counters = counters
	s.producer = producer
}
*/

func (s *Sender) queue(metric *Metric) {
	json_output, err := json.Marshal(metric)
	if err != nil {
		glog.Errorf("[%s] Unable to convert to JSON: %v", s.name, err)
		s.counters.inc_serializationError(1)
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

		s.producer.Input() <- kafkaMessage
	}
}

func (s *Sender) loop(ctx context.Context, wg *sync.WaitGroup) {
    wg.Add(1)
    defer wg.Done()

    atomic.AddInt32(&live_senders, 1)
    defer atomic.AddInt32(&live_senders, -1)

    alive := true

    for alive {
    	// Prioritize the retry queue as long as there's data in it.
    	select {
    	case metric := <-s.retryq:
			s.queue(metric)
			continue

		default:
    	}

    	select {
    	case metric := <-s.retryq:
    			s.queue(metric)

    	case metric := <-s.memq:
    		s.queue(metric)

    	case <-ctx.Done():
    		glog.Infof("[%s] Received 'done'.", s.name)
    		alive = false
    		continue
    	}
    }

    glog.Infof("[%s] Terminating.", s.name)
}
