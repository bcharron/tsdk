package main

import(
    "encoding/json"
    "github.com/golang/glog"
    "github.com/Shopify/sarama"
    "sync"
    "sync/atomic"
)

func getCompressionCodec(name string) sarama.CompressionCodec {
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

func sender(name string, qmgr chan QMessage, myqueue chan Batch, done chan bool, wg *sync.WaitGroup, counters *Counters) {
    wg.Add(1)
    defer wg.Done()

    atomic.AddInt32(&live_senders, 1)
    defer atomic.AddInt32(&live_senders, -1)

    sconfig := sarama.NewConfig()
    sconfig.Producer.Return.Successes = true
    sconfig.Producer.Compression = getCompressionCodec(configuration.CompressionCodec)

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
            counters.inc_serializationError(uint64(len(batch.metrics)));
        } else {
            value := sarama.StringEncoder(json_output)

            kafkaMessage := sarama.ProducerMessage{
                Topic: configuration.Topic,
                Key: key,
                Value: value,
                Headers: []sarama.RecordHeader{},
                Metadata: nil,
            }

            _, _, err = producer.SendMessage(&kafkaMessage)
            if err != nil {
                glog.Errorf("[%s] Producer failed: %v", name, err)
                qmgr <- QMessage{"ROLLBACK", name, myqueue}
                counters.inc_sendFailed(1);
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
