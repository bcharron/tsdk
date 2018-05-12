package main;

import(
    "encoding/json"
    "github.com/golang/glog"
    "github.com/Shopify/sarama"
    "sync"
    "sync/atomic"
)

func sender(name string, qmgr chan QMessage, myqueue chan Batch, done chan bool, wg *sync.WaitGroup) {
    sconfig := sarama.NewConfig()
    sconfig.Producer.Return.Successes = true

    wg.Add(1)
    defer wg.Done()

    atomic.AddInt32(&live_senders, 1)
    defer atomic.AddInt32(&live_senders, -1)

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
        } else {
            value := sarama.StringEncoder(json_output)

            kafkaMessage := sarama.ProducerMessage{
                Topic: configuration.Topic,
                Key: key,
                Value: value,
                Headers: []sarama.RecordHeader{},
                Metadata: nil }

            _, _, err = producer.SendMessage(&kafkaMessage)
            if err != nil {
                glog.Errorf("[%s] Producer failed: %v", name, err)
                qmgr <- QMessage{"ROLLBACK", name, myqueue}
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
