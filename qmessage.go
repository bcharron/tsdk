package main;

type Batch struct {
    metrics MetricList
}

type QMessage struct {
    msg string
    name string
    sender_channel chan Batch
}
