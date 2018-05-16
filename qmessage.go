package main;

type Batch struct {
    metrics []*Metric
}

type QMessage struct {
    msg string
    name string
    sender_channel chan Batch
}
