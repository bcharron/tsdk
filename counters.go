package main

import (
    "sync/atomic"
)

type Counters struct {
    received uint64
    sent uint64
    early_dropped uint64    // Dropped because the recvq was full
    dropped uint64          // Dropped because memq was full and disk queue wasn't responding fast-enough
    droppedDiskFull uint64  // Dropped because disk queue was full
    invalid uint64          // Malformed json, invalid metric name, missing tags, etc.
    http_errors uint64
    serializationError uint64   // Unable to convert metric to json in sender()
    sendFailed uint64       // Failed to send batch to kafka (this is a retriable error)
}

func (c *Counters) inc_http_errors(delta uint64) {
    atomic.AddUint64(&c.http_errors, delta)
}

func (c *Counters) inc_received(delta uint64) {
    atomic.AddUint64(&c.received, delta)
}

func (c *Counters) inc_sent(delta uint64) {
    atomic.AddUint64(&c.sent, delta)
}

func (c *Counters) inc_early(delta uint64) {
    atomic.AddUint64(&c.early_dropped, delta)
}

func (c *Counters) inc_dropped(delta uint64) {
    atomic.AddUint64(&c.dropped, delta)
}

func (c *Counters) inc_invalid(delta uint64) {
    atomic.AddUint64(&c.invalid, delta)
}

func (c *Counters) inc_droppedDiskFull(delta uint64) {
    atomic.AddUint64(&c.droppedDiskFull, delta)
}

func (c *Counters) inc_serializationError(delta uint64) {
    atomic.AddUint64(&c.serializationError, delta)
}

func (c *Counters) inc_sendFailed(delta uint64) {
    atomic.AddUint64(&c.sendFailed, delta)
}
