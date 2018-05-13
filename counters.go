package main

import "sync/atomic"

type Counters struct {
    received uint64
    sent uint64
    early_dropped uint64
    dropped uint64
    invalid uint64
    http_errors uint64
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
