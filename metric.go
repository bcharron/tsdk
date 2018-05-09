package main;

import(
    "fmt"
)

type Metric struct {
	Metric string `json:"metric"`
	Timestamp uint64 `json:"timestamp"`
        Value float64 `json:"value"`
	Tags map[string]string `json:"tags"`
}

func (m *Metric) dump() {
    fmt.Printf("%+v\n", m)
}

func (m *Metric) isValid() (bool, string) {
    if len(m.Metric) < 3 {
        return false, "Metric name is too short"
    } else if m.Timestamp == 0 {
        return false, "Timestamp is invalid"
    } else if len(m.Tags) > 8 {
        return false, "Too any tags"
    }

    return true, ""
}

