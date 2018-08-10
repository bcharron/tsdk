package main;

import(
    "fmt"
    "encoding/json"
)

type Metric struct {
	Metric string `json:"metric"`
	Timestamp uint64 `json:"timestamp"`
        Value json.Number `json:"value"`
	Tags map[string]string `json:"tags"`
}

type MetricList []*Metric

func (m *Metric) dump() {
    fmt.Printf("%+v\n", m)
}

// This is the method used by OpenTSDB to check if a number will be converted
// to an int or a float
func looksLikeInteger(s json.Number) bool {
    for _, c := range(s) {
        if c == '.' || c == 'e' || c == 'E' {
            return false
        }
    }

    return true
}

func (m *Metric) isValid(maxTags int) (bool, string) {
    if len(m.Metric) < 3 {
        return false, "Metric name is too short"
    }

    if m.Timestamp == 0 {
        return false, "Timestamp is invalid"
    }

    if looksLikeInteger(m.Value) {
        if _, err := m.Value.Int64(); err != nil {
            return false, fmt.Sprintf("Value is invalid: %v", err)
        }
    } else {
        if _, err := m.Value.Float64(); err != nil {
            return false, fmt.Sprintf("Value is invalid: %v", err)
        }
    }

    if len(m.Tags) > maxTags {
        return false, "Too any tags"
    }

    if len(m.Tags) == 0 {
        return false, "No tags"
    }

    return true, ""
}

