package main

import "encoding/json"
import "testing"

func TestIntegerChecking(t *testing.T) {
    testValues := []json.Number{"123456", "2.0", "10e-19", "9329329329239"};
    expectedResults := []bool{true, false, false, true};

    for i, s := range(testValues) {
        conv := looksLikeInteger(s)
        if conv != expectedResults[i] {
            t.Errorf("Expected looksLikeInteger(\"%v\") to return %v, but got %v", s, expectedResults[i], conv)
        }
    }
}

func TestMetricValidation(t *testing.T) {
    validTags := map[string]string{
        "test": "ok",
    }

    emptyTags := map[string]string{}

    maxTags := map[string]string{
        "a1": "1",
        "a2": "2",
        "a3": "3",
        "a4": "4",
        "a5": "5",
        "a6": "6",
        "a7": "7",
        "a8": "8",
    }

    tooManyTags := map[string]string{
        "a1": "1",
        "a2": "2",
        "a3": "3",
        "a4": "4",
        "a5": "5",
        "a6": "6",
        "a7": "7",
        "a8": "8",
        "a9": "9",
    }

    metrics := make([]Metric, 0, 10)
    expected := make([]bool, 0, 10)

    // Valid typical metric
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"1024", Tags:validTags})
    expected = append(expected, true)

    // Invalid value (empty)
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"", Tags:validTags})
    expected = append(expected, false)

    // 64-bit max uint
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"18446744073709551616", Tags:validTags})
    expected = append(expected, false)

    // 64-bit max signed int
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"9223372036854775807", Tags:validTags})
    expected = append(expected, true)

    // 64-bit max int + 1
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"9223372036854775808", Tags:validTags})
    expected = append(expected, false)

    // 64-bit min signed int
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"-9223372036854775808", Tags:validTags})
    expected = append(expected, true)

    // 64-bit min signed int - 1
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"-9223372036854775809", Tags:validTags})
    expected = append(expected, false)

    // Valid Float64
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"2.69", Tags:validTags})
    expected = append(expected, true)

    // Valid Float64, exponent notation
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"2.9e10", Tags:validTags})
    expected = append(expected, true)

    // Invalid number
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"hello", Tags:validTags})
    expected = append(expected, false)

    // Invalid timestamp
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:0, Value:"1024", Tags:validTags})
    expected = append(expected, false)

    // No tags
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"1024", Tags:emptyTags})
    expected = append(expected, false)

    // Maximum tags
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"1024", Tags:maxTags})
    expected = append(expected, true)

    // Too many tags
    metrics = append(metrics, Metric{Metric:"test.metric", Timestamp:1527723488, Value:"1024", Tags:tooManyTags})
    expected = append(expected, false)

    for i, m := range(metrics) {
        result, err := m.isValid()
        if result != expected[i] {
            t.Errorf("Expected isValid(%v):%v but got %v. Err: %v", m, expected[i], result, err)
        }
    }
}
