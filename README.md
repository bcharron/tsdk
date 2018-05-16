# tsdk

Receives OpenTSDB metrics (via HTTP or Telnet "put") and forwards them (in
JSON) to Kafka. Buffers to memory and disk in case of interruption.

## Usage:

    Usage of ./tsdk:
      -alsologtostderr
            log to standard error as well as files
      -log_backtrace_at value
            when logging hits line file:N, emit a stack trace
      -log_dir string
            If non-empty, write log files in this directory
      -logtostderr
            log to standard error instead of files
      -stderrthreshold value
            logs at or above this threshold go to stderr
      -v value
            log level for V logs
      -vmodule value
            comma-separated list of pattern=N settings for file-filtered logging

## Configuration

Create a file called `config.json` in the current directory:

    {
        "ListenAddr": ":4242",
        "Brokers": ["localhost:9092"],
        "Topic": "tsdb_test",
        "MemoryQueueSize": 75000,
        "ReceiveBuffer": 10000,
        "FlushPeriodMS": 5000,
        "Senders": 5,
        "SendBatchSize": 1000,
        "DiskBatchSize": 1000,
        "Tags": {
            "env": "prod"
        }
    }

- MemoryQueueSize: the maximum number of metrics to keep in memory before
  spilling to disk.
- ReceiverBuffer: list of metrics that have been received but not yet
  queued. However, it contains batches of metrics, not individual metrics. ie,
  if one HTTP POST contains 100 metrics, the ReceiveBuffer will have a single
  entry of 100 metrics. Don't set this value too high.
- Senders: the number of goroutines (~threads) that will send to Kafka.
- SendBatchSize: how many metrics to put in each Kafka message. It is NOT the
  Kafka batch size.
- DiskBatchSize is how many metrics to batch together when writing to disk. If
  it's too low, the disk becomes a bottleneck.
- FlushPeriodMS: If there are less than `SendBatchSize` in the queue, wait
  `FlushPeriodMS` milliseconds before sending the queued metrics to Kafka.

Author: Benjamin Charron <bcharron@pobox.com>
