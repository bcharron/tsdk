# tsdk

Receives OpenTSDB metrics (via HTTP or Telnet "put") and forwards them (in
JSON) to Kafka. Buffers to memory and disk in case of interruption.

## Usage:

    Usage of ./tsdk:
      -alsologtostderr
            log to standard error as well as files
      -c string
            Path to the config JSON file (default "config.json")
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
      -version
            Show version
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
        "DiskQueueBuffer": 10000,
        "DiskMaxSize": 10000000,
        "KafkaVersion" "2.3.0",
        "Tags": {
            "env": "prod"
        }
    }

- MemoryQueueSize: the maximum number of metrics to keep in memory before
  spilling to disk.
- ReceiverBufferSize: metrics that have been received but not yet
  queued.
- Senders: the number of goroutines (~threads) that will send to Kafka.
- SendBatchSize: how many metrics to put in each Kafka message. It is NOT the
  Kafka batch size.
- DiskBatchSize: how many metrics to batch together when writing to disk. If
  it's too low, the disk becomes a bottleneck.
- DiskMaxSize: how many bytes can be queued to disk. Defaults to 10000000
  (10M). If this limit is exceeded and the memory queue is full then new
  metrics are dropped.
- DiskQueueBuffer: size of the temporary queue between the memory queue manager
  and the disk queue manager. If you tend to receive a lot of metrics via the
  Telnet interface or in very small batches via HTTP, make this higher.
  Defaults to 10000.
- FlushPeriodMS: If there are less than `SendBatchSize` in the queue, wait
  `FlushPeriodMS` milliseconds before sending the queued metrics to Kafka.
- CompressionCodec: compression used when sending to kafka. Valid options are
  "gzip", "lzo", "snappy" and "none".
- KafkaVersion: The version of kafka brokers you are connecting to.
- KafkaBatchSize: Number of messages to batch together when sending to kafka

## License

This software is licensed under the GPL v3. See LICENSE for more details.

tsdk Copyright (C) 2020 Benjamin Charron <bcharron@pobox.com>
This program comes with ABSOLUTELY NO WARRANTY.
This is free software, and you are welcome to redistribute it under certain
conditions.
