# tsdk

tsdk is a program that receives OpenTSDB metrics (via HTTP or Telnet "put") and
forwards them (in JSON) to Kafka. Buffers to memory and disk in case of
interruption.

Author: Benjamin Charron <bcharron@pobox.com>
