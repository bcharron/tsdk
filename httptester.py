#!/usr/bin/env python

import json
import requests
import time


metrics = []
for i in range(1000):
    metrics.append({
        "metric": "fake.metric",
        "value": i,
        "timestamp": 1392392,
        "tags": {
            "host": "myhost.example.com",
            "env": "prod"
        }
    })

bad_metric = {
    "metric": "fake.metric",
    "value": 0.9,
    "timestamp": 1392392,
    "tags": { }
}
metrics.append(bad_metric)

output = json.dumps(metrics)

while True:
    before = time.time()
    r = requests.post("http://localhost:4242/api/put?details", data = output)
    print r.json()
    after = time.time()

    print "Sent {n} metrics in {t:0.3f} seconds".format(n = len(metrics), t = after - before)

