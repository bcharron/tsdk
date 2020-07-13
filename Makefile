src=tsdk.go metric.go sender.go qmessage.go qmgr.go dqmgr.go fake.go receiver.go configuration.go counters.go

all: tsdk

tsdk: $(src)
	go build $(src)

run: tsdk
	./tsdk -logtostderr=true -v=2 -c config.json

vrun3: tsdk
	./tsdk -logtostderr=true -v=3

vrun: tsdk
	./tsdk -logtostderr=true -v=5

clean:
	rm -f tsdk

test:
	go test

vtest:
	go test -v -args -logtostderr=true -v=5

bench:
	go test -bench=. -benchmem
