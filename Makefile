src=tsdk.go metric.go sender.go qmessage.go qmgr.go dqmgr.go fake.go receiver.go

all: tsdk

tsdk: $(src)
	go build $(src)

run: tsdk
	./tsdk -logtostderr=true -v=4

clean:
	rm -f tsdk
