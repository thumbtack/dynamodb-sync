.PHONY: clean
clean:
	rm -f dynamodb-sync

.PHONY: test
test:
	go test -v ./...

.PHONY: build
build: clean
	GOOS=linux GOARCH=amd64 go build
