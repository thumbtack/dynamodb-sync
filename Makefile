.PHONY: clean
clean:
	rm dynamodb-sync

.PHONY: test
test:
	go test -v ./...

.PHONY: build
build: clean
	docker run \
        --mount type=bind,source=$(shell pwd),destination=/mount/workdir \
        --workdir /mount/workdir \
        --user "$(shell id -u):$(shell id -g)" \
        --env HOME=/tmp \
        golang:1.14 \
        /bin/bash -c 'export PATH=$$PATH:/tmp/.local/bin && CGO_ENABLED=0 go build'
