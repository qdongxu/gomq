.PHONY: build test lint fmt clean

CGO_ENABLED := 0
LDFLAGS := -s -w
BINARY := bin/gomqd
ENTRY := cmd/gomqd/main.go

build:
	@CGO_ENABLED=$(CGO_ENABLED) go build -ldflags "$(LDFLAGS)" -o $(BINARY) $(ENTRY)

test:
	@CGO_ENABLED=$(CGO_ENABLED) go test -v ./...

lint:
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed, see https://golangci-lint.run/usage/install/"; exit 1)
	@golangci-lint run ./...

fmt:
	@gofmt -w .
	@which goimports > /dev/null && goimports -w . || true

clean:
	@rm -rf bin/
