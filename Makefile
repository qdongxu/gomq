# gomq Makefile — development, test, release and container workflows.

.PHONY: help build test bench lint fmt docker release clean version check

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CGO_ENABLED  := 0
BINARY       := gomqd
ENTRY        := cmd/gomqd/main.go
BUILD_DIR    := bin
DIST_DIR     := dist
IMAGE        := qdongxu/gomqd

# Version injection via ldflags.
VERSION     := $(shell git describe --tags --always 2>/dev/null || echo "dev")
BUILD_TIME  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w \
	-X main.version=$(VERSION) \
	-X main.buildTime=$(BUILD_TIME)

# Cross-compile matrix.
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

# ---------------------------------------------------------------------------
# Default target
# ---------------------------------------------------------------------------

.DEFAULT_GOAL := help

help: ## Print all available targets
	@echo "gomq build targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; \
			{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build: ## Compile gomqd binary with version injection
	@echo "Building $(BINARY) v$(VERSION) ..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=$(CGO_ENABLED) \
		go build -ldflags "$(LDFLAGS)" \
		-o $(BUILD_DIR)/$(BINARY) $(ENTRY)
	@echo "→ $(BUILD_DIR)/$(BINARY)"

version: build ## Print embedded version string
	@$(BUILD_DIR)/$(BINARY) -version 2>/dev/null || \
		echo "(version flag not yet implemented in main.go)"

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

test: ## Run unit tests + integration tests
	@echo "Running tests ..."
	CGO_ENABLED=$(CGO_ENABLED) go test -v ./...

bench: ## Run benchmark suite (2s per benchmark)
	@echo "Running benchmarks ..."
	CGO_ENABLED=$(CGO_ENABLED) \
		go test -bench=. -benchtime=2s ./tests/bench/

# ---------------------------------------------------------------------------
# Lint / Format
# ---------------------------------------------------------------------------

lint: ## Run golangci-lint (requires installation)
	@which golangci-lint > /dev/null || \
		(echo "golangci-lint not installed, see https://golangci-lint.run/usage/install/"; exit 1)
	@echo "Running golangci-lint ..."
	@golangci-lint run ./...

fmt: ## Auto-format with gofmt + goimports
	@echo "Formatting ..."
	@gofmt -w .
	@which goimports > /dev/null && goimports -w . || true

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

docker: ## Build Docker image (multi-stage, < 20 MB)
	@echo "Building Docker image $(IMAGE):$(VERSION) ..."
	@docker build -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .
	@echo "→ $(IMAGE):$(VERSION)"

# ---------------------------------------------------------------------------
# Release — cross compilation
# ---------------------------------------------------------------------------

release: ## Cross-compile for linux/darwin × amd64/arm64
	@echo "Building release binaries ..."
	@mkdir -p $(DIST_DIR)
	@for platform in $(PLATFORMS); do \
		GOOS=$$(echo $$platform | cut -d/ -f1); \
		GOARCH=$$(echo $$platform | cut -d/ -f2); \
		output="$(DIST_DIR)/$(BINARY)-$${GOOS}-$${GOARCH}"; \
		echo "  → $$output"; \
		CGO_ENABLED=$(CGO_ENABLED) GOOS=$$GOOS GOARCH=$$GOARCH \
			go build -ldflags "$(LDFLAGS)" -o $$output $(ENTRY); \
	done
	@echo "Release binaries in $(DIST_DIR)/"

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

clean: ## Remove build artefacts
	@echo "Cleaning ..."
	@rm -rf $(BUILD_DIR)/ $(DIST_DIR)/

# ---------------------------------------------------------------------------
# Meta — run everything that is safe in CI
# ---------------------------------------------------------------------------

check: fmt lint test ## Format, lint and test (CI gate)
