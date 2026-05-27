# Multi-stage build for a minimal gomq server image.
# Stage 1 — compile a static binary.
# Stage 2 — copy the binary into a distroless base image.

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Install git for ldflags version injection.
RUN apk add --no-cache git

# Cache Go module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a static binary.
COPY . .

ARG VERSION=dev
ARG BUILD_TIME

RUN CGO_ENABLED=0 \
    go build \
    -ldflags "-s -w \
        -X main.version=${VERSION} \
        -X main.buildTime=${BUILD_TIME}" \
    -o gomqd \
    cmd/gomqd/main.go

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot

# Non-root user already provided by distroless.
USER nonroot:nonroot

WORKDIR /gomq

# Copy the static binary.
COPY --from=builder /build/gomqd /gomqd/

# Copy default configuration.
COPY --from=builder /build/configs/gomq.default.toml /etc/gomq/

# Expose standard ports.
# 5672  — AMQP
# 5671  — AMQP over TLS
# 15672 — Web management UI
# 15692 — Prometheus metrics
EXPOSE 5672 5671 15672 15692

# Use the default config path baked into the image.
ENTRYPOINT ["/gomq/gomqd"]
CMD ["-config", "/etc/gomq/gomq.default.toml"]
