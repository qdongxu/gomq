package server

import (
	"io"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// readFrameBg reads a frame without failing the test (for bg goroutine).
func readFrameBg(r io.Reader) (*amqp091.Frame, error) {
	dec := amqp091.NewDecoder(r)
	return dec.ReadFrame(amqp091.FrameMaxDefault)
}
