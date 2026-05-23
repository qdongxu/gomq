// batch_test.go tests PublishBatch and DeliveryBatch.
package server

import (
	"testing"
	"time"
)

func TestPublishBatch_SizeFlush(t *testing.T) {
	var flushed int
	cfg := BatchConfig{MaxSize: 3, MaxWait: time.Hour, MaxBytes: 1 << 20}
	b := NewPublishBatch(cfg, func(msgs []*Message, ex, rk string, chID uint16) error {
		flushed += len(msgs)
		return nil
	})

	for i := 0; i < 5; i++ {
		msg := NewMessage([]byte("x"), Properties{})
		msg.SetRoutingMeta("ex", "rk")
		n, _ := b.Add(msg, "ex", "rk", 1)
		if i == 2 && n != 3 {
			t.Fatalf("expected flush at i=2, got %d", n)
		}
	}

	if flushed != 3 {
		t.Fatalf("expected 3 flushed, got %d", flushed)
	}
	if b.Len() != 2 {
		t.Fatalf("expected 2 remaining, got %d", b.Len())
	}
}

func TestPublishBatch_BytesFlush(t *testing.T) {
	var flushed int
	cfg := BatchConfig{MaxSize: 100, MaxWait: time.Hour, MaxBytes: 10}
	b := NewPublishBatch(cfg, func(msgs []*Message, ex, rk string, chID uint16) error {
		flushed += len(msgs)
		return nil
	})

	msg := NewMessage([]byte("1234567890"), Properties{}) // 10 bytes
	b.Add(msg, "ex", "rk", 1)
	if flushed != 1 {
		t.Fatalf("expected flush by bytes, got %d", flushed)
	}
}

func TestPublishBatch_Flush(t *testing.T) {
	var flushed int
	cfg := BatchConfig{MaxSize: 10, MaxWait: time.Hour, MaxBytes: 1 << 20}
	b := NewPublishBatch(cfg, func(msgs []*Message, ex, rk string, chID uint16) error {
		flushed += len(msgs)
		return nil
	})

	b.Add(NewMessage([]byte("a"), Properties{}), "ex", "rk", 1)
	b.Add(NewMessage([]byte("b"), Properties{}), "ex", "rk", 1)

	n, _ := b.Flush()
	if n != 2 {
		t.Fatalf("expected 2 flushed, got %d", n)
	}
	if b.Len() != 0 {
		t.Fatalf("expected empty batch, got %d", b.Len())
	}
}

func TestPublishBatch_EmptyFlush(t *testing.T) {
	b := NewPublishBatch(DefaultBatchConfig(), func([]*Message, string, string, uint16) error { return nil })
	n, _ := b.Flush()
	if n != 0 {
		t.Fatalf("expected 0 on empty flush, got %d", n)
	}
}

func TestDeliveryBatch_SizeFlush(t *testing.T) {
	var flushed int
	cfg := BatchConfig{MaxSize: 2, MaxWait: time.Hour, MaxBytes: 1 << 20}
	b := NewDeliveryBatch(nil, cfg, func(ch *Channel, items []DeliveryItem) error {
		flushed += len(items)
		return nil
	})

	for i := 0; i < 3; i++ {
		msg := NewMessage([]byte("x"), Properties{})
		n, _ := b.Add(msg, "q", uint64(i))
		if i == 1 && n != 2 {
			t.Fatalf("expected flush at i=1, got %d", n)
		}
	}

	if flushed != 2 {
		t.Fatalf("expected 2 flushed, got %d", flushed)
	}
	if b.Len() != 1 {
		t.Fatalf("expected 1 remaining, got %d", b.Len())
	}
}

func TestDeliveryBatch_Flush(t *testing.T) {
	var flushed int
	cfg := BatchConfig{MaxSize: 10, MaxWait: time.Hour, MaxBytes: 1 << 20}
	b := NewDeliveryBatch(nil, cfg, func(ch *Channel, items []DeliveryItem) error {
		flushed += len(items)
		return nil
	})

	b.Add(NewMessage([]byte("a"), Properties{}), "q", 1)
	b.Add(NewMessage([]byte("b"), Properties{}), "q", 2)

	n, _ := b.Flush()
	if n != 2 {
		t.Fatalf("expected 2 flushed, got %d", n)
	}
}
