package server

import (
	"testing"
)

// encodeTxSelect builds a Tx.Select method payload (no arguments).
func encodeTxSelect() []byte {
	return []byte{}
}

// encodeTxCommit builds a Tx.Commit method payload (no arguments).
func encodeTxCommit() []byte {
	return []byte{}
}

// encodeTxRollback builds a Tx.Rollback method payload (no arguments).
func encodeTxRollback() []byte {
	return []byte{}
}

// TestTxSelect enables transaction mode and replies with SelectOk.
func TestTxSelect(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	handler, ok := reg.Lookup(90, 10)
	if !ok {
		t.Fatal("handler (90,10) not registered")
	}
	if err := handler(ch, encodeTxSelect()); err != nil {
		t.Fatalf("tx.select: %v", err)
	}
	if !ch.IsTxMode() {
		t.Fatal("expected tx mode to be enabled")
	}
}

// TestTxSelectAfterConfirmFails returns precondition-failed.
func TestTxSelectAfterConfirmFails(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	// enable confirm mode first
	ch.SetConfirmMode()

	handler, _ := reg.Lookup(90, 10)
	err := handler(ch, encodeTxSelect())
	// sendChannelClose returns nil after sending Channel.Close frame;
	// we verify the precondition was caught by checking the returned
	// error or the channel state.
	if err == nil && ch.State() != ChanClosed {
		t.Fatal("expected channel to be closed when tx.select after confirm.select")
	}
}

// TestTxPublishDoesNotRouteImmediately verifies that publish in tx
// mode stages messages instead of routing immediately.
func TestTxPublishDoesNotRouteImmediately(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = srv.QueueManager().Declare("q1", false, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("amq.direct", "q1", "news", nil)

	// enable tx mode
	ch.SetTxMode()

	payload := encodePublish("amq.direct", "news", false, false)
	publishHandler, _ := reg.Lookup(60, 40)
	if err := publishHandler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("queue len = %d, want 0 (staged)", srv.MessageStore().Len("q1"))
	}
}

// TestTxCommitFlushesMessages verifies that Tx.Commit routes staged
// messages and replies with CommitOk.
func TestTxCommitFlushesMessages(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = srv.QueueManager().Declare("q1", false, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("amq.direct", "q1", "news", nil)

	// enable tx mode
	ch.SetTxMode()

	payload := encodePublish("amq.direct", "news", false, false)
	publishHandler, _ := reg.Lookup(60, 40)
	if err := publishHandler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("before commit: queue len = %d, want 0", srv.MessageStore().Len("q1"))
	}

	commitHandler, _ := reg.Lookup(90, 20)
	if err := commitHandler(ch, encodeTxCommit()); err != nil {
		t.Fatalf("tx.commit: %v", err)
	}
	if srv.MessageStore().Len("q1") != 1 {
		t.Fatalf("after commit: queue len = %d, want 1", srv.MessageStore().Len("q1"))
	}
}

// TestTxRollbackDiscardsMessages verifies that Tx.Rollback clears
// staged messages without routing them.
func TestTxRollbackDiscardsMessages(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = srv.QueueManager().Declare("q1", false, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("amq.direct", "q1", "news", nil)

	// enable tx mode
	ch.SetTxMode()

	payload := encodePublish("amq.direct", "news", false, false)
	publishHandler, _ := reg.Lookup(60, 40)
	if err := publishHandler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	rollbackHandler, _ := reg.Lookup(90, 30)
	if err := rollbackHandler(ch, encodeTxRollback()); err != nil {
		t.Fatalf("tx.rollback: %v", err)
	}
	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("after rollback: queue len = %d, want 0", srv.MessageStore().Len("q1"))
	}
}

// TestTxCommitEmpty replies with CommitOk even when no messages
// are staged.
func TestTxCommitEmpty(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	// enable tx mode
	ch.SetTxMode()

	commitHandler, _ := reg.Lookup(90, 20)
	if err := commitHandler(ch, encodeTxCommit()); err != nil {
		t.Fatalf("tx.commit empty: %v", err)
	}
}
