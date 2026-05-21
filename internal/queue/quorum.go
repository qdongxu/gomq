// quorum.go implements a replicated queue using simplified Raft.
package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/qdongxu/gomq/internal/cluster"
)

// QuorumQueue is a queue replicated across multiple nodes.
type QuorumQueue struct {
	name      string
	raft      *cluster.RaftNode
	messages  [][]byte // committed messages
	pending   map[uint64]chan bool // index -> ack channel
	mu        sync.RWMutex
}

// NewQuorumQueue creates a quorum queue with the given Raft node.
func NewQuorumQueue(name string, raft *cluster.RaftNode) *QuorumQueue {
	return &QuorumQueue{
		name:    name,
		raft:    raft,
		pending: make(map[uint64]chan bool),
	}
}

// Publish writes a message through Raft. It returns once the message
// is committed by a majority.
func (q *QuorumQueue) Publish(msg []byte) error {
	idx, err := q.raft.Propose(msg)
	if err != nil {
		return err
	}

	// Wait for commit.
	ch := make(chan bool, 1)
	q.mu.Lock()
	q.pending[idx] = ch
	q.mu.Unlock()

	select {
	case ok := <-ch:
		if !ok {
			return errors.New("replication failed")
		}
		return nil
	case <-time.After(5 * time.Second):
		q.mu.Lock()
		delete(q.pending, idx)
		q.mu.Unlock()
		return errors.New("replication timeout")
	}
}

// OnCommit is called by the leader when an index is committed.
func (q *QuorumQueue) OnCommit(idx uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	committed := q.raft.Committed()
	for uint64(len(q.messages)) < uint64(len(committed)) {
		ent := committed[len(q.messages)]
		q.messages = append(q.messages, ent.Command)
	}

	if ch, ok := q.pending[idx]; ok {
		ch <- true
		delete(q.pending, idx)
	}
}

// Consume returns the next available committed messages.
func (q *QuorumQueue) Consume(count int) [][]byte {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if count > len(q.messages) {
		count = len(q.messages)
	}
	out := make([][]byte, count)
	copy(out, q.messages[:count])
	return out
}

// Ack removes messages up to the given offset.
func (q *QuorumQueue) Ack(offset int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if offset > len(q.messages) {
		offset = len(q.messages)
	}
	q.messages = q.messages[offset:]
}

// Name returns the queue name.
func (q *QuorumQueue) Name() string { return q.name }

// IsLeader reports whether the local node is leader for this queue.
func (q *QuorumQueue) IsLeader() bool { return q.raft.IsLeader() }
