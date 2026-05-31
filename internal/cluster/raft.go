// raft.go provides a simplified Raft consensus node for quorum queue
// replication.
package cluster

import (
	"fmt"
	"sync"
	"time"
)

// RaftState represents the role of a node.
type RaftState int

const (
	StateFollower RaftState = iota
	StateCandidate
	StateLeader
)

func (s RaftState) String() string {
	switch s {
	case StateLeader:
		return "leader"
	case StateCandidate:
		return "candidate"
	}
	return "follower"
}

// LogEntry is a single command in the replicated log.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// RaftNode is a simplified Raft consensus node.
type RaftNode struct {
	nodeID          string
	state           RaftState
	currentTerm     uint64
	votedFor        string
	log             []LogEntry
	commitIndex     uint64
	lastApplied     uint64
	mu              sync.RWMutex
	electionTimer   *time.Timer
	peers           []string
	transport       RaftTransport
	electionResetCh chan struct{}
}

// NewRaftNode creates a new Raft node.
func NewRaftNode(nodeID string, peers []string) *RaftNode {
	r := &RaftNode{
		nodeID:          nodeID,
		state:           StateFollower,
		peers:           peers,
		log:             make([]LogEntry, 0),
		electionResetCh: make(chan struct{}, 1),
	}
	r.resetElectionTimer()
	return r
}

// State returns the current Raft state.
func (r *RaftNode) State() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// IsLeader reports whether this node is the leader.
func (r *RaftNode) IsLeader() bool {
	return r.State() == StateLeader
}

// Term returns the current term.
func (r *RaftNode) Term() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// Propose appends a command to the leader's log.
func (r *RaftNode) Propose(cmd []byte) (uint64, error) {
	r.mu.Lock()
	if r.state != StateLeader {
		r.mu.Unlock()
		return 0, ErrNotLeader
	}
	idx := uint64(len(r.log)) + 1
	r.log = append(r.log, LogEntry{
		Index:   idx,
		Term:    r.currentTerm,
		Command: cmd,
	})
	r.mu.Unlock()
	return idx, nil
}

// AppendEntries handles incoming append from a leader.
func (r *RaftNode) AppendEntries(
	term, leaderCommit, prevLogIndex, prevLogTerm uint64,
	entries []LogEntry,
) (success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term < r.currentTerm {
		return false
	}

	if term > r.currentTerm {
		r.currentTerm = term
		r.state = StateFollower
		r.votedFor = ""
	}

	r.signalElectionReset()

	// Check prevLogIndex and prevLogTerm.
	if prevLogIndex > 0 {
		if prevLogIndex > uint64(len(r.log)) {
			return false
		}
		if r.log[prevLogIndex-1].Term != prevLogTerm {
			return false
		}
	}

	// Append new entries.
	for i, ent := range entries {
		idx := ent.Index
		if idx <= uint64(len(r.log)) {
			if r.log[idx-1].Term != ent.Term {
				r.log = r.log[:idx-1]
				r.log = append(r.log, entries[i:]...)
				break
			}
		} else {
			r.log = append(r.log, entries[i:]...)
			break
		}
	}

	// Update commit index.
	if leaderCommit > r.commitIndex {
		r.commitIndex = min(leaderCommit, uint64(len(r.log)))
	}
	return true
}

// RequestVote handles a vote request.
func (r *RaftNode) RequestVote(
	term uint64, candidateID string,
	lastLogIndex, lastLogTerm uint64,
) (voteGranted bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term < r.currentTerm {
		return false
	}

	if term > r.currentTerm {
		r.currentTerm = term
		r.state = StateFollower
		r.votedFor = ""
	}

	if r.votedFor != "" && r.votedFor != candidateID {
		return false
	}

	// Check log freshness.
	myLastTerm := uint64(0)
	if len(r.log) > 0 {
		myLastTerm = r.log[len(r.log)-1].Term
	}
	if lastLogTerm < myLastTerm {
		return false
	}
	if lastLogTerm == myLastTerm &&
		lastLogIndex < uint64(len(r.log)) {
		return false
	}

	r.votedFor = candidateID
	r.signalElectionReset()
	return true
}

// BecomeCandidate increments term and starts election.
func (r *RaftNode) BecomeCandidate() {
	r.mu.Lock()
	r.currentTerm++
	r.state = StateCandidate
	r.votedFor = r.nodeID
	r.mu.Unlock()
}

// BecomeLeader switches to leader state.
func (r *RaftNode) BecomeLeader() {
	r.mu.Lock()
	r.state = StateLeader
	r.mu.Unlock()
}

// Committed returns entries up to commitIndex.
func (r *RaftNode) Committed() []LogEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.commitIndex == 0 {
		return nil
	}
	out := make([]LogEntry, r.commitIndex)
	copy(out, r.log[:r.commitIndex])
	return out
}

// CommitIndex returns the last committed index.
func (r *RaftNode) CommitIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex
}

// SetCommitIndex updates commit index (used by leader).
func (r *RaftNode) SetCommitIndex(idx uint64) {
	r.mu.Lock()
	if idx > r.commitIndex {
		r.commitIndex = idx
	}
	r.mu.Unlock()
}

// resetElectionTimer restarts the election timeout.
func (r *RaftNode) resetElectionTimer() {
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	// Randomized timeout between 150-300ms.
	d := time.Duration(150+int(time.Now().UnixNano()%150)) *
		time.Millisecond
	r.electionTimer = time.AfterFunc(d, func() {
		r.BecomeCandidate()
	})
	r.signalElectionReset()
}

// signalElectionReset notifies the Run loop to reset election timer.
func (r *RaftNode) signalElectionReset() {
	select {
	case r.electionResetCh <- struct{}{}:
	default:
	}
}

// ErrNotLeader is returned when a non-leader tries to propose.
var ErrNotLeader = fmt.Errorf("not leader")
