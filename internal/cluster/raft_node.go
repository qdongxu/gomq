// raft_node.go extends RaftNode with multi-node network communication.
package cluster

import (
	"sync"
	"time"
)

// SetTransport sets the transport for network communication.
func (r *RaftNode) SetTransport(t RaftTransport) {
	r.mu.Lock()
	r.transport = t
	r.mu.Unlock()
}

// HandleAppendEntries implements RaftHandler.
func (r *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	success := r.AppendEntries(
		req.Term, req.LeaderCommit,
		req.PrevLogIndex, req.PrevLogTerm,
		req.Entries,
	)
	return &AppendEntriesResponse{
		Term:    r.Term(),
		Success: success,
	}
}

// HandleRequestVote implements RaftHandler.
func (r *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	voteGranted := r.RequestVote(
		req.Term, req.CandidateID,
		req.LastLogIndex, req.LastLogTerm,
	)
	return &RequestVoteResponse{
		Term:        r.Term(),
		VoteGranted: voteGranted,
	}
}

// HandleInstallSnapshot implements RaftHandler.
func (r *RaftNode) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	return &InstallSnapshotResponse{Term: r.Term()}
}

// Run starts the main Raft loop with election timer and heartbeat.
func (r *RaftNode) Run(stopCh <-chan struct{}) {
	 electionTimer := time.NewTimer(r.randomElectionTimeout())
	defer electionTimer.Stop()

	heartbeat := time.NewTicker(50 * time.Millisecond)
	defer heartbeat.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-electionTimer.C:
			r.mu.RLock()
			state := r.state
			r.mu.RUnlock()
			if state != StateLeader {
				r.startElection()
			}
			if !electionTimer.Stop() {
				select {
				case <-electionTimer.C:
				default:
				}
			}
			electionTimer.Reset(r.randomElectionTimeout())
		case <-r.electionResetCh:
			if !electionTimer.Stop() {
				select {
				case <-electionTimer.C:
				default:
				}
			}
			electionTimer.Reset(r.randomElectionTimeout())
		case <-heartbeat.C:
			r.mu.RLock()
			state := r.state
			r.mu.RUnlock()
			if state == StateLeader {
				r.sendHeartbeats()
			}
		}
	}
}

// startElection becomes candidate and requests votes from all peers.
func (r *RaftNode) startElection() {
	r.BecomeCandidate()

	r.mu.RLock()
	term := r.currentTerm
	lastLogIndex := uint64(len(r.log))
	lastLogTerm := uint64(0)
	if len(r.log) > 0 {
		lastLogTerm = r.log[len(r.log)-1].Term
	}
	peers := make([]string, len(r.peers))
	copy(peers, r.peers)
	nodeID := r.nodeID
	r.mu.RUnlock()

	votes := 1 // self vote
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			req := &RequestVoteRequest{
				Term:         term,
				CandidateID:  nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			r.mu.RLock()
			t := r.transport
			r.mu.RUnlock()
			if t == nil {
				return
			}
			resp, err := t.SendRequestVote(peer, req)
			if err != nil {
				return
			}
			if resp.VoteGranted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	// Check if we got majority and are still candidate.
	r.mu.RLock()
	state := r.state
	currentTerm := r.currentTerm
	r.mu.RUnlock()

	if state == StateCandidate && currentTerm == term {
		if votes > (len(peers)+1)/2 {
			r.BecomeLeader()
			r.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends empty AppendEntries to all peers as heartbeat.
func (r *RaftNode) sendHeartbeats() {
	r.mu.RLock()
	term := r.currentTerm
	commitIndex := r.commitIndex
	peers := make([]string, len(r.peers))
	copy(peers, r.peers)
	nodeID := r.nodeID
	t := r.transport
	r.mu.RUnlock()

	if t == nil {
		return
	}

	for _, peer := range peers {
		go func(peer string) {
			req := &AppendEntriesRequest{
				Term:         term,
				LeaderID:     nodeID,
				LeaderCommit: commitIndex,
			}
			_, _ = t.SendAppendEntries(peer, req)
		}(peer)
	}
}

// Stop stops the Raft node and its transport.
func (r *RaftNode) Stop() {
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	r.mu.RLock()
	t := r.transport
	r.mu.RUnlock()
	if t != nil {
		t.Stop()
	}
}

// ReplicateAll sends the latest log entries to all peers.
func (r *RaftNode) ReplicateAll() {
	r.mu.RLock()
	if r.state != StateLeader {
		r.mu.RUnlock()
		return
	}
	term := r.currentTerm
	commitIndex := r.commitIndex
	logLen := uint64(len(r.log))
	if logLen == 0 {
		r.mu.RUnlock()
		return
	}
	entries := make([]LogEntry, logLen)
	copy(entries, r.log)
	peers := make([]string, len(r.peers))
	copy(peers, r.peers)
	nodeID := r.nodeID
	t := r.transport
	r.mu.RUnlock()

	if t == nil {
		return
	}

	for _, peer := range peers {
		go func(peer string) {
			var prevLogIndex, prevLogTerm uint64
			if logLen > 1 {
				prevLogIndex = logLen - 1
				prevLogTerm = entries[prevLogIndex-1].Term
			}
			req := &AppendEntriesRequest{
				Term:         term,
				LeaderID:     nodeID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{entries[logLen-1]},
				LeaderCommit: commitIndex,
			}
			_, _ = t.SendAppendEntries(peer, req)
		}(peer)
	}
}

// randomElectionTimeout returns a random timeout between 150-300ms.
func (r *RaftNode) randomElectionTimeout() time.Duration {
	return time.Duration(150+int(time.Now().UnixNano()%150)) * time.Millisecond
}
