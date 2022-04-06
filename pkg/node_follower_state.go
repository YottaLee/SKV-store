package pkg

import (
	"math"
)

// doFollower implements the logic for a Raft node in the follower state.
func (n *Node) doFollower() stateFunction {
	n.Out("Transitioning to FollowerState")
	n.setState(FollowerState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.
	timeout := randomTimeout(n.Config.ElectionTimeout)

	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				n.Out("graceful exit")
				return nil
			}
		case msg := <-n.appendEntries:
			reset, _ := n.handleAppendEntries(msg)
			if reset {
				timeout = randomTimeout(n.Config.ElectionTimeout)
			}
		case msg := <-n.requestVote:
			n.handleRequestVote(msg)
		case <-timeout:
			n.setLeader(nil)
			return n.doCandidate
		case msg := <-n.clientRequest:
			msg.reply <- ClientReply{
				Status:     ClientStatus_NOT_LEADER,
				LeaderHint: n.Leader,
			}
		}
	}
}

func (n *Node) handleVoteRequest(msg RequestVoteMsg) {
	request := msg.request
	reply := msg.reply
	// If a server receives a request with a stale term number, it rejects the request (&5.1)
	if n.GetCurrentTerm() > request.GetTerm() {
		reply <- RequestVoteReply{Term: n.GetCurrentTerm(), VoteGranted: false}
		return
	} else if n.GetCurrentTerm() < request.GetTerm() {
		n.SetCurrentTerm(request.GetTerm())
		// If follower and candidate are in different term. Reset the follower's vote for
		n.setVotedFor("")
	}
	if n.GetVotedFor() != "" && n.GetVotedFor() != request.GetCandidate().GetId() {
		msg.reply <- RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: false,
		}
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	lastTerm := n.GetLog(n.LastLogIndex()).GetTermId()
	if lastTerm < request.GetLastLogTerm() ||
		(lastTerm == request.GetLastLogTerm() && n.LastLogIndex() <= request.GetLastLogIndex()) {
		n.setVotedFor(request.GetCandidate().GetId())
		reply <- RequestVoteReply{Term: n.GetCurrentTerm(), VoteGranted: true}
		return
	}
	reply <- RequestVoteReply{Term: n.GetCurrentTerm(), VoteGranted: false}
	return
}

// handleAppendEntries handles an incoming AppendEntriesMsg. It is called by a
// node in a follower, candidate, or leader state. It returns two booleans:
// - resetTimeout is true if the follower node should reset the election timeout
// - fallback is true if the node should become a follower again
func (n *Node) handleAppendEntries(msg AppendEntriesMsg) (resetTimeout, fallback bool) {
	// TODO: Students should implement this method
	request := msg.request
	reply := msg.reply
	// If a server receives a request with a lower term, it rejects the request (&5.1)
	if n.GetCurrentTerm() > request.GetTerm() {
		reply <- AppendEntriesReply{Term: n.GetCurrentTerm(), Success: false}
		return false, false
	}

	n.Leader = request.Leader
	if n.GetCurrentTerm() < request.GetTerm() {
		n.SetCurrentTerm(request.GetTerm())
		n.setVotedFor("")
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if request.PrevLogIndex > 0 && (n.GetLog(request.PrevLogIndex) == nil || n.GetLog(request.PrevLogIndex).GetTermId() != request.GetPrevLogTerm()) {
		reply <- AppendEntriesReply{Term: n.GetCurrentTerm(), Success: false}
		return true, true
	}
	// Found a log entry whose term and index are matched with prevLogIndex and preLogTerm
	n.LeaderMutex.Lock()
	for _, leaderLog := range request.GetEntries() {
		if leaderLog.GetIndex() > n.LastLogIndex() {
			// Append any new entries not already in the log
			n.StoreLog(leaderLog)
		} else if n.GetLog(leaderLog.GetIndex()) == nil || (leaderLog.GetTermId() != n.GetLog(leaderLog.GetIndex()).GetTermId()) {
			// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
			n.TruncateLog(leaderLog.GetIndex())
			// Append any new entries not already in the log
			n.StoreLog(leaderLog)
		}
	}
	n.LeaderMutex.Unlock()
	if request.GetLeaderCommit() > n.CommitIndex.Load() {
		n.CommitIndex.Store(uint64(math.Min(float64(request.GetLeaderCommit()), float64(n.LastLogIndex()))))
		for n.CommitIndex.Load() > n.LastApplied.Load() {
			n.LastApplied.Add(1)
			n.processLogEntry(n.LastApplied.Load())
		}
	}
	reply <- AppendEntriesReply{Term: n.GetCurrentTerm(), Success: true}
	return true, true
}
