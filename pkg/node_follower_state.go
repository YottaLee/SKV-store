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
			request := msg.request
			if request.GetTerm() < n.GetCurrentTerm() {
				msg.reply <- RequestVoteReply{
					Term:        n.GetCurrentTerm(),
					VoteGranted: false,
				}
				continue
			}
			if request.GetTerm() > n.GetCurrentTerm() {
				n.SetCurrentTerm(request.Term)
				n.setVotedFor("")
			}

			if n.GetVotedFor() != "" && n.GetVotedFor() != request.GetCandidate().GetId() {
				msg.reply <- RequestVoteReply{
					Term:        n.GetCurrentTerm(),
					VoteGranted: false,
				}
				continue
			}

			var grantVote bool
			localLastLogIndex := n.LastLogIndex()
			localLastLogTerm := n.GetLog(localLastLogIndex).GetTermId()

			if request.LastLogTerm != localLastLogTerm {
				grantVote = request.LastLogTerm > localLastLogTerm
			} else {
				grantVote = request.LastLogIndex >= localLastLogIndex
			}

			if grantVote {
				n.setVotedFor(request.Candidate.Id)
				timeout = randomTimeout(n.Config.ElectionTimeout)
			}
			msg.reply <- RequestVoteReply{
				Term:        n.GetCurrentTerm(),
				VoteGranted: grantVote,
			}
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
	fallback = true
	if n.Leader == nil || (n.Leader.Id != request.Leader.Id) {
		n.Leader = request.Leader
	}
	if n.GetCurrentTerm() < request.GetTerm() {
		n.SetCurrentTerm(request.GetTerm())
		n.setVotedFor("")
	}

	resetTimeout = true
	if request.PrevLogIndex > n.LastLogIndex() {
		reply <- AppendEntriesReply{
			Term:    n.GetCurrentTerm(),
			Success: false,
		}
	} else if entry := n.GetLog(request.PrevLogIndex); entry != nil && (entry.TermId != request.PrevLogTerm) {
		reply <- AppendEntriesReply{
			Term:    n.GetCurrentTerm(),
			Success: false,
		}
	} else {
		n.LeaderMutex.Lock()

		for _, logEntry := range request.GetEntries() {
			if logEntry.GetIndex() > n.LastLogIndex() {
				n.StoreLog(logEntry)
			} else if n.GetLog(logEntry.GetIndex()) == nil || (logEntry.GetTermId() != n.GetLog(logEntry.GetIndex()).GetTermId()) {
				n.TruncateLog(logEntry.GetIndex())
				n.StoreLog(logEntry)
			}
		}

		//if len(request.Entries) > 0 {
		//	firstEntry := request.Entries[0]
		//	curLastEntry := n.GetLog(n.LastLogIndex())
		//	if (curLastEntry.Index >= firstEntry.Index) ||
		//		((curLastEntry.Index == firstEntry.Index) && (curLastEntry.TermId != firstEntry.TermId)) {
		//		n.TruncateLog(firstEntry.Index)
		//	}
		//
		//	if n.LastLogIndex()+1 != firstEntry.GetIndex() {
		//		panic("error in appending log entry")
		//	}
		//
		//	for _, entry := range request.Entries {
		//		n.StoreLog(entry)
		//	}
		//}
		n.LeaderMutex.Unlock()

		if request.LeaderCommit > n.CommitIndex.Load() {
			newCommitIndex := uint64(math.Min(float64(request.LeaderCommit), float64(n.LastLogIndex())))
			for newCommitIndex > n.LastApplied.Load() {
				n.LastApplied.Add(1)
				n.processLogEntry(n.LastApplied.Load())
			}
			n.CommitIndex.Store(newCommitIndex)
		}

		reply <- AppendEntriesReply{
			Term:    n.GetCurrentTerm(),
			Success: true,
		}
	}

	return
}
