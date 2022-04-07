package pkg

import (
	"fmt"
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
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if request.PrevLogIndex > 0 && (n.GetLog(request.PrevLogIndex) == nil || n.GetLog(request.PrevLogIndex).GetTermId() != request.GetPrevLogTerm()) {
		reply <- AppendEntriesReply{Term: n.GetCurrentTerm(), Success: false}
		return true, true
	}
	// Found a log entry whose term and index are matched with prevLogIndex and preLogTerm
	n.LeaderMutex.Lock()
	if len(request.Entries) > 0 {
		newFirstEntry := request.Entries[0]
		ourLastEntry := n.GetLog(n.LastLogIndex())
		if (ourLastEntry.Index >= newFirstEntry.Index) ||
			((ourLastEntry.Index == newFirstEntry.Index) && (ourLastEntry.TermId != newFirstEntry.TermId)) {
			n.TruncateLog(newFirstEntry.GetIndex())
		}

		if n.LastLogIndex()+1 != newFirstEntry.GetIndex() {
			panic("truncation not work")
		}

		for _, entry := range request.Entries {
			n.StoreLog(entry)
		}
	}
	n.LeaderMutex.Unlock()

	if request.GetLeaderCommit() > n.CommitIndex.Load() {
		newCommitIndex := uint64(math.Min(float64(request.GetLeaderCommit()), float64(n.LastLogIndex())))
		n.Out("Updating commitIndex from %v -> %v", n.CommitIndex.Load(), uint64(newCommitIndex))
		for newCommitIndex > n.LastApplied.Load() {
			n.LastApplied.Add(1)
			n.processLogEntry(n.LastApplied.Load())
		}
		n.CommitIndex.Store(newCommitIndex)
		n.Out("commitIndex %v --- lastapplied %v", n.CommitIndex.Load(), n.LastApplied.Load())
		fmt.Printf("commitIndex %v --- lastapplied %v", n.CommitIndex.Load(), n.LastApplied.Load())
	}
	reply <- AppendEntriesReply{Term: n.GetCurrentTerm(), Success: true}
	return true, true
}
