package pkg

// doCandidate implements the logic for a Raft node in the candidate state.
func (n *Node) doCandidate() stateFunction {
	n.Out("Transitioning to CANDIDATE_STATE")
	n.setState(CandidateState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	curTerm := n.GetCurrentTerm() + 1
	n.SetCurrentTerm(curTerm)
	n.setVotedFor(n.Self.Id)

	fallBack, electionResults := n.requestVotes(curTerm)

	timeout := randomTimeout(n.Config.ElectionTimeout)

	for {
		select {
		case <-timeout:
			curTerm := n.GetCurrentTerm() + 1
			n.setVotedFor(n.Self.Id)
			n.SetCurrentTerm(curTerm)

			fallBack, electionResults = n.requestVotes(curTerm)
			timeout = randomTimeout(n.Config.ElectionTimeout)

		case msg := <-n.appendEntries:
			if _, fb := n.handleAppendEntries(msg); fb {
				return n.doFollower
			}
		case msg := <-n.requestVote:
			if n.handleRequestVote(msg) {
				return n.doFollower
			}
		case msg := <-n.clientRequest:
			reply := msg.reply
			reply <- ClientReply{
				Status:     ClientStatus_ELECTION_IN_PROGRESS,
				Response:   nil,
				LeaderHint: n.Self,
			}
		case exit := <-n.gracefulExit:
			if exit {
				return nil
			}
		default:

			if fallBack {
				return n.doFollower
			}
			if electionResults {
				return n.doLeader
			}
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
	// TODO: Students should implement this method
	lastLogIndex := n.LastLogIndex()
	majority := (n.Config.ClusterSize / 2) + 1
	request := RequestVoteRequest{
		Term:         currTerm,
		Candidate:    n.Self,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  n.StableStore.GetLog(lastLogIndex).TermId,
	}
	votesChan := make(chan bool, len(n.Peers))
	for _, peer := range n.getPeers() {
		go func() {
			reply, err := peer.RequestVoteRPC(n, &request)
			if err != nil {
				votesChan <- false
				return
			}

			if reply.Term > currTerm {
				n.setVotedFor("")
				n.SetCurrentTerm(reply.Term)
				fallback = true
			}

			votesChan <- reply.VoteGranted

		}()
	}
	votesCount := 0

	for i := 0; i < len(n.Peers); i++ {
		res := <-votesChan
		if res {
			votesCount++
			if votesCount >= majority {
				electionResult = true
				return
			}
		}
	}

	electionResult = votesCount >= majority

	return
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	request := msg.request
	reply := msg.reply

	if request.Term > n.GetCurrentTerm() {
		n.setVotedFor("")
		n.SetCurrentTerm(request.Term)
		fallback = true

		requestReply := RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: false,
		}

		lastLogIndex := n.LastLogIndex()
		lastLogTerm := n.GetLog(lastLogIndex).TermId
		if request.LastLogTerm != lastLogTerm {
			requestReply.VoteGranted = request.LastLogTerm > lastLogTerm
		} else {
			requestReply.VoteGranted = request.LastLogIndex >= lastLogIndex
		}
		if requestReply.VoteGranted {
			n.setVotedFor(request.Candidate.Id)
			reply <- requestReply
			return
		}
		reply <- requestReply
		return
	} else {
		request := RequestVoteReply{
			Term:        n.GetCurrentTerm(),
			VoteGranted: false,
		}
		if request.Term == n.GetCurrentTerm() &&
			n.GetVotedFor() == msg.request.Candidate.Id {
			request.VoteGranted = true
		}
		reply <- request
		return false
	}
}
