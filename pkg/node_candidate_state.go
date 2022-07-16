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
	fallbackChan := make(chan bool)
	electionChan := make(chan bool)
	go func() {
		fallBack, electionResults := n.requestVotes(curTerm)
		if fallBack {
			fallbackChan <- true
		}

		if electionResults {
			electionChan <- true
		}
	}()
	timeout := randomTimeout(n.Config.ElectionTimeout)

	for {
		select {
		case <-timeout:
			return n.doCandidate
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
		case fallback := <-fallbackChan:
			if fallback {
				return n.doFollower
			}
		case election := <-electionChan:
			if election {
				return n.doLeader
			}
		}
	}
}

type RequestVoteResult string

const (
	RequestVoteSuccess  RequestVoteResult = "success"
	RequestVoteFail                       = "fail"
	RequestVoteFallback                   = "fallback"
)

func (n *Node) requestPeerVote(peer *RemoteNode, msg *RequestVoteRequest, resultChan chan RequestVoteResult) {
	reply, err := peer.RequestVoteRPC(n, msg)

	if err != nil {
		n.Error("Error in requesting a vote from %v", peer.GetId())
		resultChan <- RequestVoteFail
	} else {
		if reply.GetVoteGranted() {
			resultChan <- RequestVoteSuccess
		} else if reply.GetTerm() > n.GetCurrentTerm() {
			n.SetCurrentTerm(reply.GetTerm())
			n.setVotedFor("")
			resultChan <- RequestVoteFallback
		} else {
			resultChan <- RequestVoteFail
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
	// TODO: Students should implement this method
	remaining := 0
	resultChan := make(chan RequestVoteResult)
	for _, peer := range n.Peers {
		if n.Self.GetId() == peer.GetId() {
			continue
		}
		msg := RequestVoteRequest{
			Term:         currTerm,
			Candidate:    n.Self,
			LastLogIndex: n.LastLogIndex(),
			LastLogTerm:  n.GetLog(n.LastLogIndex()).GetTermId(),
		}
		remaining++
		go n.requestPeerVote(peer, &msg, resultChan)
	}

	vote := 1
	reject := 0
	majority := n.Config.ClusterSize/2 + 1
	for remaining > 0 {
		requestVoteResult := <-resultChan
		remaining--
		if requestVoteResult == RequestVoteFallback {
			fallback = true
			return
		}
		if requestVoteResult == RequestVoteSuccess {
			vote++
			if vote >= majority {
				electionResult = true
				return
			}
		} else {
			reject++
			if reject >= majority {
				electionResult = false
				return
			}
		}
	}
	return
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	request := msg.request
	reply := msg.reply
	if n.GetCurrentTerm() >= request.GetTerm() {
		reply <- RequestVoteReply{Term: n.GetCurrentTerm(), VoteGranted: false}
		return false
	}
	n.SetCurrentTerm(request.GetTerm())
	n.setVotedFor("")
	lastTerm := n.GetLog(n.LastLogIndex()).GetTermId()
	if lastTerm < request.GetLastLogTerm() ||
		(lastTerm == request.GetLastLogTerm() && n.LastLogIndex() <= request.GetLastLogIndex()) {
		n.setVotedFor(request.GetCandidate().GetId())
		reply <- RequestVoteReply{Term: n.GetCurrentTerm(), VoteGranted: true}
		return true
	}
	reply <- RequestVoteReply{Term: n.GetCurrentTerm(), VoteGranted: false}
	return true
}
