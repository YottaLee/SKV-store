package pkg

import (
	"strconv"
	"strings"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.
	if n.GetVotedFor() != n.Self.Id {
		panic("Leader should voted for itself")
	}

	n.Leader = n.Self

	n.LeaderMutex.Lock()
	for _, node := range n.getPeers() {
		n.nextIndex[node.Id] = n.LastLogIndex() + 1
		n.matchIndex[node.Id] = uint64(0)
	}
	n.matchIndex[n.Self.Id] = n.LastLogIndex()

	leaderEntry := &LogEntry{
		Index:  n.LastLogIndex() + 1,
		TermId: n.GetCurrentTerm(),
		Type:   CommandType_NOOP,
		Data:   []byte{},
	}
	n.StoreLog(leaderEntry)
	n.LeaderMutex.Unlock()

	fallbackChan := make(chan bool)
	go func() {
		fallback := n.sendHeartbeats()
		if fallback {
			fallbackChan <- true
		}
	}()

	heartbeat := time.NewTicker(n.Config.HeartbeatTimeout)
	for {
		select {
		case msg := <-n.appendEntries:
			// receive append entry in leader state
			if _, fallback := n.handleAppendEntries(msg); fallback {
				return n.doFollower
			}
		case msg := <-n.requestVote:
			// receive request vote in leader state
			if n.handleRequestVote(msg) {
				return n.doFollower
			}
		case msg := <-n.clientRequest:
			request := msg.request
			reply := msg.reply
			cacheId := CreateCacheID(request.ClientId, request.SequenceNum)

			cacheReply, exists := n.GetCachedReply(cacheId)
			if exists {
				reply <- *cacheReply
			} else {
				n.LeaderMutex.Lock()
				n.requestsMutex.Lock()

				requestEntry := LogEntry{
					Index:   n.LastLogIndex() + 1,
					TermId:  n.GetCurrentTerm(),
					Type:    CommandType_STATE_MACHINE_COMMAND,
					Command: request.GetStateMachineCmd(),
					Data:    request.GetData(),
					CacheId: cacheId,
				}

				n.requestsByCacheID[cacheId] = append(n.requestsByCacheID[cacheId], reply)

				n.StoreLog(&requestEntry)

				n.requestsMutex.Unlock()
				n.LeaderMutex.Unlock()

				fallback := n.sendHeartbeats()
				if fallback {
					return n.doFollower
				}
			}

		case fallbackFromHB := <-fallbackChan:
			if fallbackFromHB {
				return n.doFollower
			}

		case <-heartbeat.C:
			// send heartbeat in go routine
			go func() {
				fallback := n.sendHeartbeats()
				if fallback {
					fallbackChan <- true
				}
			}()
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}

}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (n *Node) sendHeartbeats() (fallback bool) {
	// TODO: Students should implement this method
	n.LeaderMutex.Lock()
	n.nextIndex[n.Self.Id] = n.LastLogIndex() + 1
	n.matchIndex[n.Self.Id] = n.LastLogIndex()
	currTerm := n.GetCurrentTerm()
	n.LeaderMutex.Unlock()

	peersLen := len(n.Peers)
	doneCh := make(chan bool, peersLen)
	fallbackCh := make(chan bool, peersLen)

	for _, item := range n.Peers {
		if item.Id != n.Self.Id {
			p := item
			go func() {
				success := false
				defer func() {
					doneCh <- success
				}()

				for {
					// sent out everything from nextIndex -> lastLogIndex
					n.LeaderMutex.Lock()
					nxtInd := n.nextIndex[p.Id]

					lastInd := n.LastLogIndex()
					var ensToSend []*LogEntry
					for i := nxtInd; i <= lastInd; i++ {
						ensToSend = append(ensToSend, n.GetLog(i))
					}

					var prevLogTerm uint64
					if n.GetLog(nxtInd-1) != nil {
						prevLogTerm = n.GetLog(nxtInd - 1).TermId
					} else {
						prevLogTerm = 0
					}
					n.LeaderMutex.Unlock()

					req := &AppendEntriesRequest{
						Term:         currTerm,
						Leader:       n.Self,
						PrevLogIndex: nxtInd - 1,
						PrevLogTerm:  prevLogTerm,
						Entries:      ensToSend,
						LeaderCommit: n.CommitIndex.Load(),
					}
					reply, err := p.AppendEntriesRPC(n, req)

					if err != nil {
						if err.Error() != "the network policy has forbid this communication" {
							n.Out("AppendEntriesRPC to %v failed with %v", p.Id, err)
						}
						return
					}

					success = reply.Success
					if reply.Term > currTerm {
						n.Out("falling back due to %v, from request %v", reply, req)
						n.SetCurrentTerm(reply.Term)
						n.setVotedFor("")
						fallbackCh <- true
						return
					}

					if reply.Success {
						n.LeaderMutex.Lock()
						n.nextIndex[p.Id] = lastInd + 1
						n.matchIndex[p.Id] = lastInd
						n.checkForCommit()
						n.LeaderMutex.Unlock()
						return
					} else if nxtInd <= 1 {
						// can't go back anymore!!
						n.Error("AppendEntriesRPC to %v failed consistency check at 0", p.Id)
						return
					} else {
						n.LeaderMutex.Lock()
						n.nextIndex[p.Id] = nxtInd - 1
						n.LeaderMutex.Unlock()
					}

				}
			}()
		}
	}

	majority := n.Config.ClusterSize/2 + 1
	successCnt := 1
	for i := 1; i < peersLen; i++ {
		select {
		case success := <-doneCh:
			if success {
				successCnt++
				if successCnt >= majority {
					return false
				}
			}
		case fall := <-fallbackCh:
			if fall {
				return true
			}
		}
	}

	return false
}

func (n *Node) checkForCommit() {
	majority := n.Config.ClusterSize/2 + 1

	newCommit := n.CommitIndex.Load()
	for i := n.CommitIndex.Load() + 1; i <= n.LastLogIndex(); i++ {
		cnt := 0
		for _, ind := range n.matchIndex {
			if ind >= i {
				cnt++
			}

			if cnt >= majority {
				newCommit = i
				break
			}
		}

		if newCommit != i {
			break
		}
	}

	if newCommit > n.CommitIndex.Load() && n.GetLog(newCommit).TermId == n.GetCurrentTerm() {
		n.CommitIndex.Store(newCommit)

		for i := n.LastApplied.Load() + 1; i <= n.CommitIndex.Load(); i++ {
			n.processLogEntry(i)
			n.LastApplied.Store(i)
		}
	}
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (n *Node) processLogEntry(logIndex uint64) (fallback bool) {
	fallback = false
	entry := n.GetLog(logIndex)
	n.Out("Processing log index: %v, entry: %v", logIndex, entry)

	status := ClientStatus_OK
	var response []byte
	var err error
	var clientId uint64

	switch entry.Type {
	case CommandType_NOOP:
		return
	case CommandType_CLIENT_REGISTRATION:
		clientId = logIndex
	case CommandType_STATE_MACHINE_COMMAND:
		if clientId, err = strconv.ParseUint(strings.Split(entry.GetCacheId(), "-")[0], 10, 64); err != nil {
			panic(err)
		}
		if resp, ok := n.GetCachedReply(entry.GetCacheId()); ok {
			status = resp.GetStatus()
			response = resp.GetResponse()
		} else {
			response, err = n.StateMachine.ApplyCommand(entry.Command, entry.Data)
			if err != nil {
				status = ClientStatus_REQ_FAILED
				response = []byte(err.Error())
			}
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		ClientId:   clientId,
		Response:   response,
		LeaderHint: &RemoteNode{Addr: n.Self.Addr, Id: n.Self.Id},
	}

	// Send reply to client
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	// Add reply to cache
	if entry.CacheId != "" {
		if err = n.CacheClientReply(entry.CacheId, reply); err != nil {
			panic(err)
		}
	}
	if replies, exists := n.requestsByCacheID[entry.CacheId]; exists {
		for _, ch := range replies {
			ch <- reply
		}
		delete(n.requestsByCacheID, entry.CacheId)
	}

	return
}
