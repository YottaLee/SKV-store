package pkg

// doFollower implements the logic for a Raft node in the follower state.
func (n *Node) doFollower() stateFunction {
	n.Out("Transitioning to FollowerState")
	n.setState(FollowerState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.
	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
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
	return
}
