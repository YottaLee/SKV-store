package test

import (
	"fmt"
	"golang.org/x/net/context"
	raft "raft/pkg"
	"raft/pkg/hashmachine"
	"testing"
	"time"
)

//the client registers itself to the old leader, and the raft should return a hint to the
//client that this node is not a leader.
func TestClientInteraction_Oldleader(t *testing.T) {
	// Out.Println("TestClientInteraction_oldleader start")
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 5

	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	oldLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	// partition leader, triggering election
	oldTerm := oldLeader.GetCurrentTerm()
	oldLeader.NetworkPolicy.PauseWorld(true)

	// wait for new leader to be elected
	time.Sleep(time.Second * WaitPeriod)

	// unpause old leader and wait for it to become a follower
	oldLeader.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * WaitPeriod)
	//Out.Printf("OLD LEADER is: %v\n", oldLeader.State.String())
	newLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	if oldLeader.Self.Id == newLeader.Self.Id {
		t.Errorf("leader did not change")
	}

	if newLeader.GetCurrentTerm() == oldTerm {
		t.Errorf("term did not change")
	}

	//register the client to the old leader
	reply, _ := oldLeader.Self.RegisterClientRPC()
	if reply.Status != raft.ClientStatus_NOT_LEADER && reply.Status != raft.ClientStatus_ELECTION_IN_PROGRESS {
		t.Error(reply.Status)
		t.Fatal("Wrong response when registering a client to an old leader")
	}

	req := raft.ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := oldLeader.Self.ClientRequestRPC(&req)
	if clientResult.Status != raft.ClientStatus_NOT_LEADER && clientResult.Status != raft.ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to an old leader")
	}
}

func TestClientInteraction_OldleaderFallBack(t *testing.T) {
	// Out.Println("TestClientInteraction_oldleader start")
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 5

	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	oldLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	//register the client to the old leader
	_, err = oldLeader.Self.RegisterClientRPC()
	if err != nil {
		t.Errorf(err.Error())
	}

	// partition leader, triggering election
	oldTerm := oldLeader.GetCurrentTerm()
	oldLeader.NetworkPolicy.PauseWorld(true)

	// wait for new leader to be elected
	time.Sleep(time.Second * 1)

	// unpause old leader and wait for it to become a follower
	oldLeader.NetworkPolicy.PauseWorld(false)
	time.Sleep(time.Second * WaitPeriod)
	//Out.Printf("OLD LEADER is: %v\n", oldLeader.State.String())
	newLeader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	if oldLeader.Self.Id == newLeader.Self.Id {
		t.Errorf("leader did not change")
	}

	if newLeader.GetCurrentTerm() == oldTerm {
		t.Errorf("term did not change")
	}

	req := raft.ClientRequest{
		ClientId:        1,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := oldLeader.Self.ClientRequestRPC(&req)
	if clientResult.Status != raft.ClientStatus_NOT_LEADER && clientResult.Status != raft.ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when sending a client request to an old leader")
	}
}

// Test leaders can register the client and process duplicate request from clients properly
func TestClientInteraction_Leader_DuplicateRequest(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	cluster, _ := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// This clientId value registers a client for the first time.
	clientid := 0

	// Hash initialization request
	initReq := raft.ClientRequest{
		ClientId:        uint64(clientid),
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status != raft.ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	// Make sure further request is correct processed
	ClientReq := raft.ClientRequest{
		ClientId:        uint64(clientid),
		SequenceNum:     2,
		StateMachineCmd: hashmachine.HashChainAdd,
		Data:            []byte{},
	}
	clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult.Status != raft.ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}
	time.Sleep(2 * time.Second)

	//second round same request
	clientResult2, _ := leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult2.Status != raft.ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}

	time.Sleep(2 * time.Second)
	//second round same request
	clientResult3, _ := leader.ClientRequestCaller(context.Background(), &ClientReq)
	if clientResult3.Status != raft.ClientStatus_OK {
		t.Fatal("Leader failed to commit a client request")
	}
}

func TestClientInteraction_TwoNodeCluster(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 2
	cluster, _ := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("cannot find leader")
	}

	// First make sure we can register a client correctly
	clientid := 0

	// Hash initialization request
	initReq := raft.ClientRequest{
		ClientId:        uint64(clientid),
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	reply, _ := leader.ClientRequestCaller(context.Background(), &initReq)

	if reply.Status != raft.ClientStatus_OK {
		t.Errorf("%v", reply.Status)
		t.Fatal("We don't have a leader yet")
	}
	logsMatch(leader, cluster)
}

// Handle RequestVote with Stale Term
func TestHandleHeartbeat_Follower(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	if err != nil {
		t.Fatal(err)
	}
	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	follower := findFollower(cluster)

	// make sure the client get the correct response while registering itself with a candidate
	reply, _ := follower.AppendEntriesCaller(context.Background(), &raft.AppendEntriesRequest{
		Term:         uint64(1),
		Leader:       leader.Self,
		PrevLogIndex: uint64(3),
		PrevLogTerm:  uint64(1),
		Entries:      []*raft.LogEntry{},
		LeaderCommit: uint64(5),
	})
	if reply.Success {
		t.Fatal("Should've denied vote")
	}

}

//the client registers itself to a caldidate, and the raft should return a hint to the client that this node is not a leader.
func TestClientInteraction_Candidate(t *testing.T) {
	// Out.Println("TestClientInteraction_Candidate start")
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 7
	cluster, _ := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(3 * time.Second)

	_, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	for _, node := range cluster {
		if node.GetState() == raft.CandidateState {
			fmt.Println("find the candidate!")
			reply, _ := node.Self.RegisterClientRPC()
			fmt.Printf("reply request: %v\n", reply.Status)
			if reply.Status != raft.ClientStatus_NOT_LEADER && reply.Status != raft.ClientStatus_ELECTION_IN_PROGRESS {
				t.Error(reply.Status)
				t.Fatal("Wrong response when registering a client to a candidate")
			}

			req := raft.ClientRequest{
				ClientId:        1,
				SequenceNum:     1,
				StateMachineCmd: hashmachine.HashChainInit,
				Data:            []byte("hello"),
			}
			clientResult, _ := node.Self.ClientRequestRPC(&req)
			fmt.Printf("client request1: %v\n", clientResult.Status)
			if clientResult.Status != raft.ClientStatus_NOT_LEADER && clientResult.Status != raft.ClientStatus_ELECTION_IN_PROGRESS {
				t.Fatal("Wrong response when sending a client request to a candidate")
			}
			break
		}
	}
}

func TestClientInteraction_Candidate_ClientRequest(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{5001, 5002, 5003, 5004, 5005})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Shutdown all nodes once finished
	defer cleanupCluster(nodes)
	time.Sleep(time.Second * WaitPeriod) // Long enough for timeout+election

	// Partition off leader
	leader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}
	leaderTerm := leader.GetCurrentTerm()
	leader.NetworkPolicy.PauseWorld(true)

	time.Sleep(time.Second * 2) // Wait, just in case the partitioned leader attempts anything

	// Partition off follower
	follower := findFollower(nodes)
	follower.NetworkPolicy.PauseWorld(true)

	// Test: Make sure that the original leader remains a leader post-partition
	if leader.GetState() != raft.LeaderState {
		t.Errorf("Leader should remain leader even when partitioned off")
		return
	}

	pausedLeaderTerm := leader.GetCurrentTerm()

	if leaderTerm != pausedLeaderTerm {
		t.Errorf("Leader's term should remain the same after partition. Went from %v to %v", leaderTerm, pausedLeaderTerm)
		return
	}

	time.Sleep(time.Second * WaitPeriod) // Finish waiting for rest of cluster to elect a new leader

	// Test: Make sure partitioned follower has transitioned to candidate state and has increased term
	if follower.GetState() != raft.CandidateState {
		t.Errorf("Partitioned follower has not transitioned to candidate after %v seconds", WaitPeriod)
	}

	clientid := 0
	// Hash initialization request
	initReq := raft.ClientRequest{
		ClientId:        uint64(clientid),
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	clientResult, _ := follower.ClientRequestCaller(context.Background(), &initReq)
	if clientResult.Status != raft.ClientStatus_ELECTION_IN_PROGRESS {
		t.Fatal("Wrong response when a client request to a candidate")
	}
}

func TestClientInteractions(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{7001, 7002, 7003, 7004, 7005})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Shutdown all nodes once finished
	defer cleanupCluster(nodes)

	time.Sleep(time.Second * WaitPeriod) // Long enough for timeout+election

	// Find leader to ensure that election has finished before we grab a follower
	leader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	leaderRemoteNode := leader.Self

	follower := findFollower(nodes)

	// Test: Request to follower should return NOT_LEADER
	// This clientId value registers a client for the first time.
	clientid := uint64(0)

	// Hash initialization request
	req := raft.ClientRequest{
		ClientId:        uint64(clientid),
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            []byte("hello"),
	}
	result, _ := follower.RegisterClient(&req)

	if result.Status != raft.ClientStatus_NOT_LEADER {
		t.Errorf("Followers should return NOT_LEADER when they receive a RegisterClient; instead returned %v", result.Status)
		return
	}

	if leader == nil || result.LeaderHint.Id != leaderRemoteNode.Id {
		t.Errorf("Follower doesn't return correct leader address to RegisterClient. Should be %v, but is %v", leaderRemoteNode, result.LeaderHint)
		return
	}

	// Test: Returning OK should mean that client registration entry is now on majority of followers

	result, _ = leader.RegisterClient(&req)

	if result.Status != raft.ClientStatus_OK {
		t.Errorf("Registering with the leader should return OK when they receive a RegisterClient; instead returned %v", result.Status)
		return
	}

	propagatedTo := 0
	for _, node := range nodes {
		nodeIndex := node.LastLogIndex()
		if nodeIndex >= result.ClientId {
			nodeEntry := node.GetLog(result.ClientId)
			if nodeEntry == nil {
				t.Errorf("The log entry for the client's registration should not be nil")
				return
			}
			if nodeEntry.Type != raft.CommandType_CLIENT_REGISTRATION {
				t.Errorf("The log entry for the client registration should have command type CLIENT_REGISTRATION; instead has %v", nodeEntry.Command)
				return
			}
			propagatedTo++
		}
	}

	quorum := (leader.Config.ClusterSize / 2) + 1

	if propagatedTo < quorum {
		t.Errorf("Leader responded with OK to client before propagating the log entry to a quorum of nodes (was only sent to %v)", propagatedTo)
		// Don't return here, since this doesn't affect future tests aside from the other propagation
	}

	// Test: Repeating same request with same session ID should return same result
	initialHash := []byte("hello")
	clientReq := raft.ClientRequest{
		ClientId:        result.ClientId,
		SequenceNum:     1,
		StateMachineCmd: hashmachine.HashChainInit,
		Data:            initialHash,
	}

	clientResult, _ := follower.ClientRequest(&clientReq)

	if clientResult.Status != raft.ClientStatus_NOT_LEADER {
		t.Errorf("Followers should return NOT_LEADER when they receive a ClientRequest; instead returned %v", clientResult.Status)
		return
	}

	if clientResult.LeaderHint.Id != leaderRemoteNode.Id {
		t.Errorf("Follower doesn't return correct leader address to ClientRequest. Should be %v, but is %v", leaderRemoteNode, result.LeaderHint)
		return
	}

	clientResult, _ = leader.ClientRequest(&clientReq)

	if clientResult.Status != raft.ClientStatus_OK {
		t.Errorf("Sending a ClientRequest to the leader should return OK; instead returned %v", clientResult.Status)
		return
	}

	initIndex := leader.LastLogIndex()

	propagatedTo = 0
	for _, node := range nodes {
		nodeIndex := node.LastLogIndex()
		if nodeIndex >= initIndex {
			nodeEntry := node.GetLog(initIndex)
			if nodeEntry == nil {
				t.Errorf("The log entry for the client's request should not be nil")
				return
			}
			if nodeEntry.Command != hashmachine.HashChainInit {
				t.Errorf("The log entry for the client's request should have command type HASH_CHAIN_INIT; instead has %v", nodeEntry.Command)
				return
			}
			propagatedTo++
		}
	}

	if propagatedTo < quorum {
		t.Errorf("Leader responded with OK to client before propagating the log entry to a quorum of nodes (was only sent to %v)", propagatedTo)
		// Don't return here, since this doesn't affect future tests
	}

	newClientResult, _ := leader.ClientRequest(&clientReq)

	if newClientResult.Status != raft.ClientStatus_OK {
		t.Errorf("Repeated ClientRequest to the leader should return OK; instead returned %v with response string \"%v\"", newClientResult.Status, newClientResult.Response)
		return
	}
}
