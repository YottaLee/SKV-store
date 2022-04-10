package test

import (
	"fmt"
	"math"
	raft "raft/pkg"
	"testing"
	"time"
)

func TestThreeWayPartition311(t *testing.T) {
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
	followerTerm := follower.GetCurrentTerm()
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

	partitionedFollowerTerm := follower.GetCurrentTerm()
	if partitionedFollowerTerm <= followerTerm {
		t.Errorf("The term of the partitioned follower, which is now a candidate, should have increased")
		return
	}
}

//*
func TestThreeWayPartition221(t *testing.T) {
	suppressLoggers()
	nodes, err := createTestCluster([]int{5011, 5012, 5013, 5014, 5015})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Shutdown all nodes once finished
	defer cleanupCluster(nodes)
	time.Sleep(time.Second * WaitPeriod) // Long enough for timeout+election

	leader, err := findLeader(nodes)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}
	leaderTerm := leader.GetCurrentTerm()

	// Partition off leader first node
	leader.NetworkPolicy.PauseWorld(true)

	followers := make([]*raft.Node, 0)
	for _, node := range nodes {
		if node != leader {
			followers = append(followers, node)
		}
	}

	// Partition second, third node into their own cluster, leaves fourth and fifth in their own cluster as well
	followers[0].NetworkPolicy.RegisterPolicy(followers[0].Self, followers[2].Self, false)
	followers[1].NetworkPolicy.RegisterPolicy(followers[1].Self, followers[2].Self, false)
	followers[0].NetworkPolicy.RegisterPolicy(followers[0].Self, followers[3].Self, false)
	followers[1].NetworkPolicy.RegisterPolicy(followers[1].Self, followers[3].Self, false)
	followers[2].NetworkPolicy.RegisterPolicy(followers[2].Self, followers[0].Self, false)
	followers[3].NetworkPolicy.RegisterPolicy(followers[3].Self, followers[0].Self, false)
	followers[2].NetworkPolicy.RegisterPolicy(followers[2].Self, followers[1].Self, false)
	followers[3].NetworkPolicy.RegisterPolicy(followers[3].Self, followers[1].Self, false)

	time.Sleep(time.Second * 2) // Wait, just in case the partitioned leader attempts anything

	// Test: Make sure that the original leader remains a leader post-partition
	if leader.GetState() != raft.LeaderState {
		t.Errorf("Leader should remain leader even when partitioned off")
		return
	}

	// Test: None of the other nodes becomes a leader
	for _, node := range followers {
		if node.GetState() == raft.LeaderState {
			t.Errorf("Node (%v) has become a leader - should remain a follower or be a candidate", node.Self.Id)
		}
	}

	pausedLeaderTerm := leader.GetCurrentTerm()

	if leaderTerm != pausedLeaderTerm {
		t.Errorf("Leader's term should remain the same after partition. Went from %v to %v", leaderTerm, pausedLeaderTerm)
		return
	}
}

func TestCandidateAndLeaderFallbackAfterPartition(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 5
	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	followers := make([]*raft.Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
			node.NetworkPolicy.PauseWorld(true)
		}
	}

	superFollower := followers[0]

	// Wait followers to become candidates and increase their terms
	time.Sleep(time.Second * WaitPeriod)

	if leader.GetState() != raft.LeaderState {
		t.Errorf("Leader's current state should remain to be a leader")
		return
	}

	// Bring back superfollower first
	superFollower.NetworkPolicy.PauseWorld(false)

	if superFollower.GetCurrentTerm() <= leader.GetCurrentTerm() {
		t.Errorf("Leader's current term should be smaller than the rejoined follower's")
		return
	}

	// Wait for superfollower turns leader back to a follower
	time.Sleep(time.Second * WaitPeriod)

	if leader.GetState() == raft.LeaderState {
		t.Errorf("Old leader should fallback to a follower")
		return
	}

	// Set superfollower's term to a large number
	superFollower.SetCurrentTerm(uint64(math.MaxUint64 / 2))

	// Bring back all other partitioned followers
	for _, node := range followers {
		if node != superFollower {
			node.NetworkPolicy.PauseWorld(false)
		}
	}

	// Wait until a new leader is elected
	time.Sleep(time.Second * WaitPeriod)

	newLeader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	if newLeader.GetCurrentTerm() < uint64(math.MaxUint64/2) {
		t.Errorf("New leader's term should be greater or equal to the large term we set")
		return
	}
}

// Leader is partitioned then rejoins back
func TestLeaderFailsAndRejoins(t *testing.T) {
	suppressLoggers()

	config := raft.DefaultConfig()
	config.ClusterSize = 5
	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)
	leader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	followers := make([]*raft.Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
		}
	}

	leader.NetworkPolicy.PauseWorld(true)

	// Wait until a new leader is selected in among the remaining four followers
	time.Sleep(time.Second * WaitPeriod)

	// Old leader should remain leader
	if leader.GetState() != raft.LeaderState {
		t.Errorf("Old leader should remain leader after partition")
		return
	}

	newLeaders := make([]*raft.Node, 0)

	for _, follower := range followers {
		if follower.GetState() == raft.LeaderState {
			newLeaders = append(newLeaders, follower)
		}
	}

	// There should be only one new leader
	if len(newLeaders) != 1 {
		t.Errorf("The number of new leader is not correct")
		return
	}

	newLeader := newLeaders[0]
	newLeaderTerm := newLeader.GetCurrentTerm()
	// New leader's term shoud be greater than old leader's term
	if newLeader.GetCurrentTerm() <= leader.GetCurrentTerm() {
		t.Errorf("New leader's term shoud be greater than old leader's term")
		return
	}

	// Add a new log entry to the new leader; SHOULD be replicated
	newLeader.LeaderMutex.Lock()
	logEntry := &raft.LogEntry{
		Index:  newLeader.LastLogIndex() + 1,
		TermId: newLeader.GetCurrentTerm(),
		Type:   raft.CommandType_NOOP,
		Data:   []byte{5, 6, 7, 8},
	}
	newLeader.StoreLog(logEntry)
	newLeader.LeaderMutex.Unlock()

	// Wait until replicates to propagate
	time.Sleep(time.Second * WaitPeriod)

	// Leader rejoins
	leader.NetworkPolicy.PauseWorld(false)

	// Wait until stablized
	time.Sleep(time.Second * WaitPeriod)

	rejoinedLeader, err := findLeader(cluster)
	if err != nil {
		t.Errorf("Failed to find leader: %v", err)
		return
	}

	if leader.GetState() != raft.FollowerState {
		t.Errorf("Old leader should become a follower after rejoining back. leader's state is %v", leader.GetState())
		return
	}

	if leader.GetCurrentTerm() != rejoinedLeader.GetCurrentTerm() {
		t.Errorf("Old leader node should have the same term as the current leader")
		return
	}

	if leader.LastLogIndex() != rejoinedLeader.LastLogIndex() {
		t.Errorf("Old leader node should have the same last log index as the current leader")
		return
	}

	if rejoinedLeader != newLeader {
		t.Errorf("Leader after rejoining should be newLeader")
	}

	if rejoinedLeader.GetCurrentTerm() != newLeaderTerm {
		t.Errorf("The term of leader after rejoining should be the same as the newLeader")
		return
	}
}
