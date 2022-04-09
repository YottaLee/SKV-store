package test

import (
	"fmt"
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
