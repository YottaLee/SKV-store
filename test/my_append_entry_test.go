package test

import (
	"fmt"
	raft "raft/pkg"
	"testing"
	"time"
)

// Test AppendEntriesRequest with PrevLogIndex and PrevLogTerm not matching
func TestAppendEntriesLogTerm(t *testing.T) {
	config := raft.DefaultConfig()
	config.ClusterSize = 3
	cluster, _ := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(2 * time.Second)

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}

	followers := make([]*raft.Node, 0)
	for _, node := range cluster {
		if node != leader {
			followers = append(followers, node)
		}
	}

	prevLogIndex := leader.LastLogIndex()
	prevLogEntry := leader.GetLog(prevLogIndex)
	prevLogTerm := prevLogEntry.TermId
	request := raft.AppendEntriesRequest{
		Term:         leader.GetCurrentTerm(),
		Leader:       leader.Self,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm + 100,
		Entries:      nil,
		LeaderCommit: leader.CommitIndex.Load(),
	}

	reply, err := followers[0].Self.AppendEntriesRPC(leader, &request)

	if err != nil {
		t.Fatal(err)
	}

	if reply.Success != false && reply.Term != leader.GetCurrentTerm() {
		t.Fatal("AppendEntriesRequest should be rejected")
	}
}

// test sendHeartbeat in the doLeader
// follower term > leader term
func TestAppendEntriesLeader(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)
	if err != nil {
		t.Fatal(err)
	}

	// wait for a leader to be elected
	time.Sleep(time.Second * WaitPeriod)

	if err != nil {
		t.Fatal(err)
	}

	leader, err := findLeader(cluster)

	if err != nil {
		t.Fatal(err)
	}

	followers := make([]*raft.Node, 0)
	for i := 0; i < config.ClusterSize; i++ {
		node := cluster[i]
		if node.GetState() != raft.LeaderState {
			node.SetCurrentTerm(leader.GetCurrentTerm() + 10)
			followers = append(followers, node)
		}
	}

	//fmt.Println(followers[0].GetCurrentTerm())
	//fmt.Println(followers[1].GetCurrentTerm())

	request1 := raft.AppendEntriesRequest{
		Term:         leader.GetCurrentTerm(),
		Leader:       leader.Self,
		PrevLogIndex: followers[0].LastLogIndex(),
		PrevLogTerm:  followers[0].GetLog(followers[0].LastLogIndex()).GetTermId(),
		Entries:      nil,
		LeaderCommit: leader.CommitIndex.Load(),
	}

	request2 := raft.AppendEntriesRequest{
		Term:         leader.GetCurrentTerm(),
		Leader:       leader.Self,
		PrevLogIndex: followers[1].LastLogIndex(),
		PrevLogTerm:  followers[1].GetLog(followers[1].LastLogIndex()).GetTermId(),
		Entries:      nil,
		LeaderCommit: leader.CommitIndex.Load(),
	}

	reply1, err := followers[0].Self.AppendEntriesRPC(leader, &request1)
	reply2, err := followers[1].Self.AppendEntriesRPC(leader, &request2)
	fmt.Println(reply1.Term)
	fmt.Println(reply2.Term)
	fmt.Println(leader.GetCurrentTerm())

	if err != nil {
		t.Fatal(err)
	}

	if reply1.Success != false || reply1.Term == leader.GetCurrentTerm() {
		t.Fatal("AppendEntriesRequest should be rejected")
	}

	if reply2.Success != false || reply2.Term == leader.GetCurrentTerm() {
		t.Fatal("AppendEntriesRequest should be rejected")
	}

	newRequest := raft.AppendEntriesRequest{
		Term:         leader.GetCurrentTerm(),
		Leader:       leader.Self,
		PrevLogIndex: followers[0].LastLogIndex(),
		PrevLogTerm:  followers[0].GetLog(followers[0].LastLogIndex()).GetTermId(),
		Entries:      nil,
		LeaderCommit: leader.CommitIndex.Load(),
	}
	reply, err := followers[0].Self.AppendEntriesRPC(leader, &newRequest)
	fmt.Println(leader.GetCurrentTerm())
	if reply.Success != false {
		t.Fatal("AppendEntriesRequest should be rejected")
	}

	if leader.GetState() == raft.LeaderState {
		t.Fatalf("Leader should turn into follower state but is %s", leader.GetState())
	}
}
