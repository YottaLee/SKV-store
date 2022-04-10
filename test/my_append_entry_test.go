package test

import (
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
