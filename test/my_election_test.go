package test

import (
	"golang.org/x/net/context"
	raft "raft/pkg"
	"testing"
	"time"
)

// Test making sure leader would behave correctly when handling RequestVote, Leader handle competing RequestVote with Stale Term
func TestVote_Leader1(t *testing.T) {
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

	leader.SetCurrentTerm(3)
	leader.LeaderMutex.Lock()
	logEntry := &raft.LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   raft.CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.StoreLog(logEntry)
	leader.LeaderMutex.Unlock()

	time.Sleep(500 * time.Millisecond)
	reply, _ := leader.RequestVoteCaller(context.Background(), &raft.RequestVoteRequest{
		Term:         uint64(1),
		Candidate:    follower.Self,
		LastLogIndex: uint64(3),
		LastLogTerm:  uint64(1),
	})
	if reply.VoteGranted {
		t.Fatal("Should've denied vote")
	}
	if leader.GetState() != raft.LeaderState {
		t.Fatalf("Leader should've stayed in leader state but is %s", leader.GetState())
	}
}

// Test making sure leader would behave correctly when handling RequestVote, Leader handle competing RequestVote with Higher Term and Out-of-date log
func TestVote_Leader2(t *testing.T) {
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

	leader.SetCurrentTerm(3)
	leader.LeaderMutex.Lock()
	logEntry := &raft.LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   raft.CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.StoreLog(logEntry)
	leader.LeaderMutex.Unlock()

	time.Sleep(500 * time.Millisecond)
	reply, _ := leader.RequestVoteCaller(context.Background(), &raft.RequestVoteRequest{
		Term:         uint64(100),
		Candidate:    follower.Self,
		LastLogIndex: uint64(1),
		LastLogTerm:  uint64(1),
	})
	if reply.VoteGranted {
		t.Fatal("Leader should've denied vote")
	}
}

// Test making sure leader would behave correctly when handling RequestVote, Handle competing RequestVote with Higher Term and Up-to-date log
func TestVote_Leader3(t *testing.T) {
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

	leader.SetCurrentTerm(3)
	leader.LeaderMutex.Lock()
	logEntry := &raft.LogEntry{
		Index:  leader.LastLogIndex() + 1,
		TermId: leader.GetCurrentTerm(),
		Type:   raft.CommandType_NOOP,
		Data:   []byte{1, 2, 3, 4},
	}
	leader.StoreLog(logEntry)
	leader.LeaderMutex.Unlock()

	time.Sleep(2 * time.Second)
	leader, err = findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
	follower = findFollower(cluster)

	reply, _ := leader.RequestVoteCaller(context.Background(), &raft.RequestVoteRequest{
		Term:         uint64(200),
		Candidate:    follower.Self,
		LastLogIndex: uint64(3),
		LastLogTerm:  uint64(150),
	})
	if !reply.VoteGranted {
		t.Fatal("Leader should've granted vote")
	}

}
