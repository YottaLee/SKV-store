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
	follower, _ := findFollower(cluster)

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
	follower, _ := findFollower(cluster)

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
	follower, nil := findFollower(cluster)

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
	follower, _ = findFollower(cluster)

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

// Test making sure candidate would behave correctly when handling RequestVote, Handle competing RequestVote with Stale Term
func TestVote_Candidate1(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 5

	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(WaitPeriod * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
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
	time.Sleep(1 * time.Second)

	followers, err := findAllFollowers(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if followers[0].GetCurrentTerm() != 3 {
		t.Fatalf("Term should've changed to %d but is %d", 3, followers[0].GetCurrentTerm())
	}

	followers[1].SetCurrentTerm(3)
	followers[1].Config.ElectionTimeout = 1 * time.Second
	followers[3].NetworkPolicy.PauseWorld(true)
	followers[2].NetworkPolicy.PauseWorld(true)
	leader.NetworkPolicy.PauseWorld(true)

	time.Sleep(500 * time.Millisecond)
	reply, _ := followers[0].RequestVoteCaller(context.Background(), &raft.RequestVoteRequest{
		Term:         uint64(1),
		Candidate:    followers[1].Self,
		LastLogIndex: uint64(3),
		LastLogTerm:  uint64(1),
	})
	if reply.VoteGranted {
		t.Fatal("Should've denied vote")
	}
}

// Test making sure candidate would behave correctly when handling RequestVote, Handle competing RequestVote with Higher Term and Out-of-date log
func TestVote_Candidate2(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 5

	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(WaitPeriod * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
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
	time.Sleep(1 * time.Second)

	followers, err := findAllFollowers(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if followers[0].GetCurrentTerm() != 3 {
		t.Fatalf("Term should've changed to %d but is %d", 3, followers[0].GetCurrentTerm())
	}

	followers[1].SetCurrentTerm(3)
	followers[1].Config.ElectionTimeout = 1 * time.Second
	followers[3].NetworkPolicy.PauseWorld(true)
	followers[2].NetworkPolicy.PauseWorld(true)
	leader.NetworkPolicy.PauseWorld(true)

	time.Sleep(500 * time.Millisecond)
	reply, _ := followers[0].RequestVoteCaller(context.Background(), &raft.RequestVoteRequest{
		Term:         uint64(100),
		Candidate:    followers[1].Self,
		LastLogIndex: uint64(1),
		LastLogTerm:  uint64(1),
	})
	if reply.VoteGranted {
		t.Fatal("Should've denied vote")
	}
}

// Test making sure candidate would behave correctly when handling RequestVote, Handle competing RequestVote with Higher Term and Up-to-date log
func TestVote_Candidate3(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 5

	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(WaitPeriod * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
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
	time.Sleep(1 * time.Second)

	followers, err := findAllFollowers(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if followers[0].GetCurrentTerm() != 3 {
		t.Fatalf("Term should've changed to %d but is %d", 3, followers[0].GetCurrentTerm())
	}

	followers[1].SetCurrentTerm(3)
	followers[1].Config.ElectionTimeout = 1 * time.Second
	followers[3].NetworkPolicy.PauseWorld(true)
	followers[2].NetworkPolicy.PauseWorld(true)
	leader.NetworkPolicy.PauseWorld(true)

	time.Sleep(500 * time.Millisecond)
	reply, _ := followers[0].RequestVoteCaller(context.Background(), &raft.RequestVoteRequest{
		Term:         uint64(200),
		Candidate:    followers[1].Self,
		LastLogIndex: uint64(2),
		LastLogTerm:  uint64(3),
	})
	if !reply.VoteGranted {
		t.Fatal("Should've granted vote")
	}
}

func TestVote_Candidate4(t *testing.T) {
	suppressLoggers()
	config := raft.DefaultConfig()
	config.ClusterSize = 5

	cluster, err := raft.CreateLocalCluster(config)
	defer cleanupCluster(cluster)

	time.Sleep(WaitPeriod * time.Second)

	if err != nil {
		t.Fatal(err)
	}

	leader, err := findLeader(cluster)
	if err != nil {
		t.Fatal(err)
	}
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
	time.Sleep(1 * time.Second)

	followers, err := findAllFollowers(cluster)
	if err != nil {
		t.Fatal(err)
	}
	if followers[0].GetCurrentTerm() != 3 {
		t.Fatalf("Term should've changed to %d but is %d", 3, followers[0].GetCurrentTerm())
	}
	followers[0].SetCurrentTerm(8)
	followers[1].SetCurrentTerm(3)
	followers[1].Config.ElectionTimeout = 1 * time.Second
	//followers[3].NetworkPolicy.PauseWorld(true)
	//followers[2].NetworkPolicy.PauseWorld(true)
	leader.NetworkPolicy.PauseWorld(true)

	time.Sleep(500 * time.Millisecond)
	reply, _ := followers[0].RequestVoteCaller(context.Background(), &raft.RequestVoteRequest{
		Term:         uint64(4),
		Candidate:    followers[1].Self,
		LastLogIndex: uint64(3),
		LastLogTerm:  uint64(1),
	})
	if reply.VoteGranted {
		t.Fatal("Should've denied vote")
	}
}
