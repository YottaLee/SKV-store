package test

import (
	"fmt"
	raft "raft/pkg"
	"raft/pkg/hashmachine"
	"testing"
	"time"
)

//the client registers itself to the old leader, and the raft should return a hint to the
//client that this node is not a leader.
func TestClientInteraction_oldleader(t *testing.T) {
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

//the client registers itself to a caldidate, and the raft should return a hint to the
//client that this node is not a leader.
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
	//reply, _ := leader.RegisterClientCaller(context.Background(), &RegisterClientRequest{})
	//
	//if reply.Status != raft.ClientStatus_OK {
	//	t.Errorf("%v", reply.Status)
	//	t.Fatal("We don't have a leader yet")
	//}
	logsMatch(leader, cluster)
}
