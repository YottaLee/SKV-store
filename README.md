# Raft

my_election_test.go

- TestVote_Leader1
  This test make sure the leader would behave properly when handling RequestVote, especially when leader handle competing RequestVote with stale term.

- TestVote_Leader2

  This test make sur  the leader would behave properly when handling RequestVote, especially when leader handle competing RequestVote with Higher Term and Out-of-date log

- TestVote_Leader3

  This test make sure the leader would behave properly when handling RequestVote, especially when leader handle competing RequestVote with Higher Term and Up-to-date log

- TestVote_Candidate1

  This test make sure the candidate would behave properly when handling RequestVote, especially when candidate handle competing RequestVote with stale term

- TestVote_Candidate2

  This test make sure the candidate would behave properly when handling RequestVote, especially when candidate handle competing RequestVote with Higher Term and Out-of-date log

- TestVote_Candidate3

  This test make sure the candidate would behave properly when handling RequestVote, especially when candidate handle competing RequestVote with Higher Term and Up-to-date log

- TestVote_Candidate4

  This test make sure the candidate would behave properly when handling RequestVote, especially when candidate receiving request from other candidate with higher term.



my_client_test.go

- TestClientInteraction_Oldleader

  This test make sure the leader would behave properly when handling ClientRequest, especially when a client registers itself to the old leader, and the client should receive a hint that this node is not a leader.

- TestClientInteraction_OldleaderFallBack

  This test make sure the leader would behave properly when handling ClientRequest, especially when a client registers itself to the old leader, and the old leader should fallback

- TestClientInteraction_Leader_DuplicateRequest

  This test make sure the leader would behave properly when handling ClientRequest, especially when leaders can register the client and process duplicate requests from clients properly

- TestClientInteraction_Follower

  This test make sure the follower would behave properly when handling ClientRequest, especially when the client registers itself to a follower, and the raft should return a hint to the client that this node is not a leader.

- TestClientInteraction_Candidate

  This test make sure the candidate would behave properly when handling ClientRequest, especially when the client registers itself to a candidate, and the raft should return a hint to the client that this node is not a leader.

- TestClientInteraction_Candidate_ClientRequest

  This test make sure the candidate would behave properly when handling ClientRequest, especially when a candidate deals with a regular client request, it should return with a reply that election is in process.

- TestClientInteractions

  This test make sure all nodes would behave properly when handling ClientRequest.



my_partition_test.go

- TestThreeWayPartition221

  This test make sure that raft will create network partitions in the specified proportion, with partition with 2 nodes, 2 nodes and 1 node.

- TestCandidateAndLeaderFallbackAfterPartition

  This test make sure that raft will behave properly, especially when a leader is partitioned and the leader should fallback to follower.

- TestLeaderFailsAndRejoins

  This test make sure that raft will behave properly, especially when a leader is partitioned then rejoins back.



my_append_entry_test.go

- This test make sure the follower would behave properly when handling AppendEntriesRequest with PrevLogIndex and PrevLogTerm not matching
