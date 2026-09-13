package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	raftnode "github.com/SagarSingh2003/Raft-KV/server/node"

	pb "github.com/SagarSingh2003/Raft-KV/server/raft_proto"
)

func TestSendHeartBeatLeader(t *testing.T) {
	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	server.SetLeader(self_id)
	server.HeartBeatTimer = time.NewTimer(raftnode.GetRandomHeartBeatTimeout())

	heartBeatRecieved := make(chan string)

	SendHeartBeatStub := func(ctx context.Context, id string, address string) {
		heartBeatRecieved <- id
	}

	go server.SendHeartBeats(SendHeartBeatStub)

	visited := make(map[string]int)

	for visited["node2"] != 1 || visited["node3"] != 1 {
		fmt.Println("waiting for heartbeat")
		nodeId := <-heartBeatRecieved

		visited[nodeId] = 1
	}

	server.SetLeader("null")
}

func TestSendHeartBeatNonLeader(t *testing.T) {
	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	// server is not leader
	server.HeartBeatTimer = time.NewTimer(raftnode.GetRandomHeartBeatTimeout())

	heartBeatRecieved := make(chan string)

	SendHeartBeatStub := func(ctx context.Context, id string, address string) {
		heartBeatRecieved <- id
	}

	go server.SendHeartBeats(SendHeartBeatStub)

	select {
	case <-heartBeatRecieved:
		t.Fatal("non leader sent heartbeat")
	case <-time.After(5 * time.Second):
		t.Log("non leader did not send heartbeat")
	}
}

func TestHeartBeatTimeoutListener(t *testing.T) {
	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	server.HeartBeatTimer = time.NewTimer(raftnode.GetRandomHeartBeatTimeout())

	go func() {
		for {
			server.ResetHeartBeatTimer()
			time.Sleep(raftnode.HeartBeatSendIntervalMS * time.Millisecond)
		}
	}()

	go server.ListenForHeartBeatTimeouts()

	select {
	case state := <-server.StateCh:
		t.Fatal("State recieved : ", state)
	case <-time.After(5 * time.Second):
		t.Log("State did not change , currentState", server.State)
	}
	//send HeartBeat and the transition must not happen to Candidate

}

func TestRequestVote(t *testing.T) {

	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	//i. if no logs yet and term is greater then grant vote

	server.SetTerm(5)
	nodeCurrentTerm := 5

	server.ResetVotedFor()
	res, err := server.RequestVote(context.Background(), &pb.RequestVoteMessage{
		Term:         int32(nodeCurrentTerm),
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if err != nil {
		t.Fatal(err.Error())
	}

	if res.VoteGranted {
		t.Log("no logs term greater , vote granted")
	} else {
		t.Fatal("logs term greater , vote not granted")
	}

	// ii. if term is lesser then don't grant vote

	nodeCurrentTerm = 3

	server.ResetVotedFor()
	res, err = server.RequestVote(context.Background(), &pb.RequestVoteMessage{
		Term:         int32(nodeCurrentTerm),
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if err != nil {
		t.Fatal(err.Error())
	}

	if res.VoteGranted {
		t.Fatal(" no logs , term lesser , vote granted")
	} else {
		t.Log("no logs , term lesser , vote not granted")
	}

	// iii. Last log index does not match
	server.SetLogs([]raftnode.Log{
		{Term: 0, LogIndex: 0},
		{Term: 1, LogIndex: 1},
		{Term: 1, LogIndex: 2},
		{Term: 1, LogIndex: 3},
		{Term: 2, LogIndex: 4},
	})
	server.SetTerm(2)
	nodeCurrentTerm = 2

	server.ResetVotedFor()
	res, err = server.RequestVote(context.Background(), &pb.RequestVoteMessage{
		Term:         int32(nodeCurrentTerm),
		CandidateId:  "node2",
		LastLogIndex: 3,
		LastLogTerm:  1,
	})

	if err != nil {
		t.Fatal(err.Error())
	}

	if res.VoteGranted {
		t.Fatal("logs don't match , vote granted")
	} else {
		t.Log("logs dont match , vote not granted")
	}

	// iv. logs match vote granted

	server.ResetVotedFor()
	req := &pb.RequestVoteMessage{
		Term:         int32(nodeCurrentTerm),
		CandidateId:  "node2",
		LastLogIndex: 4,
		LastLogTerm:  2,
	}

	res, err = server.RequestVote(context.Background(), req)

	if err != nil {
		t.Fatal(err.Error())
	}

	if res.VoteGranted {
		t.Log("logs match , vote granted")
	} else {
		t.Fatal("logs match , vote not granted", " ", req, "nodeCurrentTerm : ", nodeCurrentTerm)

	}

	// empty logs edge case , term match

	server.SetLogs([]raftnode.Log{
		{Term: 0, LogIndex: 0},
	})

	server.ResetVotedFor()
	res, err = server.RequestVote(context.Background(), &pb.RequestVoteMessage{
		Term:         int32(nodeCurrentTerm),
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if err != nil {
		t.Fatal(err.Error())
	}

	if res.VoteGranted {
		t.Log("empty logs match , vote granted")
	} else {
		t.Fatal("empty logs match , vote not granted")
	}

	// if already self voted , should not vote for that term again
	server.SetLogs([]raftnode.Log{
		{Term: 0, LogIndex: 0},
	})

	server.ResetVotedFor()
	server.SetVotedFor(self_id)
	res, err = server.RequestVote(context.Background(), &pb.RequestVoteMessage{
		Term:         int32(nodeCurrentTerm),
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if res.VoteGranted {
		t.Fatal("already voted still vote granted")
	} else {
		t.Log("Already voted so vote not granted")
	}

	//if voted for someone else , should not vote for that term again
	server.SetLogs([]raftnode.Log{
		{Term: 0, LogIndex: 0},
	})

	server.ResetVotedFor()
	server.SetVotedFor("node3")
	res, _ = server.RequestVote(context.Background(), &pb.RequestVoteMessage{
		Term:         int32(nodeCurrentTerm),
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if res.VoteGranted {
		t.Fatal("already voted still vote granted")
	} else {
		t.Log("Already voted so vote not granted")
	}
}

func TestElection(t *testing.T) {
	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	//1. stateCh will recieve Leader
	requestVoteFn := func(ctx context.Context, peerAddress string, voteCh chan int, id string) {
		voteCh <- 1
	}

	go server.StartElection(requestVoteFn)

	value := <-server.StateCh

	if value == raftnode.LEADER {
		t.Log("successfully converted to leader")
	} else {
		t.Fatal("did not convert to leader", value)
	}

	// ii. if votes rejected then it must convert to follower
	rejectVoteFn := func(ctx context.Context, peerAddress string, voteCh chan int, id string) {
		voteCh <- 0
	}
	go server.StartElection(rejectVoteFn)

	value = <-server.StateCh

	if value == raftnode.FOLLOWER {
		t.Log("successfully converted to follower")
	} else {
		t.Fatal("did not convert to follower , state : ", value)
	}

	// iii. cancelOngoingFunction should cancel the election and convert to follower
	stallVoteFn := func(ctx context.Context, _ string, VoteCh chan int, id string) {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("**stalling ended**")
				return
			default:
				fmt.Println("still stalling")
			}
		}
	}

	go server.StartElection(stallVoteFn)

	time.Sleep(1 * time.Second)

	go server.CancelOngoingElection()

	value = <-server.StateCh

	if value == raftnode.FOLLOWER {
		t.Log("successfully converted to follower")
	} else {
		t.Fatal("did not get converted to follower state : ", value)
	}
}

func TestSendHeartBeat(t *testing.T) {

	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	server.SetTerm(3)
	res, err := server.AppendEntries(context.TODO(), &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []string{},
		LeaderCommit: 0,
	})

	if err != nil {
		t.Fatal(err)
	}

	if res.Success {
		t.Log("successfully acknowledged heartbeat")
	} else {
		t.Fatal("did not acknowledge heartbeat")
	}
}

func TestElectionE2E(t *testing.T) {
	node1 := "node1"
	node2 := "node2"
	node3 := "node3"
	node4 := "node4"
	node5 := "node5"

	leader_ch := make(chan string, 5)
	follower_ch := make(chan string, 5)

	server1 := &raftnode.Server{}
	server2 := &raftnode.Server{}
	server3 := &raftnode.Server{}
	server4 := &raftnode.Server{}
	server5 := &raftnode.Server{}

	go func(server **raftnode.Server) {

		go func() {

			is_leader := false
			for {
				value := (*server).State
				if value == raftnode.LEADER {
					if !is_leader {
						is_leader = true
						leader_ch <- "node1"
					}
				} else if value == raftnode.FOLLOWER {
					if is_leader {
						follower_ch <- "node1"
						is_leader = false
					}
				}
			}

		}()

		raftnode.StartNode(&node1, server, "../../config.yaml")

	}(&server1)

	go func(server **raftnode.Server) {

		go func() {
			is_leader := false
			for {
				value := (*server).State

				if value == raftnode.LEADER {
					if !is_leader {
						is_leader = true
						leader_ch <- "node2"
					}
				} else if value == raftnode.FOLLOWER {
					if is_leader {
						follower_ch <- "node2"
						is_leader = false
					}
				}
			}

		}()
		raftnode.StartNode(&node2, server, "../../config.yaml")
	}(&server2)

	go func(server **raftnode.Server) {

		go func() {

			is_leader := false
			for {
				value := (*server).State

				if value == raftnode.LEADER {
					if !is_leader {
						is_leader = true
						leader_ch <- "node3"
					}
				} else if value == raftnode.FOLLOWER {
					if is_leader {
						follower_ch <- "node3"
						is_leader = false
					}
				}
			}

		}()

		raftnode.StartNode(&node3, server, "../../config.yaml")
	}(&server3)

	go func(server **raftnode.Server) {

		go func() {

			is_leader := false
			for {
				value := (*server).State

				if value == raftnode.LEADER {
					if !is_leader {
						is_leader = true
						leader_ch <- "node4"
					}
				} else if value == raftnode.FOLLOWER {
					if is_leader {
						follower_ch <- "node4"
						is_leader = false
					}
				}
			}

		}()

		raftnode.StartNode(&node4, server, "../../config.yaml")
	}(&server4)

	go func(server **raftnode.Server) {

		go func() {

			is_leader := false
			for {
				value := (*server).State

				if value == raftnode.LEADER {
					if !is_leader {
						is_leader = true
						leader_ch <- "node5"
					}
				} else if value == raftnode.FOLLOWER {
					if is_leader {
						follower_ch <- "node5"
						is_leader = false
					}
				}
			}

		}()
		raftnode.StartNode(&node5, server, "../../config.yaml")
	}(&server5)

	fmt.Println("reached checkpoint 1...")
	leader_map := make(map[string]bool)

	breakLoop := false

	electionCh := make(chan *raftnode.Server, 5)

	type Election struct {
		startTime    time.Time
		endTime      time.Time
		startedBy    string
		nodeTerm     int
		peerState    map[string]int
		peerTerm     map[string]int
		peerVotedFor map[string]string
	}

	var elections []Election
	go func(n1, n2, n3, n4, n5 **raftnode.Server) {

		//record election
		watchForElections := func(n **raftnode.Server, nodeId string) {
			wasOngoingElection := false
			for {
				(*n).Mu.Lock()
				ongoingElection := (*n).OngoingElection
				(*n).Mu.Unlock()

				if ongoingElection && !wasOngoingElection {
					electionCh <- *n
				}

				wasOngoingElection = ongoingElection
				time.Sleep(10 * time.Millisecond)
			}
		}

		recordElection := func(main, n2, n3, n4, n5 **raftnode.Server) {
			election := Election{
				startTime: time.Now(),
				startedBy: (*main).GetId(),
				nodeTerm:  (*main).GetTerm(),
			}
			var mu sync.Mutex
			election.peerVotedFor = make(map[string]string)
			watchForVotes := func(n **raftnode.Server) {
				for {
					if val := (*n).GetVotedFor(); val != "" {
						mu.Lock()
						election.peerVotedFor[(*n).GetId()] = val
						mu.Unlock()
						return
					}
				}
			}

			go watchForVotes(n2)
			go watchForVotes(n3)
			go watchForVotes(n4)
			go watchForVotes(n5)

			election.peerState = make(map[string]int)
			election.peerTerm = make(map[string]int)

			election.peerState[(*n2).GetId()] = (*n2).State
			election.peerState[(*n3).GetId()] = (*n3).State
			election.peerState[(*n4).GetId()] = (*n4).State
			election.peerState[(*n5).GetId()] = (*n5).State

			election.peerTerm[(*n2).GetId()] = (*n2).GetTerm()
			election.peerTerm[(*n3).GetId()] = (*n3).GetTerm()
			election.peerTerm[(*n4).GetId()] = (*n4).GetTerm()
			election.peerTerm[(*n5).GetId()] = (*n5).GetTerm()

			for (*main).OngoingElection {
				time.Sleep(5 * time.Millisecond)
			}
			election.endTime = time.Now()
			fmt.Println("election record : ", election)
			elections = append(elections, election)
		}

		go watchForElections(n1, "node1")
		go watchForElections(n2, "node2")
		go watchForElections(n3, "node3")
		go watchForElections(n4, "node4")
		go watchForElections(n5, "node5")

		fmt.Println("reached checkpoint 4....")
		for {
			node := <-electionCh

			fmt.Println("election started")
			// when did the election start!
			// when did the election finish!
			// what was the state of other nodes during this node's election
			// what was the term of each node
			switch node {
			case *n1:
				go recordElection(&node, n2, n3, n4, n5)
			case *n2:
				go recordElection(&node, n1, n3, n4, n5)
			case *n3:
				go recordElection(&node, n1, n2, n4, n5)
			case *n4:
				go recordElection(&node, n1, n2, n3, n5)
			case *n5:
				go recordElection(&node, n1, n2, n3, n4)
			}

		}

	}(&server1, &server2, &server3, &server4, &server5)

	for {

		if len(leader_map) >= 1 {

			fmt.Println("reached checkpoint 2...")
			fmt.Println("leadermap**", leader_map)
			time.Sleep(10 * time.Second)
			breakLoop = true
		}

		select {
		case node := <-leader_ch:
			leader_map[node] = true
		case node := <-follower_ch:
			delete(leader_map, node)
		default:
		}

		if breakLoop {
			fmt.Println("reached checkpoint 3...")
			break
		}
	}

	fmt.Println("elections : ", elections)
	if len(leader_map) > 1 {
		t.Fatal("more than one leader", leader_map)
	} else if len(leader_map) == 1 {
		t.Log("excatly one leader", leader_map)
	} else {
		t.Fatal("no leader elected", leader_map)
	}
	// i need to check if two leaders happen in any scenario , whenever some node transitions to leader we should know we run until we encounter the first leader , then we listen for a buffer of 10 seconds to check if another node transitions into the leader wihtout the first node returning back to the follower state

}

func TestAppendEntries(t *testing.T) {

	//i. if logs match exceed with new values

	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	server.SetLogs(
		[]raftnode.Log{
			{
				Term:     0,
				LogIndex: 0,
			},
			{
				Term:     1,
				LogIndex: 1,
			},
			{
				Term:     2,
				LogIndex: 2,
			},
		},
	)

	server.SetTerm(2)

	res, err := server.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []string{
			"add,default,x,2,,2,3",
			"delete,default,x,,,3,3",
		},
		LeaderCommit: 1,
	})

	if err != nil {
		t.Fatal("ERROR occured : ", err.Error())
	}

	if res.Success == false {
		t.Fatal("should have succeded", server.GetLogs())
	} else {
		t.Log("successfully propagated logs", server.GetLogs())
	}

	// ii. if logs don't match
	res, err = server.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "node2",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []string{
			"add,default,x,2,,2,2",
			"delete,default,x,,,3,3",
		},
		LeaderCommit: 1,
	})

	if err != nil {
		t.Fatal("ERROR occured : ", err.Error())
	}

	if res.Success {
		t.Fatal("it should have failed , the logs don't match")
	} else {
		t.Log("successfully responded as false as logs don't match")
	}

}

func TestWal(t *testing.T) {
	//i. if logs are sent to appendEntries , the wal must persist the state

	self_id := "node1"
	self_address := "localhost:3000"
	server := raftnode.ServerInitialize(raftnode.Peers{
		Node_info: []raftnode.Node{
			{Id: "node2",
				Address: "localhost:3001",
			},
			{Id: "node3",
				Address: "localhost:3002",
			},
		},
	}, self_address, &self_id, raftnode.InitializeStateMachine())

	server.SetLogs(
		[]raftnode.Log{
			{
				Term:     0,
				LogIndex: 0,
			},
		},
	)

	server.SetTerm(1)

	res, err := server.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []string{
			"add,default,x,2,,1,1",
		},
		LeaderCommit: 0,
	})

	if err != nil {
		t.Fatal("ERROR occured : ", err.Error())
	}

	res, err = server.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []string{
			"add,default,x,2,,2,2",
		},
		LeaderCommit: 1,
	})

	if err != nil {
		t.Fatal("ERROR occured : ", err.Error())
	}

	res, err = server.AppendEntries(context.Background(), &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "node2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []string{
			"add,default,x,2,,2,3",
			"delete,default,x,,,3,3",
		},
		LeaderCommit: 1,
	})

	if err != nil {
		t.Fatal("ERROR occured : ", err.Error())
	}

	if res.Success == false {
		t.Fatal("should have succeded", server.GetLogs())
	} else {
		t.Log("successfully propagated logs", server.GetLogs())
	}

}
