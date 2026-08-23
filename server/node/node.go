package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	stateMachine "github.com/SagarSingh2003/Raft-KV/server/state_machine"
)

type Log struct {
	Term      int
	Operation string
	Namespace string
	Key       string
	Value     string
	CasValue  string
	LogIndex  int
}

type Node struct {
	Id      string
	Address string
}

type Peers struct {
	node_info []Node
}

type matchIndexStateT struct {
	node_info  Node
	matchIndex int
}

type nextIndexState struct {
	node_info Node
	nextIndex int
}

type Server struct {
	// normal mutex because we can profile and if lock is a bottleneck then we switch to RWMutex
	Mu      sync.Mutex
	StateCh chan int

	//timer for the whole election to take place
	electionTimer  time.Timer
	heartBeatTimer time.Timer

	//check if there is an election taking place right now
	ongoingElection       bool
	cancelOngoingElection context.CancelFunc
	ongoingElectionTerm   int

	State       int
	currentTerm int
	votedFor    string
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex   []nextIndexState
	matchIndex  []matchIndexStateT

	node                Node
	peers               Peers
	peerLastContactTime map[string]time.Time
	leaderId            string

	Sm stateMachine.StateMachine
}

func (s *Server) GetLogs() []Log {
	return s.log
}

func (s *Server) GetCommitIdx() int {
	return s.commitIndex
}

func (s *Server) GetLastApplied() int {
	return s.lastApplied
}

func isIncomingTermGreater(incoming_term int, currentTerm int) bool {
	return incoming_term > currentTerm
}

func (s *Server) StartStateListener() {

	for {
		state := <-s.StateCh

		s.Mu.Lock()
		s.State = state
		s.Mu.Unlock()

		switch state {
		case LEADER:

			// send heartbeats
			s.SendHeartBeats()

		case FOLLOWER:

			//Listen for heartbeat and trigger election on timeout
			s.ResetHeartBeatTimer()
			s.ListenForHeartBeatTimeouts()

		case CANDIDATE:

			s.incrementTerm()
			//Start Election and Ask for vote from all nodes
			s.StartElection()

		case CloseListener:

			return
		default:

			slog.Info(fmt.Sprintf("StartStateListener -> unrecognized state %s ", state))

		}
	}

}
