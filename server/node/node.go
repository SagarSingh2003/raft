package node

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	stateMachine "github.com/SagarSingh2003/Raft-KV/server/state_machine"
	"google.golang.org/grpc"

	pb "github.com/SagarSingh2003/Raft-KV/server/raft_proto"
)

type PersistLogT struct {
	Log          Log
	VotedFor     string
	VotedForTerm int
	Invalidate   bool
}

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
	Node_info []Node
}

type matchIndexStateT map[string]int

type nextIndexState map[string]int

type Server struct {
	pb.UnimplementedRaftServer
	// normal mutex because we can profile and if lock is a bottleneck then we switch to RWMutex
	Mu                    sync.Mutex
	StateCh               chan int
	commitIndexListenerCh chan struct{}

	HeartBeatTimer *time.Timer

	//check if there is an election taking place right now
	OngoingElection       bool
	CancelOngoingElection context.CancelFunc
	OngoingElectionTerm   int

	State       int
	currentTerm int
	votedFor    string
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex   nextIndexState
	matchIndex  matchIndexStateT

	node                Node
	peers               Peers
	peerLastContactTime map[string]time.Time
	leaderId            string
	logger              *slog.Logger
	Sm                  *stateMachine.StateMachine

	appendEntriesNotificationListener map[string]chan struct{}
	finishAppendEntriesCh             chan struct{}
	connCache                         map[string]*grpc.ClientConn

	logMu                 sync.Mutex
	lastLogIndexPersisted int
	byteOffset            int

	lastRecordedLogIndex int
	ClientOpListener     map[int]chan ClientOpResponse
	voteMap              map[string]bool
}

type NodeConfig struct {
	Address string `yaml:"address"`
	Id      string `yaml:"id"`
}

type ServerConfig struct {
	Nodes      []NodeConfig `yaml:"nodes"`
	TotalNodes int          `yaml:"totalNodes"`
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

func (s *Server) StartStateListener(sendHeartBeatsFn func(context.Context, string, string)) {

	for {
		state := <-s.StateCh

		s.Mu.Lock()
		s.State = state
		s.Mu.Unlock()

		switch state {
		case LEADER:
			s.Mu.Lock()
			s.leaderId = s.node.Id
			s.Mu.Unlock()

			fmt.Println("transitioned to leader ", s.leaderId, "term ", s.currentTerm)
			s.logger.Info("transitioned to leader", "nodeId", s.leaderId)

			// send heartbeats
			go s.SendHeartBeats(sendHeartBeatsFn)
			for _, node := range s.peers.Node_info {
				s.Mu.Lock()
				s.nextIndex[node.Id] = s.log[len(s.log)-1].LogIndex + 1
				s.Mu.Unlock()
			}
			go s.StartAppendEntriesWorkers()

		case FOLLOWER:

			close(s.finishAppendEntriesCh)
			s.finishAppendEntriesCh = make(chan struct{})
			fmt.Println("locking to reset leaderId")
			s.Mu.Lock()
			s.leaderId = ""
			s.Mu.Unlock()

			//Listen for heartbeat and trigger election on timeout

			fmt.Println("creatinng a new heartbeat Timer")
			if s.HeartBeatTimer == nil {
				s.HeartBeatTimer = time.NewTimer(GetRandomHeartBeatTimeout())

			} else {
				s.HeartBeatTimer.Reset(GetRandomHeartBeatTimeout())
			}

		case CANDIDATE:

			fmt.Println("converting to candidate......")

			s.incrementTerm()
			s.Mu.Lock()
			if s.votedFor != "" {
				s.StateCh <- FOLLOWER
				s.Mu.Unlock()
				continue
			}
			s.votedFor = s.node.Id
			s.leaderId = ""
			s.Mu.Unlock()

			//Start Election and Ask for vote from all nodes
			go s.StartElection(s.requestVote)

		case CloseListener:

			fmt.Println("closing listener")
			return

		default:

			s.logger.Info(fmt.Sprintf("StartStateListener -> unrecognized state %d ", state))

		}
	}

}

func StartNode(id *string, s **Server, configPath string) {

	config := getServerConfigFromYAML(configPath)

	self_address := ""

	peers := Peers{
		Node_info: []Node{},
	}

	for _, node := range config.Nodes {
		if node.Id == *id {
			self_address = node.Address
			continue
		}

		peers.Node_info = append(peers.Node_info, Node{
			Address: node.Address,
			Id:      node.Id,
		})
	}

	if self_address == "" {
		log.Fatalf("DEBUG:StartNode -> self address is empty")
	}

	f, err := os.OpenFile(fmt.Sprintf("%s.log", *id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	logger := slog.New(slog.NewJSONHandler(f, nil)).With("node", *id)

	server := ServerInitialize(peers, self_address, id)

	*s = server

	server.logger = logger

	go server.ListenForHeartBeatTimeouts()

	go func(s *Server) {
		for {
			<-s.commitIndexListenerCh

			s.ApplyLogsToSM()

		}
	}(server)

	lis, err := net.Listen("tcp", self_address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	fmt.Println("started listening on address : ", self_address)
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterRaftServer(grpcServer, server)

	// Start State Listener
	go server.StartStateListener(server.SendHeartBeat)
	server.StateCh <- FOLLOWER

	grpcServer.Serve(lis)

}

func ServerInitialize(peers Peers, self_address string, id *string) *Server {

	logs, votedFor, votedForTerm := replayLogs(*id)

	//TODO:add a condition here for snapshotting we shoud check if snapshot exists before entrering a sentinel
	if len(logs) == 0 {
		logs = []Log{
			{
				Term:     0,
				LogIndex: 0,
			},
		}
	}

	server := &Server{
		StateCh:               make(chan int),
		commitIndexListenerCh: make(chan struct{}),

		HeartBeatTimer:        time.NewTimer(GetRandomHeartBeatTimeout()),
		OngoingElection:       false,
		CancelOngoingElection: nil,
		OngoingElectionTerm:   0,

		State:       FOLLOWER,
		currentTerm: votedForTerm,
		votedFor:    votedFor,

		//TODO:Remove this sentinel corrupts log on restart
		log:         logs,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nextIndexState{},
		matchIndex:  matchIndexStateT{},

		node: Node{
			Address: self_address,
			Id:      *id,
		},

		peers:               peers,
		peerLastContactTime: make(map[string]time.Time),
		leaderId:            "",

		Sm:        InitializeStateMachine(),
		connCache: make(map[string]*grpc.ClientConn),

		lastLogIndexPersisted: -1,

		appendEntriesNotificationListener: make(map[string]chan struct{}, 2*len(peers.Node_info)),
		ClientOpListener:                  make(map[int]chan ClientOpResponse),
		finishAppendEntriesCh:             make(chan struct{}),

		voteMap: make(map[string]bool),
	}

	server.lastRecordedLogIndex = server.log[0].LogIndex

	for _, peer := range peers.Node_info {
		server.appendEntriesNotificationListener[peer.Id] = make(chan struct{})
	}
	return server
}

func InitializeStateMachine() *stateMachine.StateMachine {
	sm := stateMachine.StateMachine{
		State:            stateMachine.StateT{},
		LastAppliedIndex: 0,
		SmErrs:           make(map[string]bool),
	}
	return &sm
}

func (s *Server) ResetVotedFor() {
	s.Mu.Lock()
	s.votedFor = ""
	s.Mu.Unlock()
}

func (s *Server) GetId() string {
	return s.node.Id
}

func (s *Server) GetVotedFor() string {
	return s.votedFor
}

func (s *Server) GetTerm() int {
	return s.currentTerm
}

func (s *Server) getLogTermExplicitLock(logIdx int) int {

	logs := s.log
	for i, log := range logs {
		if log.LogIndex == logIdx {
			return i
		}
	}

	return -1
}

func (s *Server) initializeNextIndexExplicitLock() {

	for k := range s.nextIndex {
		s.nextIndex[k] = 0
	}
}

func (s *Server) initializeMatchIndexExplicitLock() {
	for k := range s.matchIndex {
		s.matchIndex[k] = 0
	}
}
