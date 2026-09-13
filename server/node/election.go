package node

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/SagarSingh2003/Raft-KV/server/raft_proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type VoteT struct {
	ElectionTerm int
	votedBy      string
	voteVal      int
}

func (s *Server) RequestVote(ctx context.Context, in *pb.RequestVoteMessage) (*pb.RequestVoteResponse, error) {

	s.Mu.Lock()
	defer s.Mu.Unlock()

	currentTerm := s.currentTerm

	res := &pb.RequestVoteResponse{
		Term:        int32(currentTerm),
		VoteGranted: false,
	}

	if in.Term < int32(currentTerm) {

		res.Term = int32(currentTerm)
		res.VoteGranted = false

		fmt.Println("rejecting vote because the incoming term is lesser")
		return res, nil
	}

	if in.Term > int32(currentTerm) {
		s.currentTerm = int(in.Term)
		currentTerm = int(in.Term)
		s.votedFor = ""

		//Convert to FOLLOWER if not FOLLOWER
		if s.State != FOLLOWER {
			s.State = FOLLOWER
			close(s.finishAppendEntriesCh)
			s.finishAppendEntriesCh = make(chan struct{})
			s.leaderId = ""

			if s.HeartBeatTimer == nil {
				s.HeartBeatTimer = time.NewTimer(GetRandomHeartBeatTimeout())

			} else {
				s.HeartBeatTimer.Reset(GetRandomHeartBeatTimeout())
			}
		}

	}

	logs_correct := s.CheckLogCorrectness(int(in.LastLogIndex), int(in.LastLogTerm))

	if s.votedFor == "" && logs_correct {

		res.Term = int32(currentTerm)
		res.VoteGranted = true
		s.votedFor = in.CandidateId
		return res, nil

	} else {

		res.Term = int32(currentTerm)
		res.VoteGranted = false

		return res, nil
	}

}

// must be locked before usage
func (s *Server) CheckLogCorrectness(incomingLastLogIndex int, incomingLastLogTerm int) bool {

	// when the election happens for the first time there are no logs
	if incomingLastLogIndex == 0 && incomingLastLogTerm == 0 {
		return true
	}

	lastLog := s.log[len(s.log)-1]
	nodeLastLogIndex := lastLog.LogIndex

	if incomingLastLogIndex == nodeLastLogIndex && s.log[incomingLastLogIndex].Term == incomingLastLogTerm {
		return true
	}

	return false
}

func (s *Server) finishElectionImplicitLock(electionTerm int, state *int) {
	s.Mu.Lock()
	// if the electionTerm is Higher or equal than the ongoing electionTerm then finish the election
	// if the ongoing electionElectionTerm is greater meaning some other election was triggered and we have to give it more preference and we should not end up cancelling it
	if electionTerm >= s.OngoingElectionTerm {
		s.votedFor = ""
		s.OngoingElectionTerm = 0
		s.OngoingElection = false

		if *state != LEADER && s.CancelOngoingElection != nil {
			s.CancelOngoingElection()
		}
		s.CancelOngoingElection = nil

	}
	s.Mu.Unlock()
}

func (s *Server) StartElection(requestVoteFn func(context.Context, string, chan VoteT, string)) {

	var voteCh = make(chan VoteT, len(s.peers.Node_info))

	//self vote
	voteCount := 1

	s.Mu.Lock()
	// reset election timeout timer
	var electionTerm = s.currentTerm
	state := CANDIDATE

	s.OngoingElection = true
	s.OngoingElectionTerm = electionTerm
	electionCtx, cancel := context.WithCancel(context.Background())
	s.CancelOngoingElection = cancel
	s.Mu.Unlock()

	defer func() {
		s.finishElectionImplicitLock(electionTerm, &state)
		s.Mu.Lock()
		fmt.Println("election finished for term ", s.currentTerm, "node", s.node.Id, "state ", s.State)
		s.Mu.Unlock()
	}()

	for _, peer := range s.peers.Node_info {
		peer_address := peer.Address

		// ask for votes to all the nodes
		// no response for 5000 ms then cancel the request
		ctx, cancel := context.WithCancel(electionCtx)

		defer cancel()
		go requestVoteFn(ctx, peer_address, voteCh, peer.Id)

	}

	responseCount := 0

	localVoteMap := map[string]bool{
		s.node.Id: true,
	}
	for {

		select {
		case voteInfo := <-voteCh:

			voted := localVoteMap[strings.TrimSpace(voteInfo.votedBy)]
			if voteInfo.ElectionTerm == electionTerm && !voted {

				localVoteMap[voteInfo.votedBy] = (voteInfo.voteVal == 1)
				responseCount++

				voteCount += voteInfo.voteVal

				majority := voteCount > ((len(s.peers.Node_info) + 1) / 2)

				s.Mu.Lock()

				stillValid := s.State == CANDIDATE &&
					s.currentTerm == electionTerm

				s.Mu.Unlock()

				if majority && stillValid {
					fmt.Println("majority achieved! starting conversion to LEADER", s.node.Id, "for term :", s.currentTerm)
					state = LEADER
					s.StateCh <- LEADER
					return
				}

				if responseCount == len(s.peers.Node_info) && !majority && stillValid {
					state = FOLLOWER
					s.StateCh <- FOLLOWER
					return
				}
			}
		case <-electionCtx.Done():

			fmt.Println("elecition context cancelled converting to follower")
			//cancel election by higerTerm rpc or requestTimesout
			state = FOLLOWER
			s.StateCh <- FOLLOWER

			return

		case <-time.After(electionMaxDurationMs * time.Millisecond):
			state = FOLLOWER
			s.StateCh <- FOLLOWER
			return
		}

	}

	// voting finished
	// if requestVote has majority then become leader
	// otherwise become a Follower

}

// here the rpc call will happen
func (s *Server) requestVote(ctx context.Context, address string, VoteCh chan VoteT, id string) {

	s.Mu.Lock()

	lastLog := Log{}
	lastLogIndex := 0
	lastLogTerm := 0

	if len(s.log) != 0 {
		lastLog = s.log[len(s.log)-1]
		lastLogIndex = lastLog.LogIndex
		lastLogTerm = lastLog.Term
	}

	electionTerm := s.currentTerm
	payload := &pb.RequestVoteMessage{
		Term:         int32(electionTerm),
		CandidateId:  s.node.Id,
		LastLogIndex: int32(lastLogIndex),
		LastLogTerm:  int32(lastLogTerm),
	}
	s.Mu.Unlock()

	var conn *grpc.ClientConn

	maxRetries := MaxRetries
	for {

		var err error
		var ok bool
		s.Mu.Lock()
		conn, ok = s.connCache[id]
		s.Mu.Unlock()
		if !ok {

			conn, err = grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil && maxRetries <= 0 {
				s.logger.Error(err.Error(), "node", s.node.Id, "function", "requestVote")
				//can't establish conn
				VoteCh <- VoteT{
					votedBy:      id,
					voteVal:      0,
					ElectionTerm: electionTerm,
				}
				return
			}

			if err == nil {

				s.Mu.Lock()
				s.connCache[id] = conn
				s.Mu.Unlock()
				break
			}

		} else {
			break
		}
		maxRetries--
	}

	client := pb.NewRaftClient(conn)
	res, err := client.RequestVote(ctx, payload)

	if err != nil {
		s.Mu.Lock()
		delete(s.connCache, id)
		s.Mu.Unlock()
		s.logger.Error(err.Error(), "nodeId", s.node.Id, "function", "requestVote-2")
		return
	}

	s.Mu.Lock()
	higherTerm := res.Term > int32(s.currentTerm)
	s.Mu.Unlock()

	if higherTerm {
		s.logger.Info("cancelling election recieved term greater from another node")
		s.Mu.Lock()
		s.currentTerm = int(res.Term)
		s.Mu.Unlock()
		s.CancelOngoingElection()

		return
	}

	s.Mu.Lock()
	ok := s.voteMap[fmt.Sprintf("%s-%d", id, res.Term)]

	if !ok {

		if res.VoteGranted {
			s.voteMap[fmt.Sprintf("%s-%d", id, res.Term)] = true
			s.Mu.Unlock()
			VoteCh <- VoteT{
				ElectionTerm: electionTerm,
				votedBy:      id,
				voteVal:      1,
			}
			return
		} else {
			s.voteMap[fmt.Sprintf("%s-%d", id, res.Term)] = false
			s.Mu.Unlock()
			VoteCh <- VoteT{
				ElectionTerm: electionTerm,
				votedBy:      id,
				voteVal:      0,
			}
			return
		}
	}

	s.Mu.Unlock()
}

func (s *Server) SendHeartBeats(sendHeartBeatFn func(context.Context, string, string)) {

	ticker := time.NewTicker(HeartBeatSendIntervalMS * time.Millisecond)

	for {
		fmt.Println("waiting for ticker")
		<-ticker.C
		fmt.Println("ticker triggered for heartbeat")
		// if under the election timeout no appendEntries is sent then just send an empty AppendEntries RPC otherwise don't
		s.Mu.Lock()
		leaderId := s.leaderId
		nodeId := s.node.Id
		s.Mu.Unlock()

		fmt.Println("checking for leader")
		if leaderId != nodeId {
			fmt.Println("leader Id not equal to nodeId")
			return
		}

		fmt.Println("start seding heartbeats")
		for _, peer := range s.peers.Node_info {

			go func() {
				fmt.Println("sending heartbeats****")
				ctx, cancel := context.WithCancel(context.Background())
				sendHeartBeatFn(ctx, peer.Id, peer.Address)
				defer cancel()
			}()
		}

	}
}

func (s *Server) ResetHeartBeatTimer() {

	// start a timer if no heartbeat recieved then convert to candidate

	timeout := GetRandomHeartBeatTimeout()
	s.Mu.Lock()
	s.HeartBeatTimer.Reset(timeout)
	s.Mu.Unlock()
}

func (s *Server) ListenForHeartBeatTimeouts() {

	fmt.Println("started listening for heartbeat timeout")

	for {
		<-s.HeartBeatTimer.C

		s.Mu.Lock()
		if s.State != FOLLOWER {
			s.Mu.Unlock()
			return
		}
		s.Mu.Unlock()

		s.StateCh <- CANDIDATE
	}

}

func (s *Server) incrementTerm() {
	s.Mu.Lock()
	s.votedFor = ""
	s.currentTerm++
	s.Mu.Unlock()
}

func (s *Server) SetTerm(n int) {
	s.Mu.Lock()
	s.currentTerm = n
	s.Mu.Unlock()
}

func (s *Server) SetLeader(id string) {
	s.leaderId = id
}

func (s *Server) SetLogs(logs []Log) {
	s.Mu.Lock()
	s.log = logs
	s.Mu.Unlock()
}

func (s *Server) SetVotedFor(node string) {
	s.Mu.Lock()
	s.votedFor = node
	s.Mu.Unlock()
}
