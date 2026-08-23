package node

import (
	"context"
	"log/slog"
	"time"
)

func (s *Server) finishElection(electionTerm int) {
	s.Mu.Lock()
	// if the electionTerm is Higher or equal than the ongoing electionTerm then finish the election
	// if the ongoing electionElectionTerm is greater meaning some other election was triggered and we have to give it more preference and we should not end up cancelling it
	if electionTerm >= s.ongoingElectionTerm {
		s.ongoingElectionTerm = 0
		s.ongoingElection = false
		s.cancelOngoingElection = nil
	}
	s.Mu.Unlock()
}

func (s *Server) StartElection() {

	var voteCh = make(chan int, len(s.peers.node_info))

	//self vote
	voteCount := 1

	// ask for votes to all the nodes
	// no response for 500 ms then cancel the request
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(RequestTimeoutMS*time.Millisecond))
	defer cancel()

	s.Mu.Lock()
	// reset election timeout timer
	s.electionTimer.Reset(time.Millisecond * electionMaxDurationMs)
	s.ongoingElection = true
	var electionTerm = s.currentTerm
	s.ongoingElectionTerm = electionTerm
	s.cancelOngoingElection = cancel
	s.Mu.Unlock()

	defer s.finishElection(electionTerm)

	for _, peer := range s.peers.node_info {
		address := peer.Address
		go requestVote(ctx, address, voteCh)
	}

	responseCount := 0

	for {

		select {
		case voteVal := <-voteCh:
			responseCount++
			voteCount += voteVal

			majority := voteCount > ((len(s.peers.node_info) + 1) / 2)

			if majority {
				s.StateCh <- LEADER
				return
			}

			if responseCount == len(s.peers.node_info) && !majority {
				s.StateCh <- FOLLOWER
				return
			}

		case <-ctx.Done():

			//cancel election by higerTerm rpc or requestTimesout
			s.StateCh <- FOLLOWER

			return
		}
	}
	// voting finished
	// if requestVote has majority then become leader
	// otherwise become a Follower

}

// here the api call will happen
func requestVote(ctx context.Context, address string, VoteCh chan int) {

	//requestVoteRpc

}

func (s *Server) SendHeartBeats() {

	ticker := time.NewTicker(HeartBeatSendIntervalMS * time.Millisecond)

	for {

		<-ticker.C

		// if under the election timeout no appendEntries is sent then just send an empty AppendEntries RPC otherwise don't
		s.Mu.Lock()
		leaderId := s.leaderId
		nodeId := s.node.Id
		s.Mu.Unlock()

		if leaderId != nodeId {
			return
		}

		for _, peer := range s.peers.node_info {

			go func() {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(RequestTimeoutMS*time.Millisecond))
				SendHeartBeat(ctx, peer.Address)
				defer cancel()
			}()
		}

	}
}

func (s *Server) ResetHeartBeatTimer() {
	// start a timer if no heartbeat recieved then convert to candidate
	s.heartBeatTimer.Reset(getRandomHeartBeatTimeout())

}

func (s *Server) ListenForHeartBeatTimeouts() {
	<-s.heartBeatTimer.C
	s.Mu.Lock()
	slog.Info("HeartBeat Timed out", "stateChange", "converting to candidate", "action", "starting election", "currentTerm", s.currentTerm)
	s.Mu.Unlock()
	s.StateCh <- CANDIDATE
	s.heartBeatTimer.Stop()
}

func (s *Server) incrementTerm() {
	s.Mu.Lock()
	s.currentTerm++
	s.Mu.Unlock()
}
