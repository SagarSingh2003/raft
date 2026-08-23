package node

import (
	"context"

	pb "github.com/SagarSingh2003/Raft-KV/server/raft_proto"
	statemachine "github.com/SagarSingh2003/Raft-KV/server/state_machine"
)


func (s *Server) AppendEntries(ctx context.Context , in *pb.AppendEntriesRequest) ( *pb.AppendEntriesResponse, error){
	
	if in.Term 

}

func SendHeartBeat(ctx context.Context, address string) {

	//append entry rpc call here.

	//TODO: Put this in the rpc call above
	// s.Mu.Lock()
	// s.peerLastContactTime[peer.Id] = lastContactTime
	// s.Mu.Unlock()
}

func (s *Server) checkLogAvailability(nextIndex int) (int, bool) {

	s.Mu.Lock()
	last_log_index := len(s.log) - 1
	s.Mu.Unlock()

	if nextIndex <= last_log_index {
		return last_log_index, true
	}

	return -1, false
}

func (s *Server) checkLogCorrectness(prevLogIndex int, prevLogTerm int) bool {
	return true
}

func (s *Server) appendEntryToLocalLog(log Log) {

	s.Mu.Lock()
	s.log = append(s.log, log)
	s.Mu.Unlock()

}

func (s *Server) ApplyLogsToSM(log Log) {

	s.Mu.Lock()
	cid := s.commitIndex
	lastApplied := s.lastApplied
	log_entries := s.log
	s.Mu.Unlock()

	s.Sm.Mu.Lock()
	for _, log := range log_entries {
		s.Sm.ApplyLogs(cid, lastApplied, []statemachine.Log{
			{
				Operation: log.Operation,
				NameSpace: log.Namespace,
				Key:       log.Key,
				Value:     log.Value,
				CasValue:  log.CasValue,
			},
		})
	}
	s.Sm.Mu.Unlock()

}

func (s *Server) LeaderIncrementCommitIndex() int {

	// i have to find a N such that 1. N > commitIndex 2. Majority of MatchIndexSlice[i] >= N 3. log[N] == currenntTerm

	s.Mu.Lock()
	term := s.currentTerm

	log := make([]Log, len(s.log))
	copy(log, s.log)

	commitIndex := s.commitIndex

	matchIndexState := make([]matchIndexStateT, len(s.matchIndex))
	copy(matchIndexState, s.matchIndex)

	s.Mu.Unlock()

	for n := (len(log) - 1); n > commitIndex; n-- {

		count := 0

		//in matchIndex we have decided to store the leader's match Index(it's last log index as well)
		for _, matchIndexNodeInfo := range matchIndexState {
			if matchIndexNodeInfo.matchIndex >= n {
				count++
			}
		}

		// +1 because we count the leader itself too not present in peers
		if count >= ((len(s.peers.node_info)+1)/2)+1 && log[n].Term == term {
			return n
		}
	}

	return -1
}
