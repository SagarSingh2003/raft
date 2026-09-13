package node

import (
	"context"
	"fmt"

	pb "github.com/SagarSingh2003/Raft-KV/server/raft_proto"
	statemachine "github.com/SagarSingh2003/Raft-KV/server/state_machine"
)

type ClientOpResponse struct {
	success  bool
	errorStr string
}

func (s *Server) ClientOperation(ctx context.Context, in *pb.ClientOperationRequest) (*pb.ClientOperationResponse, error) {

	fmt.Println("reaching checkpoint 1")
	s.Mu.Lock()
	if s.State != LEADER && in.Operation != GETOP {
		fmt.Println("reached checkpoint 2")
		s.Mu.Unlock()
		return &pb.ClientOperationResponse{
			Success:  false,
			Error:    "",
			Value:    "",
			LeaderId: s.leaderId,
		}, nil
	}

	fmt.Println("reached checpoint 2.")
	logIdx := s.lastRecordedLogIndex + 1
	s.lastRecordedLogIndex = logIdx
	s.ClientOpListener[logIdx] = make(chan ClientOpResponse)

	if in.Operation != GETOP {
		s.log = append(s.log, Log{
			Term:      s.currentTerm,
			Namespace: in.Namespace,
			Operation: in.Operation,
			Key:       in.Key,
			Value:     in.Value,
			LogIndex:  logIdx,
		})
	} else {

		//do get op

		s.Mu.Unlock()
		return &pb.ClientOperationResponse{}, nil
	}

	s.Mu.Unlock()

	for _, peer := range s.peers.Node_info {
		go func(peer Node) {
			s.appendEntriesNotificationListener[peer.Id] <- struct{}{}
		}(peer)
	}

	fmt.Println("waiting for the log index response for client")
	select {

	case response := <-s.ClientOpListener[logIdx]:

		fmt.Println("got response for client")
		s.Mu.Lock()
		defer s.Mu.Unlock()
		delete(s.ClientOpListener, logIdx)

		return &pb.ClientOperationResponse{
			Success:  response.success,
			Error:    response.errorStr,
			Value:    "",
			LeaderId: s.leaderId,
		}, nil
	case <-ctx.Done():
		s.Mu.Lock()
		delete(s.ClientOpListener, logIdx)
		s.Mu.Unlock()
		return &pb.ClientOperationResponse{
			Success:  false,
			Error:    "timed out waiting for commit",
			LeaderId: s.leaderId,
		}, nil
	}

}

func (s *Server) ApplyLogsToSM() {

	s.Mu.Lock()
	cid := s.commitIndex
	lastApplied := s.lastApplied
	log_entries := s.log[s.lastApplied+1 : s.commitIndex+1]
	s.Mu.Unlock()

	response := &ClientOpResponse{
		errorStr: "",
		success:  true,
	}

	s.Sm.Mu.Lock()
	for _, log := range log_entries {
		lastApplied, err := s.Sm.ApplyLogs(cid, lastApplied, []statemachine.Log{
			{
				Operation: log.Operation,
				NameSpace: log.Namespace,
				Key:       log.Key,
				Value:     log.Value,
				CasValue:  log.CasValue,
			},
		})

		if ok := s.Sm.SmErrs[err.Error()]; err != nil && !ok {
			fmt.Println("ERROR occured while applying logs : ", err)
			response.errorStr = err.Error()
			response.success = false
		}

		s.lastApplied = lastApplied
		client, ok := s.ClientOpListener[log.LogIndex]

		if ok {
			go func() {
				client <- *response
			}()
		}

	}

	s.Sm.Mu.Unlock()

}
