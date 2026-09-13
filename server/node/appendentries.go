package node

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"time"

	pb "github.com/SagarSingh2003/Raft-KV/server/raft_proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *Server) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {

	s.Mu.Lock()
	term := s.currentTerm
	s.Mu.Unlock()

	res := &pb.AppendEntriesResponse{
		Term: int32(term),
	}

	if in.Term < int32(term) {
		fmt.Println("my term is greater than leader term ", "my term : ", term, " leader's term :", in.Term)
		res.Success = false
		res.Term = int32(term)
		return res, nil
	} else {
		s.Mu.Lock()
		s.currentTerm = int(in.Term)
		s.Mu.Unlock()
	}

	if len(in.Entries) == 0 {

		fmt.Println("getting heartbeats******")
		s.Mu.Lock()
		defer s.Mu.Unlock()
		state := s.State

		if s.HeartBeatTimer != nil {
			s.HeartBeatTimer.Stop()
			s.HeartBeatTimer.Reset(GetRandomHeartBeatTimeout())
		} else {
			s.HeartBeatTimer = time.NewTimer(GetRandomHeartBeatTimeout())
		}
		if state != FOLLOWER {
			s.StateCh <- FOLLOWER
		}

		s.leaderId = in.LeaderId

		if s.OngoingElection {
			s.CancelOngoingElection()
		}

		offset := s.log[0].LogIndex

		sliceIdx := int(in.PrevLogIndex) - offset
		if int(in.LeaderCommit) > s.commitIndex && sliceIdx >= 0 && sliceIdx < len(s.log) && s.log[sliceIdx].Term == int(in.PrevLogTerm) {
			s.commitIndex = min(int(in.LeaderCommit), s.log[len(s.log)-1].LogIndex)
			s.commitIndexListenerCh <- struct{}{}
		}

		res.Success = true
		res.Term = int32(term)
		return res, nil
	}

	s.Mu.Lock()
	defer s.Mu.Unlock()

	fmt.Println("checkinng log correctness")
	// the logs are contigous we need the base IndexValue of the logs
	logBaseValue := s.log[0].LogIndex
	// eg. if it's 0 , then to find logIndex 8 we need to get the 8th index value in slice
	// another eg. if it's 110 , then to find LogIndex 144 we need to get the 144 - 110 index value in slice
	// what if the base value is more than the logIndex value?? that would be devastating for lol it's 2:51 am and i don't really wanna think about it now

	if logBaseValue > int(in.PrevLogIndex) {
		res.Success = false
		res.Term = int32(s.currentTerm)
		return res, nil
	}

	prevSliceLogIndex := in.PrevLogIndex - int32(logBaseValue)
	// the index surpasses the log max index value
	if int(prevSliceLogIndex) > len(s.log)-1 {
		res.Success = false
		res.Term = int32(s.currentTerm)
		return res, nil
	}

	prevLogIndex := s.log[prevSliceLogIndex].LogIndex

	prevLogTerm := s.log[prevSliceLogIndex].Term

	if int32(prevLogIndex) < in.PrevLogIndex || int32(prevLogTerm) != in.PrevLogTerm {
		res.Success = false
		return res, nil
	}

	// the lastLogInMem Index becomes the starting point after this all the new logs will enter eg. [{1 , 0} , {2 , 0}  , {3 , 1} , {4 , 4}] <[{logIdx , term}]> , the incoming logs are [{4 , 2} , {7, 3}]
	// 1.  now as we can see the lastLogTerm does not match so the prev Entry should be deleted
	// 2. if we reach this point that means the last logTerms match
	// 3. for the next entries we check one by one if something don't match just delete the entries from there and replace them with new entries
	// start inserting entries if the entries are not already present , in case of conflicting entries delete all the following entries and then insert the rest
	firstConflict := -1
	for i, log := range in.Entries {
		parsed, err := s.ParseLog(log)
		if err != nil {
			fmt.Println("ERROR: parsing lead to error", err.Error())
			continue
		}
		sliceIdx := parsed.LogIndex - logBaseValue
		if sliceIdx > len(s.log)-1 || s.log[sliceIdx].Term != parsed.Term {
			firstConflict = i
			break
		}
	}

	if firstConflict != -1 {
		fmt.Println("First conflict found at logIndex : ", firstConflict)
		parsed, _ := s.ParseLog(in.Entries[firstConflict])
		sliceIdx := parsed.LogIndex - logBaseValue
		fmt.Println("first conflict found at slice idx ", sliceIdx)
		invalidateLogInWal := []PersistLogT{}
		if sliceIdx < len(s.log) {
			invalidatelogs := append([]Log{}, s.log[sliceIdx:]...)

			fmt.Println("invalidation logs : ", invalidatelogs)
			s.log = s.log[:sliceIdx]
			for _, invalidatelog := range invalidatelogs {
				persistLog := PersistLogT{
					Log:          invalidatelog,
					VotedFor:     s.votedFor,
					VotedForTerm: s.currentTerm,
					Invalidate:   true,
				}
				invalidateLogInWal = append(invalidateLogInWal, persistLog)
			}
		}

		fmt.Println("invalidateLogInWal : ", invalidateLogInWal)
		if len(invalidateLogInWal) > 0 {
			s.PersistInvalidationLogs(invalidateLogInWal)
		}
		s.log = append(s.log, s.parseLogEntries(in.Entries[firstConflict:])...)
	}

	if int(in.LeaderCommit) > s.commitIndex {
		s.commitIndex = min(int(in.LeaderCommit), s.log[len(s.log)-1].LogIndex)
		s.commitIndexListenerCh <- struct{}{}
		fmt.Println("sending to commitIndex listenner")
		fmt.Println("sennt to commitINdex listener")
	}

	s.PersistToLog()
	res.Success = true
	res.Term = int32(term)
	return res, nil

}

func (s *Server) SendHeartBeat(ctx context.Context, id, address string) {

	//TODO: Put this in the rpc call above

	s.Mu.Lock()
	currTerm := int32(s.currentTerm)
	// what if the s.log is empty ??
	lastLog := s.log[len(s.log)-1]
	payload := &pb.AppendEntriesRequest{
		Term:         int32(s.currentTerm),
		LeaderId:     s.leaderId,
		PrevLogIndex: int32(lastLog.LogIndex),
		PrevLogTerm:  int32(lastLog.Term),
		Entries:      []string{},
		LeaderCommit: int32(s.commitIndex),
	}
	s.Mu.Unlock()

	var conn *grpc.ClientConn

	var err error
	var ok bool
	s.Mu.Lock()
	conn, ok = s.connCache[id]
	s.Mu.Unlock()

	if !ok {
		conn, err = grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			s.logger.Error(err.Error(), "node", s.node.Id, "function", "SendHeartBeat", "dest", id)
			fmt.Println("error occured could not send hb: ", err.Error())
			return
		}

		s.Mu.Lock()
		s.connCache[id] = conn
		s.Mu.Unlock()

	}

	client := pb.NewRaftClient(conn)
	res, err := client.AppendEntries(ctx, payload)

	fmt.Println(res, err, "response from sending heartbeat")
	if err != nil {
		s.logger.Error(err.Error(), "function", "SendHeartBeat", "payload", payload, "res", res, "dest", id)
		return
	}

	if res.Success == true {
		s.logger.Info("successfully sent heartbeat to", "dest", id)
		s.Mu.Lock()
		s.peerLastContactTime[id] = time.Now()
		s.Mu.Unlock()
		return
	} else {
		inTerm := res.Term

		if currTerm < inTerm {
			s.Mu.Lock()
			s.currentTerm = int(inTerm)
			s.Mu.Unlock()
		}

	}
}

func (s *Server) checkLogAvailability(nextIndex int) (int, bool) {

	s.Mu.Lock()
	last_log := s.log[len(s.log)-1]
	last_log_idx := last_log.LogIndex
	s.Mu.Unlock()

	if nextIndex <= last_log_idx {
		return last_log_idx, true
	}

	return -1, false
}

func (s *Server) appendEntryToLocalLog(log Log) {

	s.Mu.Lock()
	s.log = append(s.log, log)
	s.Mu.Unlock()

}

func (s *Server) LeaderIncrementCommitIndex() {

	// i have to find a N such that 1. N > commitIndex 2. Majority of MatchIndexSlice[i] >= N 3. log[N] == currenntTerm

	s.Mu.Lock()
	term := s.currentTerm

	log := make([]Log, len(s.log))
	copy(log, s.log)

	commitIndex := s.commitIndex

	matchIndexState := maps.Clone(s.matchIndex)

	s.Mu.Unlock()

	for n := log[(len(log) - 1)].LogIndex; n > commitIndex; n-- {

		count := 0

		//in matchIndex we have decided to store the leader's match Index(it's last log index as well)
		for _, matchIndex := range matchIndexState {
			if matchIndex >= n {
				count++
			}
		}

		// +1 because we count the leader itself too not present in peers
		if count >= ((len(s.peers.Node_info)+1)/2)+1 && log[n].Term == term {
			s.Mu.Lock()
			s.commitIndex = n
			s.commitIndexListenerCh <- struct{}{}
			s.Mu.Unlock()
			return
		}
	}

	return
}

func (s *Server) PersistToLog() int {

	s.logMu.Lock()
	defer s.logMu.Unlock()

	lastLogIndexPersisted := s.lastLogIndexPersisted

	file, err := os.OpenFile(fmt.Sprintf("./%s.jsonl", s.node.Id), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		fmt.Println("error occured while opening wal file", err.Error())
		return -1
	}

	defer file.Close()
	defer func() {
		if err := file.Sync(); err != nil {
			fmt.Println("fsync failed:", err.Error())
		}
	}()

	offset := s.log[0].LogIndex
	sliceIdx := (lastLogIndexPersisted + 1) - offset

	if sliceIdx < len(s.log) {
		for i := sliceIdx; i < len(s.log); i++ {

			log := s.log[i]
			persistLog := PersistLogT{
				Log:          log,
				VotedFor:     s.votedFor,
				VotedForTerm: s.currentTerm,
				Invalidate:   false,
			}
			b, err := json.Marshal(persistLog)
			if err != nil {
				fmt.Println("Failed to marshal into json ", err.Error())
				return -1
			}
			_, err = file.Write(append(b, '\n'))
			if err != nil {
				fmt.Println("Failed to convert it to byte slice", err.Error())
				return -1
			}

			s.lastLogIndexPersisted = log.LogIndex

		}
	}
	return 1
}

func (s *Server) PersistInvalidationLogs(invalidationLogs []PersistLogT) int {

	s.logMu.Lock()
	defer s.logMu.Unlock()

	file, err := os.OpenFile(fmt.Sprintf("./%s.jsonl", s.node.Id), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		fmt.Println("error occured while opening wal file", err.Error())
		return -1
	}

	defer file.Close()

	for _, invalidatLog := range invalidationLogs {

		persistLog := invalidatLog

		b, err := json.Marshal(persistLog)
		if err != nil {
			fmt.Println("Failed to marshal into json ", err.Error())

		}

		_, err = file.Write(append(b, '\n'))

		if err != nil {
			fmt.Println("Error occured", err.Error())
			return -1
		}

	}

	s.lastLogIndexPersisted = invalidationLogs[0].Log.LogIndex - 1
	return 1
}

func (s *Server) StartAppendEntriesWorkers() {

	worker := func(node Node) {
		for {
			select {
			case <-s.appendEntriesNotificationListener[node.Id]:

				// send appendEntry here
				var conn *grpc.ClientConn

				var err error
				var ok bool
				s.PersistToLog()
				s.Mu.Lock()
				conn, ok = s.connCache[node.Id]
				s.Mu.Unlock()

				if !ok {
					conn, err = grpc.NewClient(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

					if err != nil {
						s.logger.Error(err.Error(), "node", s.node.Id, "function", "SendHeartBeat", "dest", node.Id)
						fmt.Println("error occured could not send hb: ", err.Error())
						continue
					}

					s.Mu.Lock()
					s.connCache[node.Id] = conn
					s.Mu.Unlock()

				}

				ctx := context.Background()

				for {
					s.Mu.Lock()
					fmt.Println("Locking for appendEntries")
					term := int32(s.currentTerm)
					nextIndex, _ := s.nextIndex[node.Id]

					sliceIdx := s.getLogTermExplicitLock(nextIndex)

					if sliceIdx == -1 {
						// the nextIndex was not Found
						fmt.Println("Next Index ", nextIndex, " not found , length : ", len(s.log), "entries : ", s.log)
						s.Mu.Unlock()
						break
					}

					prevLogIndex := s.log[sliceIdx-1].LogIndex
					prevLogTerm := s.log[sliceIdx-1].Term

					commitIndex := s.commitIndex
					entries := append([]Log{}, s.log[sliceIdx:]...)

					fmt.Println("gathered all data , unlocking for appendEntries")
					s.Mu.Unlock()

					payload := &pb.AppendEntriesRequest{
						Term:         term,
						PrevLogIndex: int32(prevLogIndex),
						PrevLogTerm:  int32(prevLogTerm),
						Entries:      s.ParseLogToText(entries),
						LeaderCommit: int32(commitIndex),
						LeaderId:     s.node.Id,
					}

					client := pb.NewRaftClient(conn)
					res, err := client.AppendEntries(ctx, payload)

					fmt.Println(res, err, "response from appendEntries")
					if err != nil {
						s.logger.Error(err.Error(), "function", "SendAppendEntriesWorkers", "payload", payload, "res", res, "dest", node.Id)
						time.Sleep(50 * time.Millisecond)
						continue
					}

					if res.Success == false {

						// if term is higher then return to Follower state , stop appendEntries
						if res.Term > term {
							s.Mu.Lock()
							s.initializeNextIndexExplicitLock()
							s.initializeMatchIndexExplicitLock()
							s.Mu.Unlock()
							fmt.Println("becoming follower idempotently")
							s.becomeIdempotentFollowerImplicitLock()
							break
						}

						// if term is not higher then the log might not match so decrease the nextIndex for this node by 1
						if nextIndex > 0 {
							s.nextIndex[node.Id] = nextIndex - 1
						}
						break
					} else {
						s.Mu.Lock()
						s.matchIndex[node.Id] = entries[len(entries)-1].LogIndex
						s.nextIndex[node.Id] = entries[len(entries)-1].LogIndex + 1
						s.Mu.Unlock()

						s.LeaderIncrementCommitIndex()
						break

					}

				}
			case <-s.finishAppendEntriesCh:
				return

			}
		}
	}

	for _, node := range s.peers.Node_info {
		go worker(node)
	}
}

func (s *Server) becomeIdempotentFollowerImplicitLock() {
	s.Mu.Lock()
	isFollower := s.State == FOLLOWER
	s.Mu.Unlock()

	if !isFollower {
		s.StateCh <- FOLLOWER
	}
}

func (s *Server) parseLogEntries(entries []string) []Log {
	parsedEntries := []Log{}
	for _, entry := range entries {
		log, err := s.ParseLog(entry)
		if err != nil {
			fmt.Println("ERROR parsing log , err :", err.Error(), " log : ", entry)
		}
		parsedEntries = append(parsedEntries, log)
	}

	return parsedEntries
}
