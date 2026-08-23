package node

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func getRandomHeartBeatTimeout() time.Duration {

	return time.Duration(rand.Intn(HeartBeatTimerMaxTimeMs-HeartBeatTimerMinTimeMs+1) + HeartBeatTimerMinTimeMs)

}

// Op,NS,Key,Value,CasValue,3
func (s *Server) ParseLog(log string) (Log, error) {

	values := strings.Split(log, ",")

	newLogEntry := Log{}

	if len(values) != 6 {
		return Log{}, fmt.Errorf("ParseLog -> error Parsing Log length not enough %s", log)
	}

	newLogEntry.Operation = values[0]
	newLogEntry.Namespace = values[1]
	newLogEntry.Key = values[2]
	newLogEntry.Value = values[3]
	newLogEntry.CasValue = values[4]
	// ignoring the err because index value is just for debugging
	index, _ := strconv.Atoi(values[5])
	newLogEntry.LogIndex = index
	newLogEntry.Term = s.currentTerm

	return newLogEntry, nil
}
