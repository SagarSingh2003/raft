package statemachine

import (
	"strings"
	"sync"
)

type Log struct {
	Operation string
	NameSpace string
	Key       string
	Value     string
	CasValue  string
}

//What should the stateMachine store
//1. State - Key value pair like "name" : "sagar"
//2. metaData -
// i. lastApplied index of Log
// ii. lock

type Name string

type KVPair struct {
	Key   string
	Value string
}

type StateT struct {
	nameSpace map[Name]KVPair
}

type StateMachine struct {
	State            StateT
	Mu               sync.Mutex
	LastAppliedIndex int
}

// This will return last_applied after applying the logs to SM or send an error
func (sm *StateMachine) ApplyLogs(commitIndex int, lastAppliedIndex int, logs []Log) (int, error) {

	if commitIndex > lastAppliedIndex {
		successful, err := applyLogsToSM(logs, commitIndex)

		if err != nil {
			return 0, err
		}
		if successful {
			return commitIndex, nil
		} else {
			return lastAppliedIndex, nil
		}
	}

	return lastAppliedIndex, nil
}

// what will be the structure of logs :
// operation_name key value
// operations allowed : get , add , put , del , cas
func applyLogsToSM(logs []Log, commit_idx int) (bool, error) {

	for _, log := range logs {
		switch strings.ToUpper(strings.TrimSpace(log.Operation)) {
		case "ADD":
			// Add a pair if the key does not exist , if exists then return error
		case "PUT":
			// Add a pair if not exist , if exist then change the pair value
		case "CAS":
			// check if the value matches if yes then change the value else return error
		case "DEL":
			// if the key exists then delete else return error
		}
	}

	return true, nil
}
