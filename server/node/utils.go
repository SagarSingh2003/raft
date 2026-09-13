package node

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

func replayLogs(id string) ([]Log, string, int) {
	path := path.Join(walParentDirPath, fmt.Sprintf("%s.jsonl", id))

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		fmt.Println("ERROR: replaying logs failed : ", err.Error())
		return []Log{}, "", 0
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	logs := []Log{}
	votedFor := ""
	votedForTerm := 0
	for {
		line, err := reader.ReadString('\n')

		if len(line) > 0 {

			var logRecord PersistLogT
			if jsonErr := json.Unmarshal([]byte(strings.TrimSpace(line)), &logRecord); jsonErr != nil {
				fmt.Println("ERROR: corrupted log ", jsonErr)
				break
			}

			votedFor = logRecord.VotedFor
			votedForTerm = logRecord.VotedForTerm

			if logRecord.Invalidate {
				if len(logs) > 0 {
					offset := logs[0].LogIndex
					indexToBeRemoved := logRecord.Log.LogIndex - offset
					if len(logs) > indexToBeRemoved && logs[indexToBeRemoved].LogIndex == logRecord.Log.LogIndex {
						logs = logs[:indexToBeRemoved]
					} else {
						for i, log := range logs {
							if log.LogIndex == logRecord.Log.LogIndex {
								logs = logs[:i]
								break
							}
						}
						fmt.Println("Log not found for invalidation", " logs :", logs, " invalidation Log", " ", logRecord)
					}
				}
			} else {
				logs = append(logs, logRecord.Log)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("WAL read error: %w", err)
			break
		}
	}

	//check if the logs are contiguous
	if len(logs) > 0 {

		lastLogIndex := logs[0].LogIndex
		for i := 1; i <= len(logs)-1; i++ {
			if logs[i].LogIndex != lastLogIndex+1 {
				panic(fmt.Sprintf("Logs are not in serial order: %v", logs))
			}
			lastLogIndex = logs[i].LogIndex
		}
	}
	return logs, votedFor, votedForTerm
}

func GetRandomHeartBeatTimeout() time.Duration {

	// timeout range is 500 to 1000 ms
	// to maximize the differennce i have to increase the no. of random fields
	// 50 * 10 is 500 ,
	// 20 * 4 is 80,
	// 35 * 6 is 215,
	// adding 500 to all gives
	// 1000 , 580 , 715 , i think that's ranndom enough
	return time.Duration(((rand.Intn(HeartBeatTimerMinTimeMs/10+1)+1)*(rand.Intn(10+1)+1) + HeartBeatTimerMinTimeMs) * int(time.Millisecond))

}

func getServerConfigFromYAML(configPath string) ServerConfig {
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	var config ServerConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error parsing YAML: %v", err)
	}

	return config
}

// Op,NS,Key,Value,CasValue,3,term
func (s *Server) ParseLog(log string) (Log, error) {

	values := strings.Split(log, ",")

	newLogEntry := Log{}

	if len(values) != 7 {
		return Log{}, fmt.Errorf("ParseLog -> error Parsing Log length not enough %s", log)
	}

	newLogEntry.Operation = values[0]
	newLogEntry.Namespace = values[1]
	newLogEntry.Key = values[2]
	newLogEntry.Value = values[3]
	newLogEntry.CasValue = values[4]
	index, err := strconv.Atoi(values[5])
	term, err := strconv.Atoi(values[6])

	if err != nil {
		return Log{}, fmt.Errorf("ParseLog -> error Parsing index value %s , error : %s", values[5], err.Error())
	}

	newLogEntry.LogIndex = index
	newLogEntry.Term = term

	return newLogEntry, nil
}

func (s *Server) ParseLogToText(log []Log) []string {
	logs := []string{}
	for _, eachLog := range log {
		logs = append(logs, fmt.Sprintf("%s,%s,%s,%s,%s,%d,%d", eachLog.Operation, eachLog.Namespace, eachLog.Key, eachLog.Value, eachLog.CasValue, eachLog.LogIndex, eachLog.Term))
	}
	return logs
}
