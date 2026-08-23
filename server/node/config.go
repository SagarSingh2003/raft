package node

const LEADER = 1
const CANDIDATE = 2
const FOLLOWER = 3
const CloseListener = -1

// 10x of HeartBeatSendInterval

const HeartBeatTimerMaxTimeMs = 1000
const HeartBeatTimerMinTimeMs = 500
const electionMaxDurationMs = 1000
const HeartBeatSendIntervalMS = 50
const RequestTimeoutMS = 500
