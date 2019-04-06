package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"github.com/Fallensouls/raft/labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type State uint8

const (
	leader State = iota
	candidate
	follower
)

const HeartBeatInterval = 50 * time.Millisecond // 50mS
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	id     string
	state  State
	leader string

	// persistent state on all servers.
	currentTerm uint64
	votedFor    string
	log         *Log

	// volatile state on all servers.
	commitIndex uint64
	lastApplied uint64

	// volatile state on leaders.
	nextIndex  []uint64
	matchIndex []uint64

	electionTimeout time.Duration
	resetCh         chan struct{} // signal for converting to follower and reset election ticker
	applyCh         chan ApplyMsg
}

/*
*****************************
*		   General			*
*****************************
 */

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.state == leader
}

func (rf *Raft) State() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

/*
*****************************
*	  State Transition		*
*****************************
 */

func (rf *Raft) convertToFollower(term uint64) {
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = ``
	rf.resetCh <- struct{}{}

	logger.Printf("follower id:%s\n", rf.id)
	logger.Printf("follower term: %d\n", rf.currentTerm)
}

func (rf *Raft) convertToCandidate() {
	rf.currentTerm += 1
	rf.state = candidate
	rf.votedFor = rf.id

	logger.Printf("candidate id:%s\n", rf.id)
	logger.Printf("candidate term: %d\n", rf.currentTerm)
}

func (rf *Raft) convertToLeader() {
	rf.state = leader
	rf.votedFor = ``
	rf.nextIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.LastIndex() + 1
	}

	logger.Printf("leader id:%s\n", rf.id)
	logger.Printf("leader term: %d\n", rf.currentTerm)
}

/*
*****************************
*	    Persistence			*
*****************************
 */

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:`
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

/*
****************************
*	   Request Vote		   *
****************************
 */

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteRequest struct {
	// Your data here (2A, 2B).
	Term         uint64 // candidate's term
	CandidateId  string // candidate's id
	LastLogIndex uint64 // index of candidate's last log entry
	LastLogTerm  uint64 // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteResponse struct {
	// Your data here (2A).
	Term        uint64 // receiver's currentTerm
	VoteGranted bool   // true if candidate can receive vote
}

func (rf *Raft) NewVoteRequest() *RequestVoteRequest {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return &RequestVoteRequest{
		rf.currentTerm,
		rf.id,
		rf.log.LastIndex(),
		rf.log.LastTerm(),
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(req *RequestVoteRequest, res *RequestVoteResponse) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// return receiver's currentTerm for candidate to update itself.
	res.Term = rf.currentTerm

	// reply false if candidate's term is less than receiver's currentTerm.
	// reply false when receiver is also a candidate or has voted for another candidate.
	if rf.currentTerm >= req.Term {
		return
	}

	// If receiver's currentTerm is less than candidate's term,
	// receiver should update itself and convert to follower.
	rf.convertToFollower(req.Term)

	// check whether candidate's log is at least as up-to-date as receiver's log.
	var upToDate bool
	if rf.log.LastTerm() < req.LastLogTerm {
		upToDate = true
	}
	if rf.log.LastTerm() == req.LastLogTerm && rf.log.LastIndex() <= req.LastLogIndex {
		upToDate = true
	}
	// if receiver is not a candidate and upToDate is true, the candidate will receive a vote.
	if rf.votedFor == `` && upToDate {
		log.Printf("%s voted for: %s", rf.id, req.CandidateId)
		rf.votedFor = req.CandidateId
		res.VoteGranted = true
	}

	// persistent state should be persisted before responding to RPCs.
	rf.persist()
}

func (rf *Raft) handleVoteResponse(res RequestVoteResponse, voteCh chan<- struct{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < res.Term {
		rf.convertToFollower(res.Term)
		return
	}
	if res.VoteGranted {
		voteCh <- struct{}{}
	}
}

/*
*****************************
*	   Append Entries		*
*****************************
 */

type AppendEntriesRequest struct {
	Term         uint64     // leader's term
	LeaderId     string     // leader's id
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit uint64     // leader's commitIndex
}

type AppendEntriesResponse struct {
	Term    uint64 // receiver's currentTerm
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
	Index   uint64 // the index to be used for updating nextIndex and matchIndex

	// extra information for conflicts
	ConflictIndex uint64
	ConflictTerm  uint64
}

func (rf *Raft) NewAppendEntriesRequest(server int) *AppendEntriesRequest {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := rf.nextIndex[server] - 1
	entry := rf.log.Entry(rf.nextIndex[server] - 1)
	var prevLogTerm uint64
	if entry == nil {
		prevLogTerm = 0
	} else {
		prevLogTerm = entry.Term
	}

	return &AppendEntriesRequest{
		rf.currentTerm,
		rf.id,
		prevLogIndex,
		prevLogTerm,
		rf.log.EntriesAfter(prevLogIndex),
		rf.commitIndex,
	}
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// return receiver's currentTerm for candidate to update itself.
	res.Term = rf.currentTerm

	// reply false if candidate's term is less than receiver's currentTerm.
	if rf.currentTerm > req.Term {
		return
	}

	// If receiver's currentTerm is not greater than candidate's term, receiver should update itself and convert to follower.
	if rf.currentTerm <= req.Term {
		rf.convertToFollower(req.Term)
		rf.leader = req.LeaderId
	}

	// reply false if receiver's log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
	entry := rf.log.Entry(req.PrevLogIndex)
	if entry == nil || entry.Term != req.PrevLogTerm {
		//res.ConflictIndex, res.ConflictTerm = rf.log.SearchConflict(req.PrevLogIndex, req.PrevLogTerm)
		return
	}

	// now receiver can reply true since entry matches successfully.
	res.Success = true

	// if the last log entry matches prevLogIndex and prevLogTerm
	if rf.log.LastIndex() == req.PrevLogIndex {
		rf.log.entries = append(rf.log.entries, req.Entries...)
	} else {
		storageIndex := req.PrevLogIndex - rf.log.lastIncludedIndex - 1
		rf.log.entries = rf.log.entries[:storageIndex]
		rf.log.entries = append(rf.log.entries, req.Entries...)
	}

	if rf.commitIndex < req.LeaderCommit {
		rf.commitIndex = min(req.LeaderCommit, rf.log.LastIndex())
	}
	res.Index = req.Entries[len(req.Entries)-1].Index

	rf.persist()
}

func (rf *Raft) handleAppendEntriesResponse(server int, res AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < res.Term {
		rf.convertToFollower(res.Term)
		return
	}

	if res.Success {
		rf.nextIndex[server] = res.Index + 1
		rf.matchIndex[server] = res.Index
	} else {
		rf.nextIndex[server]--
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", request, response)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", request, response)
	return ok
}

/*
*****************************
*	       Event			*
*****************************
 */

func (rf *Raft) electLeader(voteCh chan struct{}) {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var response RequestVoteResponse
				if rf.sendRequestVote(server, rf.NewVoteRequest(), &response) {
					rf.handleVoteResponse(response, voteCh)
				}
			}(i)
		}
	}
}

func (rf *Raft) broadcast() {
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var response AppendEntriesResponse
				if rf.sendAppendEntries(server, rf.NewAppendEntriesRequest(server), &response) {
					rf.handleAppendEntriesResponse(server, response)
				}
			}(i)
		}
	}
}

/*
*****************************
*	     State Loop			*
*****************************
 */

// FollowerLoop is loop of follower.
// Since follower can only convert to candidate, the follower loop just wait for a election timeout,
// or reset its election timer.
func (rf *Raft) followerLoop() {
	electionTimer := time.NewTimer(rf.electionTimeout)
	for {
		select {
		case <-electionTimer.C:
			rf.mu.Lock()
			rf.convertToCandidate()
			rf.mu.Unlock()
			return
		case <-rf.resetCh:
			if !electionTimer.Stop() {
				<-electionTimer.C
			}
			electionTimer.Reset(rf.electionTimeout)
		}
	}
}

// CandidateLoop is loop of candidate.
// Candidate will have the following action:
// 1) If votes received from majority of servers: become leader.
// 2) If AppendEntries RPC received from new leader: convert to follower.
// 3) If election timeout elapses: start new election.
func (rf *Raft) candidateLoop() {
	votes := 1
	voteCh := make(chan struct{}, len(rf.peers)-1)
	electionTicker := time.NewTicker(rf.electionTimeout)
	rf.electLeader(voteCh)

	for {
		select {
		case <-voteCh:
			votes++
			//logger.Printf("votes: %d\n", votes)
			if votes > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.convertToLeader()
				rf.mu.Unlock()
				return
			}
		case <-electionTicker.C:
			rf.mu.Lock()
			rf.convertToCandidate()
			rf.mu.Unlock()
			return
		case <-rf.resetCh:
			return
		}
	}
}

// LeaderLoop is loop of leader.
// Leader will convert to follower if it receives a request or response containing a higher term.
func (rf *Raft) leaderLoop() {
	heartBeatTicker := time.NewTicker(HeartBeatInterval)
	for {
		select {
		case <-heartBeatTicker.C:
			rf.broadcast()
		case <-rf.resetCh:
			return
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an electLeader. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return index, int(rf.currentTerm), rf.state == leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.id = RandomID(8)
	rf.state = follower
	rf.log = NewLog()
	rf.resetCh = make(chan struct{})

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimeout = time.Millisecond * time.Duration(400+r.Intn(200))

	go func() {
		for {
			switch rf.State() {
			case leader:
				rf.leaderLoop()
			case candidate:
				rf.candidateLoop()
			case follower:
				rf.followerLoop()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
