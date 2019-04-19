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
//   ask a Raft for its current term, and whether it thinks it is Leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/Fallensouls/raft/labgob"
	"github.com/Fallensouls/raft/labrpc"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log Entries are
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
	NoOpCommand  bool
}

type State uint8

const (
	Leader State = iota
	Candidate
	Follower
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

	ID     string
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
	resetCh         chan struct{} // signal for converting to Follower and reset election ticker
	applyCh         chan ApplyMsg
}

/*
*****************************
*          General          *
*****************************
 */

// return currentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.state == Leader
}

func (rf *Raft) State() State {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

/*
*****************************
*      State Transition     *
*****************************
 */

func (rf *Raft) convertToFollower(term uint64) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = ``

	//logger.Printf("Follower ID:%s\n", rf.ID)
	//logger.Printf("Follower term: %d\n", rf.currentTerm)
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()

	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.ID

	rf.mu.Unlock()
	//logger.Printf("Candidate ID:%s\n", rf.ID)
	//logger.Printf("Candidate term: %d\n", rf.currentTerm)
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()

	rf.state = Leader
	rf.votedFor = ``
	rf.nextIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))

	// add a no-op entry to local log.
	index := rf.log.LastIndex() + 1
	rf.log.AddEntry(rf.currentTerm, nil)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist()

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.LastIndex() + 1
	}

	rf.mu.Unlock()
	//logger.Printf("Leader ID:%s\n", rf.ID)
	//logger.Printf("Leader term: %d\n", rf.currentTerm)
}

/*
*****************************
*        Persistence        *
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(rf.log)

}

/*
****************************
*       Request Vote       *
****************************
 */

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteRequest struct {
	// Your data here (2A, 2B).
	Term         uint64 // Candidate's term
	CandidateId  string // Candidate's ID
	LastLogIndex uint64 // index of Candidate's last log entry
	LastLogTerm  uint64 // term of Candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteResponse struct {
	// Your data here (2A).
	Term        uint64 // receiver's currentTerm
	VoteGranted bool   // true if Candidate can receive vote
}

func (rf *Raft) NewVoteRequest() *RequestVoteRequest {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return &RequestVoteRequest{
		rf.currentTerm,
		rf.ID,
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

	// return receiver's currentTerm for Candidate to update itself.
	res.Term = rf.currentTerm

	// reply false if Candidate's term is less than receiver's currentTerm.
	// reply false when receiver is also a Candidate or has voted for another Candidate.
	if rf.currentTerm >= req.Term {
		return
	}

	var reset bool
	if rf.state == Leader {
		reset = true
	}
	// If receiver's currentTerm is less than Candidate's term,
	// receiver should update itself and convert to Follower.
	rf.convertToFollower(req.Term)

	// check whether Candidate's log is at least as up-to-date as receiver's log.
	var upToDate bool
	if rf.log.LastTerm() < req.LastLogTerm {
		upToDate = true
	}
	if rf.log.LastTerm() == req.LastLogTerm && rf.log.LastIndex() <= req.LastLogIndex {
		upToDate = true
	}
	// if receiver is not a Candidate and upToDate is true, the Candidate will receive a vote.
	if rf.votedFor == `` && upToDate {
		//log.Printf("%s voted for: %s", rf.ID, req.CandidateId)
		rf.votedFor = req.CandidateId
		res.VoteGranted = true
		rf.resetCh <- struct{}{}
	} else {
		if reset {
			rf.resetCh <- struct{}{}
		}
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
*      Append Entries       *
*****************************
 */

type AppendEntriesRequest struct {
	Term         uint64     // Leader's term
	LeaderId     string     // Leader's ID
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of prevLogIndex entry
	Entries      []LogEntry // log Entries to store
	LeaderCommit uint64     // Leader's commitIndex
}

type AppendEntriesResponse struct {
	Term    uint64 // receiver's currentTerm
	Success bool   // true if Follower contained entry matching prevLogIndex and prevLogTerm
	Index   uint64 // the index to be used for updating nextIndex and matchIndex

	// extra information for conflicts
	FirstIndex   uint64
	ConflictTerm uint64
}

func (rf *Raft) NewAppendEntriesRequest(server int) *AppendEntriesRequest {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := rf.nextIndex[server] - 1
	entry := rf.log.Entry(prevLogIndex)
	var prevLogTerm uint64
	if entry != nil {
		prevLogTerm = entry.Term
	}

	return &AppendEntriesRequest{
		rf.currentTerm,
		rf.ID,
		prevLogIndex,
		prevLogTerm,
		rf.log.EntriesAfter(prevLogIndex),
		rf.commitIndex,
	}
}

func (rf *Raft) NewHeartBeat() *AppendEntriesRequest {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return &AppendEntriesRequest{
		rf.currentTerm,
		rf.ID,
		0,
		1,
		nil,
		0,
	}
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//logger.Printf("server %s receives request: %v", rf.ID, req)

	// return receiver's currentTerm for Candidate to update itself.
	res.Term = rf.currentTerm

	// reply false if Candidate's term is less than receiver's currentTerm.
	if rf.currentTerm > req.Term {
		return
	}

	// If receiver's currentTerm is not greater than Candidate's term, receiver should update itself and convert to Follower.
	if rf.currentTerm <= req.Term {
		rf.convertToFollower(req.Term)
		rf.leader = req.LeaderId
		rf.resetCh <- struct{}{}
	}

	if !(rf.log.LastIncludedIndex == req.PrevLogIndex && rf.log.LastIncludedTerm == req.PrevLogTerm) {
		// reply false if receiver's log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
		entry := rf.log.Entry(req.PrevLogIndex)
		if entry == nil {
			// Leader has more logs, let nextIndex be the last index of receiver.
			res.FirstIndex = rf.log.LastIndex() + 1
			return
		}
		if entry.Term != req.PrevLogTerm {
			// there are conflicting entries
			res.FirstIndex = rf.log.SearchFirstIndex(req.PrevLogIndex, entry.Term)
			res.ConflictTerm = entry.Term
			return
		}
	}

	// now receiver can reply true since entry matches successfully.
	res.Success = true

	//logger.Printf("last index of server %v: %v", rf.ID, rf.log.LastIndex())

	// if the last log entry matches prevLogIndex and prevLogTerm
	if rf.log.LastIndex() == req.PrevLogIndex {
		rf.log.Entries = append(rf.log.Entries, req.Entries...)
	} else {
		storageIndex := req.PrevLogIndex - rf.log.LastIncludedIndex
		rf.log.Entries = rf.log.Entries[:storageIndex]
		rf.log.Entries = append(rf.log.Entries, req.Entries...)
	}

	if rf.commitIndex < req.LeaderCommit {
		rf.commitIndex = min(req.LeaderCommit, rf.log.LastIndex())
	}

	// not a heartbeat
	if req.Entries != nil {
		res.Index = req.Entries[len(req.Entries)-1].Index
	}

	//logger.Printf("commit index of server %v: %v", rf.ID, rf.commitIndex)
	go rf.apply()
	rf.persist()
}

func (rf *Raft) handleAppendEntriesResponse(server int, res AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < res.Term {
		rf.convertToFollower(res.Term)
		rf.resetCh <- struct{}{}
		return
	}

	if res.Success {
		if res.Index != 0 {
			rf.nextIndex[server] = res.Index + 1
			rf.matchIndex[server] = res.Index
		}
	} else {
		rf.nextIndex[server] = res.FirstIndex
	}

	go rf.updateCommitIndex()
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
*           Event           *
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

func (rf *Raft) HeartBeat() error {
	res := make(chan struct{}, len(rf.peers)-1)
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var response AppendEntriesResponse
				if rf.sendAppendEntries(server, rf.NewHeartBeat(), &response) {
					res <- struct{}{}
				}
			}(i)
		}
	}
	j := 1
	timeout := time.NewTimer(100 * time.Millisecond)
	for {
		select {
		case <-res:
			j++
			if j > len(rf.peers)/2 {
				return nil
			}
		case <-timeout.C:
			timeout.Stop()
			return errors.New("leader can't exchange heartbeat with a majority of the cluster")
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()

	sorted := make([]uint64, len(rf.matchIndex))
	copy(sorted, rf.matchIndex)
	sort.Sort(UintSlice(sorted))

	index := sorted[len(sorted)/2]
	if rf.commitIndex < index && rf.log.Entry(index).Term == rf.currentTerm {
		rf.commitIndex = index
		go rf.apply()
	}
	//logger.Printf("commit index of Leader: %v", rf.commitIndex)
	//logger.Printf("entries of Leader: %v", rf.log.Entries)
	rf.mu.Unlock()
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	var applyMsg []ApplyMsg
	for rf.lastApplied < rf.commitIndex {
		//log.Printf("commit index of server %v: %v", rf.ID, rf.commitIndex)
		//if rf.state == Leader {
		//log.Printf("log of server %v: %v", rf.ID, rf.log.Entries)
		//}
		rf.lastApplied++
		apply := rf.log.Apply(rf.lastApplied)
		applyMsg = append(applyMsg, apply)
	}
	rf.mu.Unlock()

	//logger.Printf("apply message of server %v: %v", rf.ID, applyMsg)
	for _, msg := range applyMsg {
		rf.applyCh <- msg
	}
}

/*
*****************************
*        State Loop         *
*****************************
 */

// FollowerLoop is loop of Follower.
// Since Follower can only convert to Candidate, the Follower loop just wait for a election timeout,
// or reset its election timer.
func (rf *Raft) followerLoop() {
	electionTimer := time.NewTimer(rf.electionTimeout)
	for {
		select {
		case <-electionTimer.C:
			electionTimer.Stop()
			rf.convertToCandidate()
			return
		case <-rf.resetCh:
			if !electionTimer.Stop() {
				<-electionTimer.C
			}
			electionTimer.Reset(rf.electionTimeout)
		}
	}
}

// CandidateLoop is loop of Candidate.
// Candidate will have the following action:
// 1) If votes received from majority of servers: become Leader.
// 2) If AppendEntries RPC received from new Leader: convert to Follower.
// 3) If election timeout elapses: start new election.
func (rf *Raft) candidateLoop() {
	votes := 1
	voteCh := make(chan struct{}, len(rf.peers)-1)
	electionTicker := time.NewTicker(rf.electionTimeout)
	rf.electLeader(voteCh)

Loop:
	for {
		select {
		case <-voteCh:
			votes++
			if votes > len(rf.peers)/2 {
				rf.convertToLeader()
				break Loop
			}
		case <-electionTicker.C:
			rf.convertToCandidate()
			break Loop
		case <-rf.resetCh:
			break Loop
		}
	}
	electionTicker.Stop()
}

// LeaderLoop is loop of Leader.
// Leader will convert to Follower if it receives a request or response containing a higher term.
func (rf *Raft) leaderLoop() {
	heartBeatTicker := time.NewTicker(HeartBeatInterval)
	for {
		select {
		case <-heartBeatTicker.C:
			rf.broadcast()
		case <-rf.resetCh:
			heartBeatTicker.Stop()
			return
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the Leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the Leader
// may fail or lose an electLeader. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the Leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	index := rf.log.LastIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		rf.log.AddEntry(rf.currentTerm, command)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	rf.mu.Unlock()
	return int(index), int(term), isLeader
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
	rf.ID = RandomID(8)
	rf.state = Follower
	rf.log = NewLog()
	rf.resetCh = make(chan struct{})

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimeout = time.Millisecond * time.Duration(400+rand.Intn(200))

	go func() {
		for {
			switch rf.State() {
			case Leader:
				rf.leaderLoop()
			case Candidate:
				rf.candidateLoop()
			case Follower:
				rf.followerLoop()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
