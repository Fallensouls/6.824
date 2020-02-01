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
	"sync/atomic"
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
	Recover      bool
}

type State = uint32

const (
	Leader State = iota
	PreCandidate
	Candidate
	Follower
)

type CommandType uint8

const (
	Read CommandType = iota
	Write
)

type MessageType uint8

const (
	PreRequestVote = iota
	RequestVote
	AppendEntries
)

const HeartBeatInterval = 40 * time.Millisecond // 40mS

var (
	ErrNotLeader   = errors.New("not a leader")
	ErrPartitioned = errors.New("network partitions")
	ErrTimeout     = errors.New("timeout")
	ErrShutdown    = errors.New("server shutdown")
)

type Snapshot struct {
	Index uint64
	Data  []byte
}

type Event struct {
	request  interface{}
	response interface{}
	done     chan struct{}
}

type Transition struct {
	State
	done chan struct{}
}

type Request struct {
	to      int
	content interface{}
}

type Message struct {
	MessageType
	requests []Request
	created  chan struct{}
}

type Response struct {
	from    int
	content interface{}
	ch      chan struct{}
}

type Command struct {
	CommandType
	content  interface{}
	response chan interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// config
	electionTimeout   time.Duration
	heartBeatInterval time.Duration
	preVote           bool

	ID     string
	state  State
	leader string // id of the node which is considered as leader

	// persistent state on all servers.
	currentTerm uint64
	votedFor    string
	*Log

	// volatile state on leaders.
	nextIndex  []uint64
	matchIndex []uint64

	// records
	recover       uint64    // last applied index before the server crashes
	lastHeartBeat time.Time // timestamp of last heartbeat

	// signals
	cmd               chan *Command
	event             chan *Event
	msg               chan *Message
	res               chan Response
	trans             chan *Transition
	SnapshotCh        chan struct{}
	InstallSnapshotCh chan uint64
	SnapshotData      chan Snapshot
	resetCh           chan struct{} // signal for converting to Follower and reset election ticker
	applyReqCh        chan struct{}
	applyCh           chan ApplyMsg
	shutdown          chan struct{} // shutdown raft node
}

/*
*****************************
*          General          *
*****************************
 */

// return currentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.getCurrentTerm()), atomic.LoadUint32(&rf.state) == Leader
}

func (rf *Raft) State() State {
	return atomic.LoadUint32(&rf.state)
}

func (rf *Raft) setState(state State) {
	atomic.StoreUint32(&rf.state, state)
}

func (rf *Raft) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&rf.currentTerm)
}

func (rf *Raft) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&rf.currentTerm, term)
}

func (rf *Raft) sendTrans(trans *Transition) {
	rf.trans <- trans
	<-trans.done
}

func (rf *Raft) SetMaxSize(maxSize int) {
	if maxSize < 0 {
		maxSize = 0
	}
	rf.setMaxSize(uint64(maxSize))
}

/*
*****************************
*      State Transition     *
*****************************
 */

func (rf *Raft) convertToFollower(term uint64) {
	rf.setState(Follower)
	if rf.getCurrentTerm() < term {
		rf.setCurrentTerm(term)
		rf.votedFor = ``
	}

	logger.Printf("Follower ID:%v\n", rf.ID)
	logger.Printf("Follower term: %d\n", rf.currentTerm)
}

func (rf *Raft) convertToPreCandidate() {
	rf.setState(PreCandidate)

	logger.Printf("Pre-candidate ID:%v\n", rf.ID)
	logger.Printf("Pre-candidate term: %d\n", rf.currentTerm)
}

func (rf *Raft) convertToCandidate() {
	atomic.AddUint64(&rf.currentTerm, 1)
	rf.setState(Candidate)
	rf.votedFor = rf.ID

	logger.Printf("Candidate ID:%v\n", rf.ID)
	logger.Printf("Candidate term: %d\n", rf.currentTerm)
}

func (rf *Raft) convertToLeader() {
	rf.setState(Leader)
	rf.votedFor = ``
	rf.nextIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))

	// add a no-op entry to local log.
	rf.mu.Lock()
	rf.addEntry(rf.getCurrentTerm(), nil)
	rf.matchIndex[rf.me] = rf.lastIndex()

	// since there is a no-op log, next index can be the index of this no-op log
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastIndex()
	}

	rf.persist()
	rf.mu.Unlock()

	logger.Printf("Leader ID:%v\n", rf.ID)
	logger.Printf("Leader term: %d\n", rf.currentTerm)
}

/*
*****************************
*        Persistence        *
*****************************
 */

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Log.state)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:`
	state := rf.encodeState()
	rf.persister.SaveRaftState(state)
}

// restore previously persisted state.
func (rf *Raft) restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(rf.Log.state)
}

func (rf *Raft) handleRequest(req interface{}, res interface{}) {
	event := &Event{request: req, response: res, done: make(chan struct{})}
	rf.event <- event
	<-event.done

	logger.Printf("response of server %v: %v", rf.ID, res)
}

func (rf *Raft) RequestVote(req *RequestVoteRequest, res *RequestVoteResponse) {
	rf.handleRequest(req, res)
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	rf.handleRequest(req, res)
}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, res *InstallSnapshotResponse) {
	rf.handleRequest(req, res)
}

/*
****************************
*       Request Vote       *
****************************
 */

// example handleRequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteRequest struct {
	// Your Data here (2A, 2B).
	PreVote      bool
	Term         uint64 // Candidate's term
	CandidateId  string // Candidate's ID
	LastLogIndex uint64 // index of Candidate's last log entry
	LastLogTerm  uint64 // term of Candidate's last log entry
}

// example handleRequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteResponse struct {
	// Your Data here (2A).
	Term        uint64 // receiver's currentTerm
	VoteGranted bool   // true if Candidate can receive vote
}

func (rf *Raft) newVoteRequest(preVote bool) *RequestVoteRequest {
	index, term := rf.lastIndexAndTerm()
	return &RequestVoteRequest{
		preVote,
		rf.currentTerm,
		rf.ID,
		index,
		term,
	}
}

// example handleRequestVote RPC handler.
func (rf *Raft) handleRequestVote(req *RequestVoteRequest, res *RequestVoteResponse) {
	// return receiver's currentTerm for Candidate to update itself.
	res.Term = rf.currentTerm

	// reply false if Candidate's term is less than receiver's currentTerm.
	// reply false when receiver is also a Candidate or has voted for another Candidate.
	if rf.currentTerm > req.Term {
		return
	}

	if rf.currentTerm < req.Term {
		rf.convertToFollower(req.Term)
		rf.resetCh <- struct{}{}
	} else if !req.PreVote && rf.votedFor != `` && rf.votedFor != req.CandidateId {
		return
	}

	// check whether Candidate's log is at least as up-to-date as receiver's log.
	upToDate := rf.isUpToDate(req)
	canVote := rf.votedFor == `` || rf.votedFor == req.CandidateId || req.PreVote
	// if receiver is not a Candidate and upToDate is true, the Candidate will receive a vote.
	if canVote && upToDate && time.Now().Sub(rf.lastHeartBeat) > rf.heartBeatInterval/2 {
		//log.Printf("%s voted for: %s", rf.ID, req.CandidateId)
		if !req.PreVote {
			rf.votedFor = req.CandidateId
		}
		res.VoteGranted = true
	}

	// persistent state should be persisted before responding to RPCs.
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) handleVoteResponse(res RequestVoteResponse, voteCh chan<- struct{}) {

	if rf.currentTerm < res.Term {
		rf.convertToFollower(res.Term)
		rf.resetCh <- struct{}{}
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
	ConflictIndex uint64
	ConflictTerm  uint64
}

func (rf *Raft) newAppendEntriesRequest(server int) *AppendEntriesRequest {

	var (
		prevLogIndex uint64
		prevLogTerm  uint64
	)

	prevLogIndex = rf.nextIndex[server] - 1
	prevLogTerm, entries := rf.prevLogTermAndNewEntries(prevLogIndex)

	return &AppendEntriesRequest{
		rf.currentTerm,
		rf.ID,
		prevLogIndex,
		prevLogTerm,
		entries,
		rf.commitIndex,
	}
}

func (rf *Raft) newHeartBeat() *AppendEntriesRequest {

	return &AppendEntriesRequest{
		rf.currentTerm,
		rf.ID,
		0,
		1,
		nil,
		0,
	}
}

func (rf *Raft) handleAppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {

	//logger.Printf("server %v receives request: %v", rf.ID, req)

	// return receiver's currentTerm for Candidate to update itself.
	res.Term = rf.currentTerm

	// reply false if Candidate's term is less than receiver's currentTerm.
	if rf.currentTerm > req.Term {
		return
	}

	// If receiver's currentTerm is not greater than Candidate's term, receiver should update itself and convert to Follower
	rf.convertToFollower(req.Term)
	rf.lastHeartBeat = time.Now()
	rf.leader = req.LeaderId
	rf.resetCh <- struct{}{}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.appendEntries(req, res)
	if !res.Success {
		return
	}

	if rf.commitIndex < req.LeaderCommit {
		rf.commitIndex = min(req.LeaderCommit, rf.lastIndex())
	}

	// not a heartbeat
	if req.Entries != nil {
		res.Index = req.Entries[len(req.Entries)-1].Index
	}

	rf.applyReqCh <- struct{}{}
	rf.persist()
}

func (rf *Raft) handleAppendEntriesResponse(server int, res AppendEntriesResponse) {

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
		rf.nextIndex[server] = res.ConflictIndex
		if res.ConflictTerm > 0 {
			lastIndex := rf.searchLastIndex(res.ConflictTerm)
			if lastIndex > 0 {
				rf.nextIndex[server] = lastIndex + 1
			}
		}
	}
	rf.updateCommitIndex()
}

/*
*****************************
*     Install Snapshot      *
*****************************
 */
type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Offset            uint64
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term uint64
}

func (rf *Raft) newInstallSnapshotRequest() *InstallSnapshotRequest {

	index, term := rf.lastIncludedIndexAndTerm()
	return &InstallSnapshotRequest{
		rf.currentTerm,
		rf.ID,
		index,
		term,
		0,
		rf.persister.ReadSnapshot(),
		true,
	}
}

// handleInstallSnapshot rpc will be send when the leader has already
// discarded the next log entry that it needs to send to a follower.
func (rf *Raft) handleInstallSnapshot(req *InstallSnapshotRequest, res *InstallSnapshotResponse) {

	//log.Printf("server %s recieves snapshot rpc: %v", rf.ID, req)
	res.Term = rf.currentTerm
	if rf.currentTerm <= req.Term {
		rf.convertToFollower(req.Term)
		rf.lastHeartBeat = time.Now()
		rf.leader = req.LeaderID
		rf.resetCh <- struct{}{}
	}

	// reject old snapshot
	if req.LastIncludedIndex < rf.lastIncludedIndex() {
		return
	}

	// save snapshot
	rf.mu.Lock()
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), req.Data)
	rf.installSnapshot(req)
	rf.commitIndex = req.LastIncludedIndex
	rf.lastApplied = req.LastIncludedIndex
	rf.InstallSnapshotCh <- rf.lastIncludedIndex()
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) handleInstallSnapshotResponse(server int, res InstallSnapshotResponse) {

	if rf.currentTerm < res.Term {
		rf.convertToFollower(res.Term)
		rf.resetCh <- struct{}{}
		return
	}
	rf.mu.RLock()
	rf.matchIndex[server] = rf.lastIncludedIndex()
	rf.nextIndex[server] = rf.lastIncludedIndex() + 1
	rf.mu.RUnlock()
}

//
// example code to send a handleRequestVote RPC to a server.
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
// Call() sends a content and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost content, or a lost reply.
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

func (rf *Raft) sendRequestVote(server int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", request, response)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", request, response)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", request, response)
	return ok
}

/*
*****************************
*           Event           *
*****************************
 */

func (rf *Raft) electLeader(msgType MessageType, voteCh chan struct{}) {
	msg := &Message{MessageType: msgType, created: make(chan struct{})}
	rf.msg <- msg
	<-msg.created
	req := msg.requests[0].content.(*RequestVoteRequest)

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var response RequestVoteResponse
				if rf.sendRequestVote(server, req, &response) {
					rf.res <- Response{content: response, ch: voteCh}
				}
			}(i)
		}
	}
}

func (rf *Raft) broadcast(requests []Request) {
	for _, req := range requests {
		go func(r Request) {
			switch content := r.content.(type) {
			case *AppendEntriesRequest:
				var res AppendEntriesResponse
				if rf.sendAppendEntries(r.to, content, &res) {
					rf.res <- Response{from: r.to, content: res}
				}
			case *InstallSnapshotRequest:
				var res InstallSnapshotResponse
				if rf.sendInstallSnapshot(r.to, content, &res) {
					rf.res <- Response{from: r.to, content: res}
				}
			}
		}(req)
	}
}

func (rf *Raft) heartBeat() error {
	res := make(chan struct{}, len(rf.peers)-1)
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var response AppendEntriesResponse
				if rf.sendAppendEntries(server, rf.newHeartBeat(), &response) {
					res <- struct{}{}
				}
			}(i)
		}
	}
	j := 1
	timeout := time.NewTimer(time.Second)
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

	sorted := make([]uint64, len(rf.matchIndex))
	copy(sorted, rf.matchIndex)
	sort.Sort(UintSlice(sorted))

	index := sorted[len(sorted)/2]
	rf.mu.Lock()
	if rf.commitIndex < index && rf.entry(index).Term == rf.currentTerm {
		rf.commitIndex = index
		rf.applyReqCh <- struct{}{}
	}
	rf.mu.Unlock()

}

func (rf *Raft) needSnapshot() bool {
	return rf.maxSize() != 0 && rf.persister.RaftStateSize() >= int(rf.maxSize())
}

func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
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
		case <-rf.shutdown:
			return
		case <-rf.resetCh:
			logger.Printf("server %v reset", rf.ID)
			if !electionTimer.Stop() {
				<-electionTimer.C
			}
			electionTimer.Reset(rf.electionTimeout)
		case <-electionTimer.C:
			logger.Printf("server %v timeout", rf.ID)
			electionTimer.Stop()
			var state State
			if rf.preVote {
				state = PreCandidate
			} else {
				state = Candidate
			}
			rf.sendTrans(&Transition{state, make(chan struct{})})
			return
		}
	}
}

func (rf *Raft) preCandidateLoop() {
	votes := 1
	voteCh := make(chan struct{}, len(rf.peers)-1)
	timeout := time.NewTimer(5 * rf.heartBeatInterval)
	rf.electLeader(PreRequestVote, voteCh)

	for {
		select {
		case <-rf.shutdown:
			return
		case <-rf.resetCh:
			return
		case <-timeout.C:
			timeout.Stop()
			rf.sendTrans(&Transition{Follower, make(chan struct{})})
			return
		case <-voteCh:
			votes++
			logger.Printf("vote of server %v: %v", rf.ID, votes)
			if votes > len(rf.peers)/2 {
				rf.sendTrans(&Transition{Candidate, make(chan struct{})})
				return
			}
		}
	}
}

// CandidateLoop is loop of Candidate.
// Candidate will have the following action:
// 1) If votes received from majority of servers: become Leader.
// 2) If handleAppendEntries RPC received from new Leader: convert to Follower.
// 3) If election timeout elapses: start new election.
func (rf *Raft) candidateLoop() {
	votes := 1
	voteCh := make(chan struct{}, len(rf.peers)-1)
	timeout := time.NewTimer(rf.electionTimeout)
	rf.electLeader(RequestVote, voteCh)

	for {
		select {
		case <-rf.shutdown:
			return
		case <-rf.resetCh:
			return
		case <-timeout.C:
			timeout.Stop()
			rf.sendTrans(&Transition{Candidate, make(chan struct{})})
			rf.electionTimeout = time.Millisecond * time.Duration(400+rand.Intn(200)) // reset election timeout
			return
		case <-voteCh:
			votes++
			logger.Printf("vote of server %v: %v", rf.ID, votes)
			if votes > len(rf.peers)/2 {
				rf.sendTrans(&Transition{Leader, make(chan struct{})})
				return
			}
		}
	}
}

// LeaderLoop is loop of Leader.
// Leader will convert to Follower if it receives a content or response containing a higher term.
func (rf *Raft) leaderLoop() {
	heartBeatTicker := time.NewTicker(rf.heartBeatInterval)
	for {
		select {
		case <-rf.shutdown:
			return
		case <-rf.resetCh:
			logger.Printf("server %v reset", rf.ID)
			heartBeatTicker.Stop()
			return
		case <-heartBeatTicker.C:
			msg := &Message{MessageType: AppendEntries, created: make(chan struct{})}
			rf.msg <- msg
			<-msg.created
			//logger.Printf("server %v broadcast", rf.ID)
			rf.broadcast(msg.requests)
		}
	}
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.shutdown:
			return
		case trans := <-rf.trans:
			//logger.Printf("server %v change state", rf.ID)
			switch trans.State {
			case Follower:
				rf.convertToFollower(rf.currentTerm)
			case PreCandidate:
				rf.convertToPreCandidate()
			case Candidate:
				rf.convertToCandidate()
			case Leader:
				rf.convertToLeader()
			}
			trans.done <- struct{}{}
		case msg := <-rf.msg:
			//logger.Printf("server %v create request", rf.ID)
			switch msg.MessageType {
			case PreRequestVote:
				msg.requests = append(msg.requests, Request{content: rf.newVoteRequest(true)})
			case RequestVote:
				msg.requests = append(msg.requests, Request{content: rf.newVoteRequest(false)})
			case AppendEntries:
				rf.mu.RLock()
				for i := range rf.peers {
					if i != rf.me {
						if rf.nextIndex[i] <= rf.lastIncludedIndex() && rf.lastIncludedIndex() != 0 {
							msg.requests = append(msg.requests, Request{to: i, content: rf.newInstallSnapshotRequest()})
						} else {
							msg.requests = append(msg.requests, Request{to: i, content: rf.newAppendEntriesRequest(i)})
						}
					}
				}
				rf.mu.RUnlock()
			}
			msg.created <- struct{}{}
		case cmd := <-rf.cmd:
			//logger.Printf("server %v execute command", rf.ID)
			switch cmd.CommandType {
			case Write:
				res := rf.write(cmd.content)
				cmd.response <- res
			case Read:
				go func() {
					err := rf.read()
					cmd.response <- err
				}()
			}
		case e := <-rf.event:
			logger.Printf("server %v handle request", rf.ID)
			logger.Printf("server %v receives request: %v", rf.ID, e.request)
			switch req := e.request.(type) {
			case *RequestVoteRequest:
				rf.handleRequestVote(req, e.response.(*RequestVoteResponse))
			case *AppendEntriesRequest:
				rf.handleAppendEntries(req, e.response.(*AppendEntriesResponse))
			case *InstallSnapshotRequest:
				rf.handleInstallSnapshot(req, e.response.(*InstallSnapshotResponse))
			}
			e.done <- struct{}{}
		case r := <-rf.res:
			//log.Printf("server %v handle response", rf.ID)
			switch res := r.content.(type) {
			case RequestVoteResponse:
				rf.handleVoteResponse(res, r.ch)
			case AppendEntriesResponse:
				rf.handleAppendEntriesResponse(r.from, res)
			case InstallSnapshotResponse:
				rf.handleInstallSnapshotResponse(r.from, res)
			}
		}
	}
}

func (rf *Raft) runLog() {
	for {
		select {
		case <-rf.shutdown:
			return
		case <-rf.applyReqCh:
			rf.apply(rf.applyCh, rf.recover)
			if rf.needSnapshot() {
				rf.SnapshotCh <- struct{}{}
				snapshot := <-rf.SnapshotData
				// log.Printf("server %v creates snapshot at index %v", rf.ID, snapshot.Index)
				rf.mu.Lock()
				if rf.createSnapshot(snapshot) {
					rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot.Data)
				}
				rf.mu.Unlock()
			}
		}
	}
}

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
type CommandResponse struct {
	index    int
	term     int
	isLeader bool
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	select {
	case <-rf.shutdown:
		return -1, 0, false
	default:
		cmd := &Command{Write, command, make(chan interface{})}
		rf.cmd <- cmd
		select {
		case response := <-cmd.response:
			res := response.(CommandResponse)
			return res.index, res.term, res.isLeader
		case <-time.After(100 * time.Millisecond):
			return int(rf.lastIndex() + 1), int(rf.currentTerm), rf.state == Leader
		}
	}
}

func (rf *Raft) write(command interface{}) CommandResponse {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.lastIndex() + 1
	term := rf.currentTerm
	isLeader := rf.State() == Leader

	if isLeader {
		rf.addEntry(rf.currentTerm, command)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	return CommandResponse{index: int(index), term: int(term), isLeader: isLeader}
}

func (rf *Raft) Read() error {
	select {
	case <-rf.shutdown:
		return ErrShutdown
	default:
		cmd := &Command{Read, nil, make(chan interface{})}
		rf.cmd <- cmd
		err := <-cmd.response
		if err != nil {
			return err.(error)
		}
		return nil
	}
}

func (rf *Raft) read() error {
	readIndex := rf.CommitIndex()
	if readIndex < rf.recover {
		readIndex = rf.recover
	}

	if err := rf.heartBeat(); err != nil {
		return ErrPartitioned
	}
	if rf.State() != Leader {
		return ErrNotLeader
	}
	done := make(chan struct{})
	go func() {
		for rf.LastApplied() < readIndex {
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
		return nil
	case <-time.After(5 * rf.heartBeatInterval):
		return ErrTimeout
	}
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.shutdown)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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
	rf.Log = newLog()
	rf.cmd = make(chan *Command)
	rf.event = make(chan *Event)
	rf.msg = make(chan *Message)
	rf.res = make(chan Response)
	rf.trans = make(chan *Transition)
	rf.resetCh = make(chan struct{})
	rf.SnapshotCh = make(chan struct{})
	rf.InstallSnapshotCh = make(chan uint64)
	rf.SnapshotData = make(chan Snapshot)
	rf.applyReqCh = make(chan struct{}, 20)
	rf.shutdown = make(chan struct{})

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimeout = time.Millisecond * time.Duration(400+rand.Intn(200))
	//log.Printf("timeout of server %v: %v", rf.ID, rf.electionTimeout)
	rf.heartBeatInterval = 40 * time.Millisecond
	rf.preVote = true

	go func() {
		for {
			select {
			case <-rf.shutdown:
				return
			default:
				switch rf.State() {
				case Leader:
					rf.leaderLoop()
				case PreCandidate:
					rf.preCandidateLoop()
				case Candidate:
					rf.candidateLoop()
				case Follower:
					rf.followerLoop()
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.restore(persister.ReadRaftState())
	rf.commitIndex = rf.lastIncludedIndex()
	rf.lastApplied = rf.lastIncludedIndex()
	rf.recover = rf.lastIndex()

	go rf.runLog()
	go rf.run()
	return rf
}
