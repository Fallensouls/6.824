package raft

import (
	"log"
	"sync"
	"time"
)

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command interface{}
}

type LogState struct {
	Entries           []LogEntry
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	MaxSize           uint64
}

type Log struct {
	mu          sync.RWMutex
	state       *LogState
	commitIndex uint64
	lastApplied uint64
}

func newLog() *Log {
	return &Log{state: &LogState{Entries: make([]LogEntry, 0)}}
}

func (l *Log) setMaxSize(maxSize uint64) {
	l.state.MaxSize = maxSize
}

func (l *Log) maxSize() uint64 {
	return l.state.MaxSize
}

func (l *Log) CommitIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.commitIndex
}

func (l *Log) LastApplied() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastApplied
}

func (l *Log) lastIncludedTerm() uint64 {
	return l.state.LastIncludedTerm
}

func (l *Log) setLastIncludedTerm(term uint64) {
	l.state.LastIncludedTerm = term
}

func (l *Log) lastIncludedIndex() uint64 {
	return l.state.LastIncludedIndex
}

func (l *Log) setLastIncludedIndex(index uint64) {
	l.state.LastIncludedIndex = index
}

func (l *Log) addEntry(term uint64, command interface{}) {
	index := l.lastIndex() + 1
	l.state.Entries = append(l.state.Entries, LogEntry{index, term, command})
}

func (l *Log) addNoOpEntry(term uint64) {
	index := l.lastIndex() + 1
	l.state.Entries = append(l.state.Entries, LogEntry{index, term, nil})
}

func (l *Log) lastEntry() *LogEntry {
	if l != nil && len(l.state.Entries) > 0 {
		return &l.state.Entries[len(l.state.Entries)-1]
	}
	return nil
}

func (l *Log) lastTerm() uint64 {
	entry := l.lastEntry()
	if entry != nil {
		return entry.Term
	}
	return l.state.LastIncludedTerm
}

func (l *Log) lastIndex() uint64 {
	entry := l.lastEntry()
	if entry != nil {
		return entry.Index
	}
	return l.state.LastIncludedIndex
}

func (l *Log) entry(index uint64) *LogEntry {
	//log.Printf("last index: %v, index: %v", l.LastIndex(), index)
	if index <= l.state.LastIncludedIndex || l.lastIndex() < index {
		return nil
	}
	storageIndex := index - l.state.LastIncludedIndex - 1
	return &l.state.Entries[storageIndex]
}

func (l *Log) entriesAfter(index uint64) []LogEntry {
	if index < l.state.LastIncludedIndex || l.lastIndex() < index {
		return nil
	}

	if index == l.state.LastIncludedIndex {
		return l.state.Entries
	}

	storageIndex := index - l.state.LastIncludedIndex
	entries := make([]LogEntry, len(l.state.Entries)-int(storageIndex))
	copy(entries, l.state.Entries[storageIndex:])
	return entries
}

func (l *Log) applyMessage(index uint64, recover bool) ApplyMsg {
	command := l.entry(index).Command
	var noop bool
	if command == nil {
		command = 1
		noop = true
	}
	applyMsg := ApplyMsg{
		CommandValid: true,
		CommandIndex: int(index),
		Command:      command,
		NoOpCommand:  noop,
		Recover:      recover,
	}
	return applyMsg
}

func (l *Log) searchFirstIndex(index, term uint64) (firstIndex uint64) {
	storageIndex := int(index - l.state.LastIncludedIndex - 1)
	for i := storageIndex; i >= 0; i-- {
		if l.state.Entries[i].Term < term {
			firstIndex = l.state.Entries[i].Index + 1
			return
		}
	}
	return 1
}

func (l *Log) discardLogBefore(index uint64) {
	if index <= l.state.LastIncludedIndex {
		return
	}
	if l.lastIndex() < index {
		l.state.Entries = make([]LogEntry, 0)
		return
	}
	storageIndex := index - l.state.LastIncludedIndex - 1
	l.state.Entries = l.state.Entries[storageIndex:]
}

/*
****************************
*       Request Vote       *
****************************
 */

func (l *Log) lastIndexAndTerm() (uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	entry := l.lastEntry()
	if entry != nil {
		return entry.Index, entry.Term
	}
	return l.state.LastIncludedIndex, l.state.LastIncludedTerm
}

func (l *Log) isUpToDate(req *RequestVoteRequest) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.lastTerm() < req.LastLogTerm {
		return true
	}
	if l.lastTerm() == req.LastLogTerm && l.lastIndex() <= req.LastLogIndex {
		return true
	}
	return false
}

/*
*****************************
*      Append Entries       *
*****************************
 */

func (l *Log) prevLogTermAndNewEntries(prevLogIndex uint64) (uint64, []LogEntry) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var prevLogTerm uint64
	if prevLogIndex == l.lastIncludedIndex() {
		prevLogTerm = l.lastIncludedTerm()
	} else {
		entry := l.entry(prevLogIndex)
		if entry != nil {
			prevLogTerm = entry.Term
		}
	}
	return prevLogTerm, l.entriesAfter(prevLogIndex)
}

func (l *Log) appendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	if !(l.lastIncludedIndex() == req.PrevLogIndex && l.lastIncludedTerm() == req.PrevLogTerm) {
		// reply false if receiver's log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
		entry := l.entry(req.PrevLogIndex)

		// Leader has more logs, let nextIndex be the last index of receiver plus one.
		if entry == nil {
			res.FirstIndex = l.lastIndex() + 1
			return
		}

		// there are conflicting entries
		if entry.Term != req.PrevLogTerm {
			//res.FirstIndex = rf.log.searchFirstIndex(req.PrevLogIndex, entry.Term)
			res.FirstIndex = req.PrevLogIndex - 1
			res.ConflictTerm = entry.Term
			return
		}
	}

	// now receiver can reply true since entry matches successfully.
	res.Success = true

	//logger.Printf("last index of server %v: %v", rf.ID, rf.log.LastIndex())

	// if the last log entry matches prevLogIndex and prevLogTerm
	if l.lastIndex() == req.PrevLogIndex {
		l.state.Entries = append(l.state.Entries, req.Entries...)
	} else {
		storageIndex := req.PrevLogIndex - l.state.LastIncludedIndex
		l.state.Entries = l.state.Entries[:storageIndex]
		l.state.Entries = append(l.state.Entries, req.Entries...)
	}
}

/*
*****************************
*     Install Snapshot      *
*****************************
 */

func (l *Log) lastIncludedIndexAndTerm() (uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state.LastIncludedIndex, l.state.LastIncludedTerm
}

func (l *Log) installSnapshot(req *InstallSnapshotRequest) {
	// update lastIncludedIndex and LastIncludedTerm
	l.setLastIncludedIndex(req.LastIncludedIndex)
	l.setLastIncludedTerm(req.LastIncludedTerm)

	// write Data into snapshot file at given offset
	l.discardLogBefore(req.LastIncludedIndex + 1)
}

func (l *Log) apply(applyCh chan<- ApplyMsg, recover uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var applyMsg []ApplyMsg
	lastApplied := l.lastApplied
	for lastApplied < l.commitIndex {
		lastApplied++
		msg := l.applyMessage(lastApplied, lastApplied < recover)
		applyMsg = append(applyMsg, msg)
	}
	//log.Printf("handleLog message of server %v: %v", rf.ID, applyMsg)
	//log.Printf("log of server %v: %v", rf.ID, rf.log.Entries)
	if len(applyMsg) != 0 {
	loop:
		for _, msg := range applyMsg {
			select {
			case <-time.After(100 * time.Millisecond):
				break loop
			default:
				applyCh <- msg
				l.lastApplied = uint64(msg.CommandIndex)
				//log.Printf("handleLog index of server %v: %v", rf.ID, rf.lastApplied)
			}
		}
		//log.Printf("handleLog index of server %v: %v", rf.ID, rf.lastApplied)
	}
}

func (l *Log) createSnapshot(snapshot Snapshot) (success bool) {
	if snapshot.Index > l.lastApplied {
		log.Panicf("can not create snapshot with log entries that haven't been applied")
	}
	if snapshot.Index > l.lastIncludedIndex() {
		term := l.entry(snapshot.Index).Term
		l.discardLogBefore(snapshot.Index + 1)
		l.setLastIncludedIndex(snapshot.Index)
		l.setLastIncludedTerm(term)
		return true
	}
	return
}
