package raft

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command interface{}
}

type Log struct {
	entries           []LogEntry
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
}

func NewLog() *Log {
	return &Log{entries: make([]LogEntry, 0)}
}

func (l *Log) LastIncludedTerm() uint64 {
	return l.lastIncludedTerm
}

func (l *Log) LastIncludedIndex() uint64 {
	return l.lastIncludedIndex
}

func (l *Log) AddEntry(term uint64, command interface{}) {
	index := l.LastIndex() + 1
	l.entries = append(l.entries, LogEntry{index, term, command})
}

func (l *Log) AddNoOpEntry(term uint64) {
	index := l.LastIndex() + 1
	l.entries = append(l.entries, LogEntry{index, term, nil})
}

func (l *Log) LastLog() *LogEntry {
	if l != nil && len(l.entries) > 0 {
		return &l.entries[len(l.entries)-1]
	}
	return nil
}

func (l *Log) LastTerm() uint64 {
	entry := l.LastLog()
	if entry != nil {
		return entry.Term
	}
	return 0
}

func (l *Log) LastIndex() uint64 {
	entry := l.LastLog()
	if entry != nil {
		return entry.Index
	}
	return 0
}

func (l *Log) Entry(index uint64) *LogEntry {
	if index <= l.lastIncludedIndex || l.LastIndex() < index {
		return nil
	}
	storageIndex := index - l.lastIncludedIndex - 1
	return &l.entries[storageIndex]
}

func (l *Log) EntriesAfter(index uint64) []LogEntry {
	if index < l.lastIncludedIndex || l.LastIndex() < index {
		return nil
	}

	if index == l.lastIncludedIndex {
		return l.entries
	}

	storageIndex := index - l.lastIncludedIndex
	return l.entries[storageIndex:]
}

func (l *Log) Apply(index uint64) ApplyMsg {
	command := l.Entry(index).Command
	if command == nil {
		command = 0
	}
	applyMsg := ApplyMsg{
		CommandValid: true,
		CommandIndex: int(index),
		Command:      command,
	}
	return applyMsg
}

func (l *Log) SearchConflict(index, term uint64) (conflictIndex, conflictTerm uint64) {
	storageIndex := index - l.lastIncludedIndex - 1
	for i := storageIndex; i >= 0; i-- {
		if l.entries[i].Index == index && l.entries[i].Term == term {
			conflictIndex = l.entries[i].Index
			conflictTerm = l.entries[i].Term
			return
		}
	}
	return l.lastIncludedIndex, l.lastIncludedTerm
}
