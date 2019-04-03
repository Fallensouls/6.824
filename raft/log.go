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

func (l *Log) LastLog() *LogEntry {
	if l != nil && len(l.entries) > 0 {
		return &l.entries[len(l.entries)-1]
	}
	return nil
}

func (l *Log) LastIncludedTerm() uint64 {
	return l.lastIncludedTerm
}

func (l *Log) LastIncludedIndex() uint64 {
	return l.lastIncludedIndex
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
	if index <= l.lastIncludedIndex || l.LastIndex() < index {
		return nil
	}
	storageIndex := index - l.lastIncludedIndex - 1
	return l.entries[storageIndex:]
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
