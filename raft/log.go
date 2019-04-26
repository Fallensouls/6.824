package raft

type LogEntry struct {
	Index   uint64
	Term    uint64
	Command interface{}
}

type Log struct {
	Entries           []LogEntry
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

func NewLog() *Log {
	return &Log{Entries: make([]LogEntry, 0)}
}

func (l *Log) GetLastIncludedTerm() uint64 {
	return l.LastIncludedTerm
}

func (l *Log) GetLastIncludedIndex() uint64 {
	return l.LastIncludedIndex
}

func (l *Log) AddEntry(term uint64, command interface{}) {
	index := l.LastIndex() + 1
	l.Entries = append(l.Entries, LogEntry{index, term, command})
}

func (l *Log) AddNoOpEntry(term uint64) {
	index := l.LastIndex() + 1
	l.Entries = append(l.Entries, LogEntry{index, term, nil})
}

func (l *Log) LastLog() *LogEntry {
	if l != nil && len(l.Entries) > 0 {
		return &l.Entries[len(l.Entries)-1]
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
	//log.Printf("last index: %v, index: %v", l.LastIndex(), index)
	if index <= l.LastIncludedIndex || l.LastIndex() < index {
		return nil
	}
	storageIndex := index - l.LastIncludedIndex - 1
	return &l.Entries[storageIndex]
}

func (l *Log) EntriesAfter(index uint64) []LogEntry {
	if index < l.LastIncludedIndex || l.LastIndex() < index {
		return nil
	}

	if index == l.LastIncludedIndex {
		return l.Entries
	}

	storageIndex := index - l.LastIncludedIndex
	return l.Entries[storageIndex:]
}

func (l *Log) Apply(index uint64, recover bool) ApplyMsg {
	command := l.Entry(index).Command
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

func (l *Log) SearchFirstIndex(index, term uint64) (firstIndex uint64) {
	storageIndex := int(index - l.LastIncludedIndex - 1)
	for i := storageIndex; i >= 0; i-- {
		if l.Entries[i].Term < term {
			firstIndex = l.Entries[i].Index + 1
			return
		}
	}
	return 1
}
