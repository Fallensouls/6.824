# Raft
Raft is a consensus algorithm for managing a replicated log. It
produces a result equivalent to (multi-)Paxos with different structure. Please
Read [extended Raft paper](https://raft.github.io/raft.pdf) for more details.


## Lab 2
Lab 2 is not so hard because section 5 of Raft paper provides enough details. 
The most important things are:
1. Implement RequestVote RPC for a correct leader election.
2. Implement AppendEntries RPC with no log entries, namely heartbeats.
This will help the leader prevent election timeout.
3. Now the state machine runs correctly. Note that server's state should
be maintained all the time, a single goroutine for it may be a good choice.
4. Finally, consider how to deal with log entries. That's hardest part of the
whole algorithm. It is worthwhile to spend most time implementing a replicated log service.

Obviously, Lab 2 is not a complete Raft. If you are not familiar with Raft, this lab will help a lot.   
