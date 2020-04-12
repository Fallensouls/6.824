# Raft
Raft is a consensus algorithm for managing a replicated log. It
produces a result equivalent to (multi-)Paxos with different structure. Please
Read [extended Raft paper](https://raft.github.io/raft.pdf) for more details.


## Lab 2
Lab 2要求实现的是一个基础的raft算法，与原论文相比缺少了以下细节：
1. 每次Leader上任时，先往log中写入一个no-op log，用于保证之前已被复制到大多数节点的log能够commit。因为raft算法要求leader只能commit当前任期的log，所以这一步是必须的。
2. 不写入log的读操作。事实上读操作是可以直接进行处理的，只要leader通过与大多数节点的交流确认自己依然是leader，就可以保证线性一致的读操作。对这部分的优化可以参见租约机制。
3. Pre-Vote。网络分区时，少数节点由于无法赢得选举，会进行无意义的term自增操作，导致term出现暴涨。网络分区结束后，这类节点的重新加入会导致当前的leader被强行终止，重新进行选举（因为leader会收到一个比它大的term）。使用pre-vote机制可以有效避免这个问题。

本项目将上述细节纳入了实现范畴，更加符合论文对raft算法的构想。
