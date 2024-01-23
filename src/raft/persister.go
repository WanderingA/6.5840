package raft

//
// 支持 Raft 和 kvraft，
// 以保存持久的 Raft 状态（日志和数据）和 k/v 服务器快照。
//
// 我们将使用原始的 persister.go 来测试您的代码，以便进行评分。
// 因此，虽然您可以修改这段代码来帮助调试，但在提交之前，请使用原始代码进行测试。
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte					// 用于保存 Raft 的状态
	snapshot  []byte					// 用于保存快照
}
// 该函数用于创建一个 Persister
func MakePersister() *Persister {
	return &Persister{}
}
// 该函数用于复制一个字节切片
func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}
// 该函数用于复制一个 Persister
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}
// 读取raftstate
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}
// 读取raftstate的长度
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

// C
func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}

// 读取快照
func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}
// 读取快照的长度
func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
