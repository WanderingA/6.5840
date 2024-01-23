package raft

//
// support for Raft tester.
//
// we will use the original config.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "6.5840/labgob"			
import "6.5840/labrpc"
import "bytes"
import "log"
import "sync"
import "sync/atomic"
import "testing"
import "runtime"
import "math/rand"
import crand "crypto/rand"
import "math/big"
import "encoding/base64"
import "time"
import "fmt"

// 生成 n 个随机字符
func randstring(n int) string {
	b := make([]byte, 2*n)								// 生成 2n 个字节的切片
	crand.Read(b)										// 从随机源中读取随机字节并将其写入 b
	s := base64.URLEncoding.EncodeToString(b)			// 将 b 编码为 base64 字符串
	return s[0:n]										// 返回前 n 个字符
}
// 生成随机种子
func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)					
	bigx, _ := crand.Int(crand.Reader, max)				
	x := bigx.Int64()
	return x
}

type config struct {
	mu          sync.Mutex
	t           *testing.T								// 测试对象
	finished    int32									// 是否完成
	net         *labrpc.Network							// 表示一个网络对象，用于模拟网络通信。
	n           int										// 节点数
	rafts       []*Raft									// 表示一组raft服务器
	applyErr    []string 								// 存储从应用通道读取的错误信息。
	connected   []bool   								// 表示每个服务器是否连接到网络。
	saved       []*Persister							// 每个服务器的持久状态
	endnames    [][]string        					    // 每个服务器发送到的端口文件名。
	logs        []map[int]interface{} 					// copy of each server's committed entries
	lastApplied []int									// 每个服务器的最后应用的索引。
	start       time.Time 								// time at which make_config() was called
	// begin()/end() statistics
	t0        time.Time 								// time at which test_test.go called cfg.begin()
	rpcs0     int       								// 测试开始时的RPC总数。
	cmds0     int      									// 达成一致的命令数量。
	bytes0    int64
	maxIndex  int
	maxIndex0 int										// 测试开始时的最大索引。
}

var ncpu_once sync.Once									// 保证只执行一次
// 创建一个配置对象
func make_config(t *testing.T, n int, unreliable bool, snapshot bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*Raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]interface{}, cfg.n)
	cfg.lastApplied = make([]int, cfg.n)
	cfg.start = time.Now()

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(true)

	applier := cfg.applier
	if snapshot {
		applier = cfg.applierSnap
	}
	// create a full set of Rafts.
	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int]interface{}{}
		cfg.start1(i, applier)
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}
	// fmt.Println(cfg)
	return cfg
}

// 关闭 Raft 服务器，但保存其持久状态。
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // 关闭客户端与服务器的连接。

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// 一个新的持久化器，以防旧实例继续更新持久化器，
	// 但要复制旧持久化器的内容，这样我们就能始终将最后持久化的状态传递给 Make()。
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.rafts[i]
	if rf != nil {
		cfg.mu.Unlock()
		rf.Kill()
		cfg.mu.Lock()
		cfg.rafts[i] = nil
	}

	if cfg.saved[i] != nil {
		raftlog := cfg.saved[i].ReadRaftState()
		snapshot := cfg.saved[i].ReadSnapshot()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].Save(raftlog, snapshot)
	}
}

func (cfg *config) checkLogs(i int, m ApplyMsg) (string, bool) {
	err_msg := ""
	v := m.Command
	for j := 0; j < len(cfg.logs); j++ {
		if old, oldok := cfg.logs[j][m.CommandIndex]; oldok && old != v {
			log.Printf("%v: log %v; server %v\n", i, cfg.logs[i], cfg.logs[j])
			// some server has already committed a different value for this entry!
			err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	_, prevok := cfg.logs[i][m.CommandIndex-1]
	cfg.logs[i][m.CommandIndex] = v
	if m.CommandIndex > cfg.maxIndex {
		cfg.maxIndex = m.CommandIndex
	}
	return err_msg, prevok
}

// applier reads message from apply ch and checks that they match the log
// contents
func (cfg *config) applier(i int, applyCh chan ApplyMsg) {
	for m := range applyCh {
		if m.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			cfg.mu.Lock()
			err_msg, prevok := cfg.checkLogs(i, m)
			cfg.mu.Unlock()
			if m.CommandIndex > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
			}
			if err_msg != "" {
				log.Fatalf("apply error: %v", err_msg)
				cfg.applyErr[i] = err_msg
				// keep reading after error so that Raft doesn't block
				// holding locks...
			}
		}
	}
}

// returns "" or error string  返回 "" 或错误字符串
func (cfg *config) ingestSnap(i int, snapshot []byte, index int) string {
	if snapshot == nil {
		log.Fatalf("nil snapshot")
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []interface{}
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		log.Fatalf("snapshot decode error")
		return "snapshot Decode() error"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", i)
		return err
	}
	cfg.logs[i] = map[int]interface{}{}
	for j := 0; j < len(xlog); j++ {
		cfg.logs[i][j] = xlog[j]
	}
	cfg.lastApplied[i] = lastIncludedIndex
	return ""
}

const SnapShotInterval = 10

// periodically snapshot raft state 定期快照 Raft 状态
func (cfg *config) applierSnap(i int, applyCh chan ApplyMsg) {
	cfg.mu.Lock()
	rf := cfg.rafts[i]
	cfg.mu.Unlock()
	if rf == nil {
		return // ???
	}

	for m := range applyCh {
		err_msg := ""
		if m.SnapshotValid {
			cfg.mu.Lock()
			err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
			cfg.mu.Unlock()
		} else if m.CommandValid {
			if m.CommandIndex != cfg.lastApplied[i]+1 {
				err_msg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", i, cfg.lastApplied[i]+1, m.CommandIndex)
			}

			if err_msg == "" {
				cfg.mu.Lock()
				var prevok bool
				err_msg, prevok = cfg.checkLogs(i, m)
				cfg.mu.Unlock()
				if m.CommandIndex > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			}

			cfg.mu.Lock()
			cfg.lastApplied[i] = m.CommandIndex
			cfg.mu.Unlock()

			if (m.CommandIndex+1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				var xlog []interface{}
				for j := 0; j <= m.CommandIndex; j++ {
					xlog = append(xlog, cfg.logs[i][j])
				}
				e.Encode(xlog)
				rf.Snapshot(m.CommandIndex, w.Bytes())
			}
		} else {
			// Ignore other types of ApplyMsg.
		}
		if err_msg != "" {
			log.Fatalf("apply error: %v", err_msg)
			cfg.applyErr[i] = err_msg
			// keep reading after error so that Raft doesn't block
			// holding locks...
		}
	}
}

// 启动或重新启动raft。
// 如果已经存在，则先 "杀死 "它。
// 分配新的输出端口文件名和一个新的状态保持器，以隔离该服务器的上一个实例。
func (cfg *config) start1(i int, applier func(int, chan ApplyMsg)) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	cfg.lastApplied[i] = 0

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()

		snapshot := cfg.saved[i].ReadSnapshot()
		if snapshot != nil && len(snapshot) > 0 {
			// mimic KV server and process snapshot now.
			// ideally Raft should send it up on applyCh...
			err := cfg.ingestSnap(i, snapshot, -1)
			if err != "" {
				cfg.t.Fatal(err)
			}
		}
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	applyCh := make(chan ApplyMsg)

	rf := Make(ends, i, cfg.saved[i], applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	go applier(i, applyCh)

	svc := labrpc.MakeService(rf)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}
// 检查是否已完成测试。
func (cfg *config) checkFinished() bool {
	z := atomic.LoadInt32(&cfg.finished)
	return z != 0
}

func (cfg *config) cleanup() {
	atomic.StoreInt32(&cfg.finished, 1)
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// 将服务器 I 连接到网络。
func (cfg *config) connect(i int) {
	// fmt.Printf("connect(%d)\n", i)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// 将服务器i从网络中分离。
func (cfg *config) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

// 检查其中一台已连接的服务器是否认为自己是领导者，
// 以及其他已连接的服务器是否认为自己不是领导者。
// 多试几次，以防需要重新选举。
func (cfg *config) checkOneLeader() int {
	// fmt.Println(cfg.connected)
	// fmt.Println(cfg.rafts[0].GetState())
	// fmt.Println(cfg.rafts[1].GetState())
	// fmt.Println(cfg.rafts[2].GetState())
	for iters := 0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)						// 生成 450-550 之间的随机数
		time.Sleep(time.Duration(ms) * time.Millisecond)		// 休眠 ms 毫秒

		leaders := make(map[int][]int)							// map[term][]leader
		for i := 0; i < cfg.n; i++ {							// 遍历每个节点
			if cfg.connected[i] {								// 如果该节点已连接
				if term, leader := cfg.rafts[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		
		lastTermWithLeader := -1								// 最后一个任期的领导者
		for term, leaders := range leaders {
			if len(leaders) > 1 {		// 如果某个任期有多个领导者，报错
				cfg.t.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term						// 更新最后一个任期的领导者
			}
		}

		if len(leaders) != 0 {									// 如果有领导者
			return leaders[lastTermWithLeader][0]				// 返回领导者
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

// 检查所有人是否都同意该任期。
// checkTerms checks the terms of the connected servers in the configuration.
// It returns the term if all servers agree on the same term, otherwise it returns -1.
func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			xterm, _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {			// 如果有服务器的任期不同，报错
				cfg.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

// 检查所连接的服务器中没有一个认为自己是领导者。报错
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			_, is_leader := cfg.rafts[i].GetState()
			if is_leader {
				cfg.t.Fatalf("expected no leader among connected servers, but %v claims to be leader", i)
			}
		}
	}
}

// 有多少台服务器认为日志条目已提交？
func (cfg *config) nCommitted(index int) (int, interface{}) {
	count := 0
	var cmd interface{} = nil
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// 等待至少 n 个服务器提交，但不要一直等下去。
func (cfg *config) wait(index int, n int, startTerm int) interface{} {
	to := 10 * time.Millisecond						// 10 毫秒
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nCommitted(index)				// 有多少台服务器认为日志条目已提交？
		if nd >= n {
			break
		}
		time.Sleep(to)								// 休眠 to ms
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {							// 如果有任期
			for _, r := range cfg.rafts {
				if t, _ := r.GetState(); t > startTerm { 		// 如果有服务器的任期大于 startTerm，报错
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := cfg.nCommitted(index)
	if nd < n {									// 如果有服务器没有提交，报错
		cfg.t.Fatalf("only %d decided for index %d; wanted %d",
			nd, index, n)
	}
	return cmd
}

// do a complete agreement.
// 它可能一开始就选错了领导者，在放弃后不得不重新提交。
// 大约 10 秒后就完全放弃了。
// 间接检查服务器是否同意相同的值，因为 nCommitted() 会检查这一点，
// 从 applyCh 读取的线程也会检查这一点。返回索引。
// 如果重试==true，可能会多次提交命令，以防某个领导者在 Start() 之后失败。
// 如果重试==false，只调用一次 Start()，以简化早期 Lab 2B 测试。
// 输入：cmd：要提交的命令；expectedServers：至少有多少台服务器提交；retry：是否重试
func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int {
	t0 := time.Now()																// 当前时间
	starts := 0																		// 从第 0 台服务器开始
	for time.Since(t0).Seconds() < 10 && cfg.checkFinished() == false {				// 10 秒内，且没有完成测试
		// 尝试所有服务器，也许有一个是领导者。
		index := -1
		for si := 0; si < cfg.n; si++ {
			starts = (starts + 1) % cfg.n
			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts] {												// 如果该服务器已连接
				rf = cfg.rafts[starts]												// 获取该服务器
			}
			cfg.mu.Unlock()
			if rf != nil {															// 如果该服务器不为空
				index1, _, ok := rf.Start(cmd)										// 向该服务器提交命令
				if ok {
					index = index1
					break
				}
			}
		}
		// 如果没有服务器是领导者，等待一会儿，然后重试。
		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := cfg.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd1 == cmd {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if cfg.checkFinished() == false {									// 如果还没有完成测试，报错
		cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	}
	return -1
}

// 结束测试
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
	cfg.cmds0 = 0
	cfg.maxIndex0 = cfg.maxIndex
}

// 结束测试 -- 我们到此为止，说明没有失败。
// 打印通过信息和一些性能数据。
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()       // real time
		npeers := cfg.n                         // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nbytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		ncmds := cfg.maxIndex - cfg.maxIndex0   // number of Raft agreements reported
		cfg.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
	}
}

// 所有服务器的最大日志大小
func (cfg *config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].RaftStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}
