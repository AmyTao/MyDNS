package raftkv

import (
	"os"
	"testing"

	"MIT6.824/labrpc"

	// import "log"
	crand "crypto/rand"
	// "encoding/base64"
	"fmt"
	"math/big"

	// "math/rand"
	// "runtime"
	"sync"
	"sync/atomic"
	"time"

	"MIT6.824/raft"
)

// func randstring(N int) string {
// 	b := make([]byte, 2*N)
// 	crand.Read(b)
// 	s := base64.URLEncoding.EncodeToString(b)
// 	return s[0:N]
// }

func MakeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// // Randomize server handles
// func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
// 	sa := make([]*labrpc.ClientEnd, len(kvh))
// 	copy(sa, kvh)
// 	for i := range sa {
// 		j := rand.Intn(i + 1)
// 		sa[i], sa[j] = sa[j], sa[i]
// 	}
// 	return sa
// }

type Config struct {
	mu           sync.Mutex
	T            *testing.T
	Net          *labrpc.Network
	N            int
	Kvservers    []*KVServer
	Saved        []*raft.Persister
	Endnames     [][]string // names of each server's sending ClientEnds
	Clerks       map[*Clerk][]string
	NextClientId int
	Maxraftstate int
	Start        time.Time // time at which make_config() was called
	// begin()/end() statistics
	t0    time.Time // time at which test_test.go called cfg.begin()
	rpcs0 int       // rpcTotal() at Start of test
	ops   int32     // number of clerk get/put/append method calls
}

func (cfg *Config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.T.Failed() && time.Since(cfg.Start) > 120_000*time.Second {
		cfg.T.Fatal("test took longer than 120_000 seconds")
	}
}

func (cfg *Config) Cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < len(cfg.Kvservers); i++ {
		if cfg.Kvservers[i] != nil {
			cfg.Kvservers[i].Kill()
		}
	}
	cfg.Net.Cleanup()
	cfg.checkTimeout()
}

// Maximum log size across all servers
func (cfg *Config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.N; i++ {
		N := cfg.Saved[i].RaftStateSize()
		if N > logsize {
			logsize = N
		}
	}
	return logsize
}

// Maximum snapshot size across all servers
func (cfg *Config) SnapshotSize() int {
	snapshotsize := 0
	for i := 0; i < cfg.N; i++ {
		N := cfg.Saved[i].SnapshotSize()
		if N > snapshotsize {
			snapshotsize = N
		}
	}
	return snapshotsize
}

// attach server i to servers listed in to
// caller must hold cfg.mu
func (cfg *Config) connectUnlocked(i int, to []int) {
	// log.Printf("connect peer %d to %v\N", i, to)

	// outgoing socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.Endnames[i][to[j]]
		cfg.Net.Enable(endname, true)
	}

	// incoming socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.Endnames[to[j]][i]
		cfg.Net.Enable(endname, true)
	}
}

func (cfg *Config) connect(i int, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connectUnlocked(i, to)
}

// detach server i from the servers listed in from
// caller must hold cfg.mu
func (cfg *Config) disconnectUnlocked(i int, from []int) {
	// log.Printf("disconnect peer %d from %v\N", i, from)

	// outgoing socket files
	for j := 0; j < len(from); j++ {
		if cfg.Endnames[i] != nil {
			endname := cfg.Endnames[i][from[j]]
			cfg.Net.Enable(endname, false)
		}
	}

	// incoming socket files
	for j := 0; j < len(from); j++ {
		if cfg.Endnames[j] != nil {
			endname := cfg.Endnames[from[j]][i]
			cfg.Net.Enable(endname, false)
		}
	}
}

func (cfg *Config) disconnect(i int, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.disconnectUnlocked(i, from)
}

func (cfg *Config) All() []int {
	all := make([]int, cfg.N)
	for i := 0; i < cfg.N; i++ {
		all[i] = i
	}
	return all
}

func (cfg *Config) ConnectAll() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.N; i++ {
		cfg.connectUnlocked(i, cfg.All())
	}
}

// Sets up 2 partitions with connectivity between servers in each  partition.
func (cfg *Config) partition(p1 []int, p2 []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	// log.Printf("partition servers into: %v %v\N", p1, p2)
	for i := 0; i < len(p1); i++ {
		cfg.disconnectUnlocked(p1[i], p2)
		cfg.connectUnlocked(p1[i], p1)
	}
	for i := 0; i < len(p2); i++ {
		cfg.disconnectUnlocked(p2[i], p1)
		cfg.connectUnlocked(p2[i], p2)
	}
}

// Create a clerk with clerk specific server names.
// Give it connections to all of the servers, but for
// now enable only connections to servers in to[].
func (cfg *Config) MakeClient(to []int) *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.N)
	Endnames := make([]string, cfg.N)
	for j := 0; j < cfg.N; j++ {
		Endnames[j] = randstring(20)
		ends[j] = cfg.Net.MakeEnd(Endnames[j])
		cfg.Net.Connect(Endnames[j], j)
	}

	ck := MakeClerk(random_handles(ends))
	cfg.Clerks[ck] = Endnames
	cfg.NextClientId++
	cfg.ConnectClientUnlocked(ck, to)
	return ck
}

func (cfg *Config) DeleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.Clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.Clerks, ck)
}

// caller should hold cfg.mu
func (cfg *Config) ConnectClientUnlocked(ck *Clerk, to []int) {
	// log.Printf("ConnectClient %v to %v\N", ck, to)
	Endnames := cfg.Clerks[ck]
	for j := 0; j < len(to); j++ {
		s := Endnames[to[j]]
		cfg.Net.Enable(s, true)
	}
}

func (cfg *Config) ConnectClient(ck *Clerk, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.ConnectClientUnlocked(ck, to)
}

// caller should hold cfg.mu
func (cfg *Config) DisconnectClientUnlocked(ck *Clerk, from []int) {
	// log.Printf("DisconnectClient %v from %v\N", ck, from)
	Endnames := cfg.Clerks[ck]
	for j := 0; j < len(from); j++ {
		s := Endnames[from[j]]
		cfg.Net.Enable(s, false)
	}
}

func (cfg *Config) DisconnectClient(ck *Clerk, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.DisconnectClientUnlocked(ck, from)
}

// Shutdown a server by isolating it
func (cfg *Config) ShutdownServer(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	cfg.disconnectUnlocked(i, cfg.All())

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in Saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.Net.DeleteServer(i)

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.Saved[i] != nil {
		cfg.Saved[i] = cfg.Saved[i].Copy()
	}

	kv := cfg.Kvservers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		cfg.Kvservers[i] = nil
	}
}

// If restart servers, first call ShutdownServer
func (cfg *Config) StartServer(i int) {
	cfg.mu.Lock()

	// a fresh set of outgoing ClientEnd names.
	cfg.Endnames[i] = make([]string, cfg.N)
	for j := 0; j < cfg.N; j++ {
		cfg.Endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.N)
	for j := 0; j < cfg.N; j++ {
		ends[j] = cfg.Net.MakeEnd(cfg.Endnames[i][j])
		cfg.Net.Connect(cfg.Endnames[i][j], j)
	}

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// give the fresh persister a copy of the old persister's
	// state, so that the spec is that we pass StartKVServer()
	// the last persisted state.
	if cfg.Saved[i] != nil {
		cfg.Saved[i] = cfg.Saved[i].Copy()
	} else {
		cfg.Saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()

	cfg.Kvservers[i] = StartKVServer(ends, i, cfg.Saved[i], cfg.Maxraftstate)

	kvsvc := labrpc.MakeService(cfg.Kvservers[i])
	rfsvc := labrpc.MakeService(cfg.Kvservers[i].rf)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.Net.AddServer(i, srv)
}

func (cfg *Config) Leader() (bool, int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for i := 0; i < cfg.N; i++ {
		_, is_leader := cfg.Kvservers[i].rf.GetState()
		if is_leader {
			return true, i
		}
	}
	return false, 0
}

// Partition servers into 2 groups and put current leader in minority
func (cfg *Config) make_partition() ([]int, []int) {
	_, l := cfg.Leader()
	p1 := make([]int, cfg.N/2+1)
	p2 := make([]int, cfg.N/2)
	j := 0
	for i := 0; i < cfg.N; i++ {
		if i != l {
			if j < len(p1) {
				p1[j] = i
			} else {
				p2[j-len(p1)] = i
			}
			j++
		}
	}
	p2[len(p2)-1] = l
	return p1, p2
}

var Ncpu_once sync.Once

// func make_config(t *testing.T, N int, unreliable bool, Maxraftstate int) *Config {
// 	ncpu_once.Do(func() {
// 		if runtime.NumCPU() < 2 {
// 			fmt.Printf("warning: only one CPU, which may conceal locking bugs\N")
// 		}
// 		rand.Seed(makeSeed())
// 	})
// 	runtime.GOMAXPROCS(4)
// 	cfg := &Config{}
// 	cfg.T = t
// 	cfg.Net = labrpc.MakeNetwork()
// 	cfg.N = N
// 	cfg.Kvservers = make([]*KVServer, cfg.N)
// 	cfg.Saved = make([]*raft.Persister, cfg.N)
// 	cfg.Endnames = make([][]string, cfg.N)
// 	cfg.Clerks = make(map[*Clerk][]string)
// 	cfg.NextClientId = cfg.N + 1000 // client ids Start 1000 above the highest serverid
// 	cfg.Maxraftstate = Maxraftstate
// 	cfg.Start = time.Now()

// 	// create a full set of KV servers.
// 	for i := 0; i < cfg.N; i++ {
// 		cfg.StartServer(i)
// 	}

// 	cfg.ConnectAll()

// 	cfg.Net.Reliable(!unreliable)

// 	return cfg
// }

func (cfg *Config) rpcTotal() int {
	return cfg.Net.GetTotalCount()
}

// Start a Test.
// print the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *Config) Begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	atomic.StoreInt32(&cfg.ops, 0)
}

func (cfg *Config) Op() {
	atomic.AddInt32(&cfg.ops, 1)
}

// end a Test -- the fact that we got here means there
// was no failure.
// print the Passed message,
// and some performance numbers.
func (cfg *Config) End() {
	cfg.checkTimeout()
	if !cfg.T.Failed() {
		t := time.Since(cfg.t0).Seconds()  // real time
		npeers := cfg.N                    // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0 // number of RPC sends
		// ops := atomic.LoadInt32(&cfg.ops)  //  number of clerk get/put/append calls

		fmt.Printf("  ... Passed --")
		// fmt.Printf("  %4.1fs, %d servers, %5d rpcs, %4d clerk ops\n", t, npeers, nrpc, ops)
		fmt.Printf("  %4.1fs, %d servers, %5d rpcs\n", t, npeers, nrpc)
	}
}
