package alg

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../myrpc"
)

const (
	Leader = iota
	Cand
	Follwer
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu                      sync.Mutex         // Lock to protect shared access to this peer's State
	peers                   []*myrpc.ClientEnd // RPC end points of all peers
	persister               *Persister         // Object to hold this peer's persisted State
	me                      int                // this peer's index into peers[]
	dead                    int32              // set by Kill()
	isLeader                bool
	State                   int
	Term                    int
	becomeFollwerFromLeader chan bool
	becomeFollwerFromCand   chan bool
	receiveHB               chan bool
	voteChance              bool
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	Term := rf.Term
	isleader := rf.isLeader
	rf.mu.Unlock()
	return Term, isleader
}

func (rf *Raft) GetState2() (int, string) {
	rf.mu.Lock()
	Term := rf.Term
	var State string
	if rf.State == Follwer {
		State = "Follower"
	} else if rf.State == Cand {
		State = "Candidate"
	} else {
		State = "Leader"
	}
	rf.mu.Unlock()
	return Term, State
}

func (rf *Raft) persist() {

}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
		return
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term   int
	PeerId int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	State       int
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.isLeader {
		if args.Term >= rf.Term {
			rf.becomeFollwerFromLeader <- true
			rf.State = Follwer
			rf.Term = args.Term
			rf.voteChance = false
		}
	} else {
		if args.Term >= rf.Term {
			if len(args.Entries) == 0 {
				rf.Term = args.Term
				rf.receiveHB <- true
				rf.State = Follwer
				rf.voteChance = false
			}
		}
	}
	reply.Success = true
	reply.Term = rf.Term
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if rf.isLeader {
		if args.Term > rf.Term {
			rf.becomeFollwerFromLeader <- true
			//println("xiaTAI ", rf.me)
			rf.State = Follwer
			rf.Term = args.Term
			reply.VoteGranted = true
			reply.Term = rf.Term
			reply.State = rf.State
			rf.voteChance = false
		} else {
			reply.VoteGranted = false
			reply.Term = rf.Term
			reply.State = rf.State
		}
	} else {
		if args.Term > rf.Term {
			rf.receiveHB <- true
			rf.State = Follwer
			rf.Term = args.Term
			rf.voteChance = true
		}
		reply.Term = rf.Term
		reply.State = rf.State
		if rf.voteChance == true {
			reply.VoteGranted = true
			rf.voteChance = false
		} else {
			reply.VoteGranted = false
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	Term := -1
	isLeader := true
	return index, Term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func generateTime() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := 700 - 350
	return 350 + r.Intn(diff)
}

func Make(peers []*myrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.receiveHB = make(chan bool, 1)
	rf.becomeFollwerFromCand = make(chan bool, 1)
	rf.becomeFollwerFromLeader = make(chan bool, 1)
	rf.isLeader = false
	rf.Term = 1
	rf.State = Follwer
	rf.voteChance = false
	go rf.startElection()
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) startAsLeader() {
	for !rf.killed() {
		go rf.sendHeartBeat()
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
	rf.mu.Lock()
	rf.becomeFollwerFromLeader <- true
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeat() {
	args := AppendEntriesArgs{}
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}
	args.LeaderId = rf.me
	args.Term = rf.Term
	rf.mu.Unlock()
	args.Entries = []int{}
	outDate := make(chan bool, 1)
	for s := 0; s < len(rf.peers); s++ {
		if s == rf.me {
			continue
		}
		server := s
		reply := AppendEntriesReply{}
		go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				outDate <- false
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.Term {
				rf.Term = reply.Term
				rf.mu.Unlock()
				outDate <- true
				return
			}
			rf.mu.Unlock()
			outDate <- false
		}(server, args, reply)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		a := <-outDate
		if a {
			rf.becomeFollwerFromLeader <- true
		}
	}
}

func (rf *Raft) startAsCand() bool {
	votes := 1
	var done sync.WaitGroup
	getReply := 1
	args := RequestVoteArgs{}
	rf.mu.Lock()
	rf.State = Cand
	rf.Term = rf.Term + 1
	args.PeerId = rf.me
	args.Term = rf.Term
	rf.mu.Unlock()
	for s := 0; s < len(rf.peers); s++ {
		if s == rf.me {
			continue
		}
		server := s
		done.Add(1)
		reply := RequestVoteReply{}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			defer done.Done()
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			getReply++
			if reply.Term > rf.Term || (reply.Term == rf.Term && reply.State == Leader) {
				rf.Term = reply.Term
				rf.becomeFollwerFromCand <- true
				rf.State = Follwer
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted == true {
				votes++
			}
			rf.mu.Unlock()
		}(server, args, reply)
	}
	done.Wait()
	if getReply == 1 {
		rf.mu.Lock()
		rf.Term--
		rf.becomeFollwerFromCand <- true
		rf.mu.Unlock()
		return false
	}
	if votes > getReply/2 && votes > 1 {
		return true
	} else {
		return false
	}
}

func (rf *Raft) setLeader(bo bool) {
	rf.mu.Lock()
	rf.isLeader = bo
	if bo {
		rf.State = Leader
	} else {
		rf.State = Follwer
	}
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	for !rf.killed() {
		ticker := time.NewTicker(time.Duration(generateTime()) * time.Millisecond)
		elec := false
		leader := make(chan bool, 1)
	Loop:
		for !rf.killed() {
			select {
			case <-ticker.C:
				ticker = time.NewTicker(time.Duration(generateTime()) * time.Millisecond)
				elec = true
				go func() {
					leader <- rf.startAsCand()
				}()
			case <-rf.receiveHB:
				ticker = time.NewTicker(time.Duration(generateTime()) * time.Millisecond)
				elec = false
			case a := <-leader:
				if a && elec {
					break Loop
				}
			case <-rf.becomeFollwerFromCand:
				ticker = time.NewTicker(time.Duration(generateTime()) * time.Millisecond)
				elec = false
			default:
			}
		}
		ticker.Stop()

		rf.setLeader(true)
		go rf.startAsLeader()
		a := <-rf.becomeFollwerFromLeader
		if a {
			rf.setLeader(false)
		}
	}

}
