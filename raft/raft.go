package raft

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

type Entry struct {
	Index   int
	Term    int
	Command interface{}
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
	votedFor                int
	log                     []Entry
	commitIndex             int
	lastApplied             int
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
	Term         int
	LeaderId     int
	Entries      []Entry
	PrevLogIndex int //index of log entry immediately precedingnew ones
	PrevLogTerm  int //term of PrevLogIndex entry
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	PeerId       int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	State       int
}

func min(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.isLeader {
		if args.Term >= rf.Term {
			rf.becomeFollwerFromLeader <- true
			rf.State = Follwer
			rf.Term = args.Term
		}
		reply.Success = true
	} else {
		if args.Term >= rf.Term {
			if len(args.Entries) == 0 {
				rf.Term = args.Term
				rf.receiveHB <- true
				rf.State = Follwer
				reply.Success = true
			} else { //append
				if args.LeaderCommit > rf.commitIndex {
					println("Commit")
					rf.commitIndex = args.LeaderCommit
				}
				contain := rf.isContain(args.PrevLogIndex, args.PrevLogTerm)
				if !contain {
					reply.Success = false
				} else {
					rf.log = append(rf.log, args.Entries[args.PrevLogIndex:]...)
					rf.commitIndex = min(args.LeaderCommit, len(rf.log))
					reply.Success = true
				}
			}
		}
	}
	reply.Term = rf.Term
	rf.mu.Unlock()
}

func (rf *Raft) isContain(index int, term int) bool {
	for i := len(rf.log); i >= 0; i-- {
		if index > i {
			return false
		}
		if index == i && rf.getTermFromIndex(i) == term {
			rf.log = rf.log[:index]
			return true
		}
	}
	return false
}

func (rf *Raft) getTermFromIndex(index int) int {
	if index == 0 {
		return -1
	} else {
		return rf.log[index-1].Term
	}
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
			rf.State = Follwer
			rf.Term = args.Term
			rfLastIndex := len(rf.log)
			rfLastTerm := rf.getTermFromIndex(rfLastIndex)
			if (rfLastTerm <= args.LastLogTerm) && ((rfLastTerm != args.LastLogTerm) || (rfLastIndex <= args.LastLogIndex)) {
				rf.votedFor = args.PeerId
				reply.VoteGranted = true
			} else {
				rf.votedFor = -1
				reply.VoteGranted = false
			}
			reply.Term = rf.Term
			reply.State = rf.State
		} else {
			reply.Term = rf.Term
			reply.State = rf.State
		}
	} else {
		if args.Term > rf.Term {
			rf.receiveHB <- true
			rf.State = Follwer
			rf.Term = args.Term
			rf.votedFor = -1
		}
		reply.Term = rf.Term
		reply.State = rf.State
		if rf.votedFor == -1 {
			reply.VoteGranted = true
			rf.votedFor = args.PeerId
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

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func generateTime() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := 600 - 300
	return 300 + r.Intn(diff)
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.State = Follwer
	rf.log = []Entry{}
	rf.votedFor = -1
	go rf.startElection()
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				am := ApplyMsg{}
				am.Command = rf.log[rf.lastApplied-1].Command
				am.CommandIndex = rf.lastApplied
				am.CommandValid = true
				applyCh <- am
			}
			rf.mu.Unlock()
		}
	}()
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	cond := sync.NewCond(&rf.mu)
	sucReplicate := 1
	finished := 1
	rf.mu.Lock()
	index := len(rf.log) + 1
	Term := rf.Term
	isLeader := rf.isLeader
	rf.mu.Unlock()
	if isLeader == false {
		return 0, 0, false
	} else { //start append cmd to log and append to other servers
		go func() {
			newCmd := Entry{}
			newCmd.Command = command
			newCmd.Index = index
			newCmd.Term = Term
			rf.mu.Lock()
			rf.log = append(rf.log, newCmd)
			rf.mu.Unlock()
			for s := 0; s < len(rf.peers); s++ {
				if s == rf.me {
					continue
				}
				args := AppendEntriesArgs{}
				rf.mu.Lock()
				if !rf.isLeader {
					rf.mu.Unlock()
					return
				}
				args.LeaderId = rf.me
				args.Term = rf.Term
				args.Entries = rf.log
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = index - 1
				args.PrevLogTerm = rf.getTermFromIndex(args.PrevLogIndex)
				rf.mu.Unlock()
				server := s
				reply := AppendEntriesReply{}
				go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
					success := false
					for {
						ok := rf.sendAppendEntries(server, &args, &reply)
						if !ok {
							rf.mu.Lock()
							finished++
							rf.mu.Unlock()
							cond.Signal()
							return
						}
						success = reply.Success
						if success {
							rf.mu.Lock()
							finished++
							sucReplicate++
							rf.mu.Unlock()
							cond.Signal()
							break
						} else {
							args.PrevLogIndex = args.PrevLogIndex - 1
							rf.mu.Lock()
							args.PrevLogTerm = rf.getTermFromIndex(args.PrevLogIndex)
							rf.mu.Unlock()

						}
					}
				}(server, args, reply)
			}
		}()
	}
	rf.mu.Lock()
	for sucReplicate <= len(rf.peers)/2 && finished != len(rf.peers) {
		cond.Wait()
	}
	rf.mu.Unlock()
	if sucReplicate > len(rf.peers)/2 {
		//send committe
		rf.mu.Lock()
		rf.commitIndex = len(rf.log)
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.Term = rf.Term
		args.Entries = rf.log
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = index
		args.PrevLogTerm = rf.getTermFromIndex(args.PrevLogIndex)
		rf.mu.Unlock()
		for s := 0; s < len(rf.peers); s++ {
			if s == rf.me {
				continue
			}
			reply := AppendEntriesReply{}
			server := s
			go func() {
				_ = rf.sendAppendEntries(server, &args, &reply)
			}()
		}
	}
	return index, Term, isLeader
}

func (rf *Raft) sendHeartBeat() {
	//a := time.Now()
	args := AppendEntriesArgs{}
	numLost := len(rf.peers) - 1
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}
	args.LeaderId = rf.me
	args.Term = rf.Term
	rf.mu.Unlock()
	args.Entries = []Entry{}
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
			numLost--
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
			return
		}
	}
	if numLost == len(rf.peers)-1 {
		rf.becomeFollwerFromLeader <- true
	}
	//println(time.Since(a)*time.Millisecond, "  ", time.Duration(generateTime())*time.Millisecond)
}

func (rf *Raft) startAsCand() bool {
	votes := 1
	cond := sync.NewCond(&rf.mu)
	getReply := 1
	args := RequestVoteArgs{}
	rf.mu.Lock()
	rf.State = Cand
	rf.Term = rf.Term + 1
	rf.votedFor = rf.me
	args.PeerId = rf.me
	args.Term = rf.Term
	args.LastLogIndex = len(rf.log)
	args.LastLogTerm = rf.getTermFromIndex(args.LastLogIndex)
	rf.mu.Unlock()
	for s := 0; s < len(rf.peers); s++ {
		if s == rf.me {
			continue
		}
		server := s
		reply := RequestVoteReply{}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				getReply++
				cond.Signal()
				return
			}
			rf.mu.Lock()
			getReply++
			if reply.Term > rf.Term || (reply.Term == rf.Term && reply.State == Leader) {
				rf.Term = reply.Term
				rf.becomeFollwerFromCand <- true
				rf.State = Follwer
				cond.Signal()
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted == true {
				votes++
			}
			cond.Signal()
			rf.mu.Unlock()
		}(server, args, reply)
	}
	rf.mu.Lock()
	for getReply != len(rf.peers) && votes <= len(rf.peers)/2 {
		cond.Wait()
	}
	rf.mu.Unlock()
	if getReply == 1 && len(rf.peers) > 2 {
		rf.mu.Lock()
		rf.Term--
		rf.becomeFollwerFromCand <- true
		rf.mu.Unlock()
		return false
	}
	if votes > len(rf.peers)/2 {
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
