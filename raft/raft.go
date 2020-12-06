package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../mygob"
	"../myrpc"
)

const (
	Leader = iota
	Cand
	Follwer
)
const (
	DidNotWin = iota
	Win
	Disconnect
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Index   int
	Command interface{}
	Term    int
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
	votedFor                int
	nextIndex               []int
	matchIndex              []int
	log                     []Entry
	//peerAppendBuffer        []chan bool
	peerAlive           []bool
	committeGetUpdate   *sync.Cond
	commitGetUpdateDone *sync.Cond
	lastApplied         int
	commitIndex         int
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
	w := new(bytes.Buffer)
	e := mygob.NewEncoder(w)
	e.Encode(rf.Term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := mygob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Logs []Entry
	d.Decode(&CurrentTerm)
	d.Decode(&VotedFor)
	d.Decode(&Logs)
	rf.Term = CurrentTerm
	rf.votedFor = VotedFor
	rf.log = Logs
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
	LastIndex int
	Term      int
	Success   bool
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
	if a < b {
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
			rf.persist()
		}
		reply.Success = true
	} else {
		if args.Term >= rf.Term {
			if len(args.Entries) == 0 {
				rf.Term = args.Term
				rf.receiveHB <- true
				rf.State = Follwer
				reply.Success = true
				rf.persist()
			}
		}
	}
	if args.PrevLogIndex == -1 {
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogEntryWithoutLock().Index)
			rf.committeGetUpdate.Signal()
			rf.commitGetUpdateDone.Wait()
			reply.Success = true
		}
	} else { //append
		entr, find := rf.GetLogAtIndexWithoutLock(args.PrevLogIndex)
		if !find { //give the last index
			reply.LastIndex = rf.GetLastLogEntryWithoutLock().Index
			reply.Success = false
		} else {
			if entr.Term != args.PrevLogTerm {
				rf.log = rf.log[:IndexInLog(args.PrevLogIndex)]
				reply.LastIndex = -1
				reply.Success = false
			} else {
				rf.log = rf.log[:IndexInLog(args.PrevLogIndex+1)]
				rf.log = append(rf.log, args.Entries...)
				reply.LastIndex = -1
				reply.Success = true
			}
		}
		rf.persist()
	}
	reply.Term = rf.Term
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rfLastIndex := rf.GetLastLogEntryWithoutLock().Index
	rfLastTerm := rf.TermForLog(rfLastIndex)
	if rf.isLeader {
		if args.Term > rf.Term {
			rf.becomeFollwerFromLeader <- true
			rf.State = Follwer
			rf.Term = args.Term
			if (rfLastTerm < args.LastLogTerm) || ((rfLastTerm == args.LastLogTerm) && (rfLastIndex <= args.LastLogIndex)) {
				rf.votedFor = args.PeerId
				reply.VoteGranted = true
			} else {
				rf.votedFor = -1
				reply.VoteGranted = false
			}
			reply.Term = rf.Term
			reply.State = rf.State
			rf.persist()
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
			if (rfLastTerm < args.LastLogTerm) || ((rfLastTerm == args.LastLogTerm) && (rfLastIndex <= args.LastLogIndex)) { //fmt.Println(rf.me, " ", rfLastTerm, " ", args.LastLogTerm)
				rf.votedFor = args.PeerId
				reply.VoteGranted = true
			} else {
				rf.votedFor = -1
				reply.VoteGranted = false
			}
		} else {
			reply.VoteGranted = false
		}
		rf.persist()
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
	rf.mu.Lock()
	isLeader := rf.isLeader
	rf.mu.Unlock()
	if isLeader {
		// rf.mu.Lock()
		//<-rf.peerAppendBuffer[rf.me]
		rf.mu.Lock()
		index = rf.GetLastLogEntryWithoutLock().Index + 1
		Term = rf.Term
		isLeader = true
		newE := Entry{}
		newE.Command = command
		newE.Index = index
		newE.Term = rf.Term
		rf.log = append(rf.log, newE)
		rf.persist()
		rf.mu.Unlock()
		c := make(chan bool)
		cond := sync.NewCond(&rf.mu)
		committe := false
		for i := 0; i < len(rf.peers); i++ {
			server := i
			if server == rf.me {
				continue
			}
			go rf.StarOnePeer(server, c, cond, &committe)
		}
		for i := 0; i < len(rf.peers)-1; i++ {
			<-c
		}
		rf.mu.Lock()
		if rf.updateCommitForLeader() {
			rf.committeGetUpdate.Signal()
			rf.commitGetUpdateDone.Wait()
			committe = true
			cond.Broadcast()
		} else {
			cond.Broadcast()
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers)-1; i++ {
			<-c
		}
		// go func() {
		// 	rf.peerAppendBuffer[rf.me] <- true
		// }()
	}
	return index, Term, isLeader
}

func (rf *Raft) StarOnePeer(server int, c chan bool, cond *sync.Cond, committe *bool) {
	//<-rf.peerAppendBuffer[server]
	rf.StarOnePeerAppend(server)
	go func() {
		c <- true
	}()
	rf.mu.Lock()
	cond.Wait()
	if *committe {
		rf.mu.Unlock()
		rf.StartOnePeerCommitte(server)
	} else {
		rf.mu.Unlock()
	}
	go func() {
		c <- true
	}()
	// go func() {
	// 	rf.peerAppendBuffer[server] <- true
	// }()
}

func (rf *Raft) StarOnePeerAppend(server int) {
	if rf.isLeader {
		entries := []Entry{}
		rf.mu.Lock()
		for i := rf.matchIndex[server] + 1; i <= rf.GetLastLogEntryWithoutLock().Index; i++ {
			entry, find := rf.GetLogAtIndexWithoutLock(i)
			if !find {
				entries = []Entry{}
				break
			}
			entries = append(entries, entry)
		}
		args := AppendEntriesArgs{}
		args.LeaderId = rf.me
		args.Term = rf.Term
		args.PrevLogIndex = rf.matchIndex[server]
		args.PrevLogTerm = rf.TermForLog(args.PrevLogIndex)
		args.Entries = entries
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()
		success := false
		for !rf.killed() {
			reply := AppendEntriesReply{}
			var ok bool
			rf.mu.Lock()
			if rf.peerAlive[server] {
				rf.mu.Unlock()
				ok = rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					rf.mu.Lock()
					rf.peerAlive[server] = false
					rf.mu.Unlock()
					break
				}
			} else {
				rf.mu.Unlock()
				break
			}

			success = reply.Success
			if success {
				rf.mu.Lock()
				rf.matchIndex[server] = len(args.Entries) + args.PrevLogIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.mu.Unlock()
				break
			} else {
				if reply.LastIndex != -1 {
					args.PrevLogIndex = reply.LastIndex
				} else {
					args.PrevLogIndex = args.PrevLogIndex - 1
				}
				rf.mu.Lock()
				args.PrevLogTerm = rf.TermForLog(args.PrevLogIndex)
				args.Entries = rf.log[IndexInLog(args.PrevLogIndex+1):]
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) StartOnePeerCommitte(server int) {
	if rf.isLeader {
		args := AppendEntriesArgs{}
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
		rf.mu.Lock()
		args.LeaderCommit = rf.commitIndex
		args.Term = rf.Term
		args.LeaderId = rf.me
		rf.mu.Unlock()
		args.Entries = []Entry{}
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		if rf.peerAlive[server] {
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				rf.mu.Lock()
				rf.peerAlive[server] = false
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) updateCommitForLeader() bool {
	beginIndex := rf.commitIndex + 1
	lastCommittedIndex := -1
	updated := false
	for ; beginIndex <= rf.GetLastLogEntryWithoutLock().Index; beginIndex++ {
		granted := 1

		for peerIndex := 0; peerIndex < len(rf.matchIndex); peerIndex++ {
			if peerIndex == rf.me {
				continue
			}
			if rf.matchIndex[peerIndex] >= beginIndex {
				granted++
			}
		}

		if granted >= len(rf.peers)/2+1 && (rf.TermForLog(beginIndex) == rf.Term) {
			lastCommittedIndex = beginIndex
		}
	}
	if lastCommittedIndex > rf.commitIndex {
		rf.commitIndex = lastCommittedIndex
		updated = true
	}
	return updated

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
	rf.votedFor = -1
	rf.log = []Entry{}
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.peerAlive = make([]bool, len(peers))
	//rf.peerAppendBuffer = make([]chan bool, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.committeGetUpdate = sync.NewCond(&rf.mu)
	rf.commitGetUpdateDone = sync.NewCond(&rf.mu)
	go rf.startElection()
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			rf.committeGetUpdate.Wait()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied = rf.lastApplied + 1
				am := ApplyMsg{}
				am.Command = rf.log[IndexInLog(rf.lastApplied)].Command
				am.CommandIndex = rf.lastApplied
				am.CommandValid = true
				applyCh <- am
			}
			rf.commitGetUpdateDone.Signal()
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
	if rf.State == Leader {
		rf.becomeFollwerFromLeader <- true
	}
	rf.mu.Unlock()
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
	args.PrevLogIndex = -1
	outDate := make(chan bool, 1)
	for s := 0; s < len(rf.peers); s++ {
		if s == rf.me {
			continue
		}
		server := s
		reply := AppendEntriesReply{}
		go func() {
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				rf.mu.Lock()
				rf.peerAlive[server] = false
				outDate <- false
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			if !rf.peerAlive[server] {
				rf.peerAlive[server] = true
				rf.mu.Unlock()
				go func() {
					//<-rf.peerAppendBuffer[server]
					rf.StarOnePeerAppend(server)
					rf.StartOnePeerCommitte(server)
					// go func() {
					// 	rf.peerAppendBuffer[server] <- true
					// }()
				}()
			} else {
				rf.mu.Unlock()
			}
			rf.mu.Lock()
			numLost--
			if reply.Term > rf.Term {
				rf.Term = reply.Term
				rf.persist()
				rf.mu.Unlock()
				outDate <- true
				return
			}
			rf.mu.Unlock()
			outDate <- false
		}()
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		a := <-outDate
		if a {
			rf.mu.Lock()
			if rf.State == Leader {
				rf.becomeFollwerFromLeader <- true
			}
			rf.mu.Unlock()
			return
		}
	}
	if numLost == len(rf.peers)-1 {
		rf.mu.Lock()
		if rf.State == Leader {
			rf.becomeFollwerFromLeader <- true
		}
		rf.mu.Unlock()
	}
	//println(time.Since(a), "  ", time.Duration(generateTime())*time.Millisecond)
}

func (rf *Raft) startAsCand(interval int) int {
	votes := 1
	cond := sync.NewCond(&rf.mu)
	var needReturn bool

	needReturn = false
	go func(needReturn *bool, cond *sync.Cond) {
		time.Sleep(time.Duration(interval-20) * time.Millisecond)
		rf.mu.Lock()
		*needReturn = true
		cond.Signal()
		rf.mu.Unlock()
	}(&needReturn, cond)

	getReply := 1
	numOfDisc := 1
	args := RequestVoteArgs{}
	rf.mu.Lock()
	rf.State = Cand
	rf.Term = rf.Term + 1
	rf.votedFor = rf.me
	args.PeerId = rf.me
	args.Term = rf.Term
	args.LastLogIndex = rf.GetLastLogEntryWithoutLock().Index
	args.LastLogTerm = rf.TermForLog(args.LastLogIndex)
	rf.persist()
	rf.mu.Unlock()

	//send rpcs
	for s := 0; s < len(rf.peers); s++ {
		if s == rf.me {
			continue
		}
		server := s
		reply := RequestVoteReply{}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				rf.mu.Lock()
				getReply++
				numOfDisc++
				rf.mu.Unlock()
				cond.Signal()
				return
			}
			rf.mu.Lock()
			getReply++
			if reply.Term > rf.Term || (reply.Term == rf.Term && reply.State == Leader) {
				rf.Term = reply.Term
				rf.receiveHB <- true
				rf.State = Follwer
				cond.Signal()
				rf.persist()
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
	//wait
	rf.mu.Lock()
	for getReply != len(rf.peers) && votes <= len(rf.peers)/2 {
		cond.Wait()
	}
	rf.mu.Unlock()
	//decide
	if needReturn == true {
		return DidNotWin
	}
	rf.mu.Lock()
	if votes > len(rf.peers)/2 && rf.State == Cand {
		rf.mu.Unlock()
		return Win
	} else {
		rf.mu.Unlock()
		return DidNotWin
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
		leader := make(chan int, 1)
	Loop:
		for !rf.killed() {
			select {
			case <-ticker.C:
				interval := generateTime()
				ticker = time.NewTicker(time.Duration(interval) * time.Millisecond)
				go func() {
					leader <- rf.startAsCand(interval)
				}()
			case <-rf.receiveHB:
				ticker = time.NewTicker(time.Duration(generateTime()) * time.Millisecond)
			case a := <-leader:
				if a == Win {
					break Loop
				}
			default:
			}
		}
		ticker.Stop()
		rf.mu.Lock()
		for i := 0; i < len(rf.peers); i++ {
			s := i
			rf.nextIndex[s] = rf.GetLastLogEntryWithoutLock().Index + 1
			rf.nextIndex[s] = 0
			rf.peerAlive[s] = true
			// rf.peerAppendBuffer[s] = make(chan bool)
			// go func() {
			// 	rf.peerAppendBuffer[s] <- true
			// }()
		}
		rf.mu.Unlock()
		rf.setLeader(true)
		go rf.startAsLeader()
		<-rf.becomeFollwerFromLeader
		rf.setLeader(false)
	}
}

func (rf *Raft) GetLogAtIndexWithoutLock(index int) (Entry, bool) {
	if index == 0 {
		return Entry{}, true
	} else if len(rf.log) == 0 {
		return Entry{}, false
	} else if (index < -1) || (index > rf.GetLastLogEntryWithoutLock().Index) {
		return Entry{}, false
	} else {
		localIndex := index - rf.log[0].Index
		return rf.log[localIndex], true
	}
}

func (rf *Raft) GetLogAtIndex(index int) (Entry, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.GetLogAtIndexWithoutLock(index)
}

func (rf *Raft) GetLastLogEntryWithoutLock() Entry {
	entry := Entry{}
	if len(rf.log) == 0 {
		entry.Term = 0
		entry.Index = 0
	} else {
		entry = rf.log[len(rf.log)-1]
	}
	return entry
}

func (rf *Raft) GetLastLogEntry() Entry {
	entry := Entry{}
	rf.mu.Lock()
	entry = rf.GetLastLogEntryWithoutLock()
	rf.mu.Unlock()
	return entry
}

func (rf *Raft) TermForLog(index int) int {
	entry, ok := rf.GetLogAtIndexWithoutLock(index)
	if ok {
		return entry.Term
	} else {
		return -1
	}
}

func IndexInLog(index int) int {
	if index > 0 {
		return index - 1
	} else {
		println("ERROR")
		return -1
	}
}

func printLog(log []Entry) {
	println()
	for _, v := range log {
		fmt.Print(v.Command, " ")
	}
	println()
}
