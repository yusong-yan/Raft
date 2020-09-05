package raft

import (
	"fmt"
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
	StartDone               chan bool
	commitGetUpdateDone     *sync.Cond
	committeGetUpdate       *sync.Cond
	StartDoneCond           *sync.Cond
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
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	<-rf.StartDone
	//a := time.Now()
	cond := sync.NewCond(&rf.mu)
	rf.mu.Lock()
	index := rf.GetLastLogEntryWithoutLock().Index + 1
	Term := rf.Term
	isLeader := rf.isLeader
	rf.mu.Unlock()
	completePeer := make(chan int)
	if isLeader == false {
		rf.StartDoneCond.Signal()
		return -1, -1, false
	} else { //start append cmd to log and append to other servers
		//go func() {
		finished := 1
		sucReplicate := 1
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
				rf.StartDoneCond.Signal()
				return -1, -1, false
			}
			args.LeaderId = rf.me
			args.Term = rf.Term
			args.PrevLogIndex = index - 1
			args.PrevLogTerm = rf.TermForLog(args.PrevLogIndex)
			args.Entries = rf.log[IndexInLog(args.PrevLogIndex+1):]
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			server := s
			go func() {
				success := false
				for !rf.killed() {
					//fmt.Println("Send to server", server, "with command", command)
					reply := AppendEntriesReply{}
					//role := "Follwer"
					// //if rf.State == Leader {
					// 	role = "Leader"
					// }
					// fmt.Println(args.PrevLogIndex, " ", rf.log, " ", role)
					okchan := make(chan bool, 1)
					go func() {
						returnBool := rf.sendAppendEntries(server, &args, &reply)
						okchan <- returnBool
					}()
					//a := time.Now()
					var ok bool
					ticker := time.NewTicker(10000000)
					select {
					case <-ticker.C:
						ok = false
					case <-okchan:
						ok = true
					}
					//println(time.Since(a), " ", 9000000)

					if !ok {
						rf.mu.Lock()
						finished++
						rf.mu.Unlock()
						go func() {
							completePeer <- -1
						}()
						cond.Signal()
						break
					}
					success = reply.Success
					if success {
						rf.mu.Lock()
						finished++
						sucReplicate++
						rf.mu.Unlock()
						go func() {
							completePeer <- server
						}()
						cond.Signal()
						break
					} else {
						if reply.LastIndex != -1 {
							//fmt.Println(reply.LastIndex, " ", args.PrevLogIndex, " ", args.Entries[0].Command)
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
			}()
		}
		rf.mu.Lock()
		for sucReplicate <= len(rf.peers)/2 && finished != len(rf.peers) {
			cond.Wait()
		}
		rf.mu.Unlock()
		if sucReplicate > len(rf.peers)/2 {
			rf.commitIndex = rf.GetLastLogEntryWithoutLock().Index
			rf.committeGetUpdate.Signal()
			//send committe
			rf.mu.Lock()
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.Term
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = -1
			finishedC := 1
			rf.mu.Unlock()
			condForCommi := sync.NewCond(&rf.mu)
			for i := 0; i < len(rf.peers)-1; i++ {
				s := <-completePeer
				if s != -1 {
					reply := AppendEntriesReply{}
					server := s
					go func() {
						for !rf.killed() {
							ok := rf.sendAppendEntries(server, &args, &reply)
							if ok {
								if reply.Success == true {
									rf.mu.Lock()
									finishedC++
									rf.mu.Unlock()
									condForCommi.Signal()
									return
								}
							}
						}
					}()
				} else {
					rf.mu.Lock()
					finishedC++
					rf.mu.Unlock()
					condForCommi.Signal()
				}
			}
			rf.mu.Lock()
			for finishedC != len(rf.peers) {
				condForCommi.Wait()
			}
			rf.mu.Unlock()
			rf.StartDoneCond.Signal()
			//fmt.Println(time.Since(a), " with command ", command)
		} else {
			for i := 0; i < len(rf.peers)-1; i++ {
				<-completePeer
			}
			rf.StartDoneCond.Signal()
			//fmt.Println(time.Since(a), "NOT ENOUGH REPLICA with command ", command)
		}
		//}()
	}
	return index, Term, isLeader
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
			}
		}
	}
	if args.PrevLogIndex == -1 {
		//println("readycommit")
		if args.LeaderCommit > rf.commitIndex {
			//println("Commit")
			rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogEntryWithoutLock().Index)
			//println("rf ", rf.me, "commite ", rf.commitIndex)
			rf.committeGetUpdate.Signal()
			rf.commitGetUpdateDone.Wait()
			reply.Success = true
		}
	} else { //append
		entr, find := rf.GetLogAtIndexWithoutLock(args.PrevLogIndex)
		//fmt.Println("THE LAST IS, ", rf.GetLastLogEntryWithoutLock().Index, " ", rf.GetLastLogEntryWithoutLock().Command, " ", rf.GetLastLogEntryWithoutLock().Term)
		if !find { //give the last index
			reply.LastIndex = rf.GetLastLogEntryWithoutLock().Index
			reply.Success = false
			//println("couldn;t find,", args.PrevLogIndex, " the last index of reply is , ", reply.LastIndex)
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
				//println("rf ", rf.me, "with lengthlog ", len(rf.log))
				//printLog(rf.log)
			}
		}
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
			if (rfLastTerm < args.LastLogTerm) || ((rfLastTerm == args.LastLogTerm) && (rfLastIndex <= args.LastLogIndex)) {
				rf.votedFor = args.PeerId
				reply.VoteGranted = true
			} else {
				rf.votedFor = -1
				reply.VoteGranted = false
			}
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
	rf.committeGetUpdate = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.receiveHB = make(chan bool, 1)
	rf.becomeFollwerFromCand = make(chan bool, 1)
	rf.becomeFollwerFromLeader = make(chan bool, 1)
	rf.isLeader = false
	rf.Term = 1
	rf.StartDoneCond = sync.NewCond(&rf.mu)
	rf.commitGetUpdateDone = sync.NewCond(&rf.mu)
	rf.StartDone = make(chan bool, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.State = Follwer
	rf.log = []Entry{}
	rf.votedFor = -1
	go func() {
		rf.mu.Lock()
		for {
			rf.StartDone <- true
			rf.StartDoneCond.Wait()
		}
	}()
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
				// if rf.State != Leader {
				// 	fmt.Println("rf", rf.me, " apply", am.Command, " last appy ", rf.lastApplied)
				// }
				applyCh <- am
			}
			rf.mu.Unlock()
			rf.commitGetUpdateDone.Signal()
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
	args.PrevLogIndex = -1
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

func (rf *Raft) startAsCand(interval int) bool {
	votes := 1
	cond := sync.NewCond(&rf.mu)
	var needReturn bool

	needReturn = false
	go func(needReturn *bool, cond *sync.Cond) {
		time.Sleep(time.Duration(interval-100) * time.Millisecond)
		rf.mu.Lock()
		*needReturn = true
		cond.Signal()
		rf.mu.Unlock()
	}(&needReturn, cond)

	getReply := 1
	args := RequestVoteArgs{}
	rf.mu.Lock()
	rf.State = Cand
	rf.Term = rf.Term + 1
	rf.votedFor = rf.me
	args.PeerId = rf.me
	args.Term = rf.Term
	args.LastLogIndex = rf.GetLastLogEntryWithoutLock().Index
	args.LastLogTerm = rf.TermForLog(args.LastLogIndex)
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
	for getReply != len(rf.peers) && votes <= len(rf.peers)/2 && needReturn == false {
		cond.Wait()
	}
	rf.mu.Unlock()
	if needReturn == true && votes == 1 {
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
				interval := generateTime()
				ticker = time.NewTicker(time.Duration(interval) * time.Millisecond)
				elec = true
				go func() {
					leader <- rf.startAsCand(interval)
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
