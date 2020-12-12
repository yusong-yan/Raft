package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../mygob"
	"../myrpc"
)

// import "bytes"
// import "../mygob"

const (
	Leader = iota + 1
	Cand
	Follwer
)
const (
	DidNotWin = iota + 1
	Win
)
const (
	Connect = iota + 1
	Disconnect
)
const (
	Ok = iota + 1
	Fail
)

const (
	Append = iota + 1
	CommitAndHeartBeat
	HeartBeat
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
	Id      int
}

type ClientMessageArgs struct {
	Message int
}

type ClientMessageReply struct {
	Message int
	Status  int
}

type AppendEntriesArgs struct {
	Job          int
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
	PeerId       int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	State       int
}

func generateTime() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := 700 - 350
	return 350 + r.Intn(diff)
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	Term := rf.Term
	isleader := rf.IsLeader
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

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := mygob.NewEncoder(w)
	e.Encode(rf.Term)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
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
	rf.VotedFor = VotedFor
	rf.Log = Logs
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.Peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.Peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.Term {
		//heartBeat
		rf.Term = args.Term
		if rf.IsLeader {
			rf.BecomeFollwerFromLeader <- true

		} else {
			rf.ReceiveHB <- true
		}
		rf.setFollwer()
		reply.Success = true
		reply.Term = rf.Term

		if args.Job == Append {
			//APPEND
			entr, find := rf.getLogAtIndexWithoutLock(args.PrevLogIndex)
			if !find { //give the last index
				reply.LastIndex = rf.getLastLogEntryWithoutLock().Index
				reply.Success = false
			} else {
				if entr.Term != args.PrevLogTerm {
					rf.Log = rf.Log[:indexInLog(args.PrevLogIndex)]
					reply.LastIndex = -1
					reply.Success = false
				} else {
					rf.Log = rf.Log[:indexInLog(args.PrevLogIndex+1)]
					if len(args.Entries) == 0 {
						if (len(rf.Log)) > 0 {
							rf.Log[len(rf.Log)-1].Term = args.Term
						}
					} else {
						rf.Log = append(rf.Log, args.Entries...)
					}
					reply.LastIndex = -1
					reply.Success = true
				}
			}
			rf.Term = args.Term
			rf.persist()
			return

		} else if args.Job == CommitAndHeartBeat {
			rf.CommitIndex = min(args.LeaderCommit, rf.getLastLogEntryWithoutLock().Index)
			rf.startApply()
			rf.persist()
			return
		}
	}
	//TERM IS BIGGER JUST REPLY TERM
	reply.Term = rf.Term
	reply.Success = false
	rf.persist()
	return
}

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//CHECK NETWORK

	rfLastIndex := rf.getLastLogEntryWithoutLock().Index
	rfLastTerm := rf.termForLog(rfLastIndex)

	if args.Term > rf.Term {
		rf.Term = args.Term
		if rf.IsLeader {
			rf.BecomeFollwerFromLeader <- true
		} else {
			rf.ReceiveHB <- true
		}
		rf.setFollwer()
		rf.VotedFor = -1
		////fmt.Println("receieve HB")
	}
	if (rf.VotedFor == -1) && ((rfLastTerm < args.LastLogTerm) || ((rfLastTerm == args.LastLogTerm) && (rfLastIndex <= args.LastLogIndex))) {
		rf.VotedFor = args.PeerId
		//fmt.Println("grand vote")
		reply.VoteGranted = true
	} else {
		//fmt.Println("Not grand vote")
		reply.VoteGranted = false
	}
	reply.Term = rf.Term
	reply.State = rf.State
	rf.persist()
}

type Raft struct {
	mu        sync.Mutex         // Lock to protect shared access to this peer's state
	Peers     []*myrpc.ClientEnd // RPC end points of all peers
	persister *Persister         // Object to hold this peer's persisted state
	Me        int                // this peer's index into peers[]
	dead      int32              // set by Kill()
	//
	Log                     []Entry
	IsLeader                bool
	State                   int
	Term                    int
	VotedFor                int
	ReceiveHB               chan bool
	BecomeFollwerFromLeader chan bool
	NextIndex               map[int]int
	MatchIndex              map[int]int
	PeerAlive               map[int]bool
	OpenCommit              map[int]bool
	CommitIndex             int
	LastApply               int
	ApplyChan               chan ApplyMsg
	peerAppendBuffer        []chan bool
}

func Make(peers []*myrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Peers = peers
	rf.persister = persister
	rf.Me = me
	rf.State = Follwer
	rf.Log = []Entry{}
	rf.VotedFor = -1
	rf.IsLeader = false
	rf.Me = me
	rf.Term = 0
	rf.ReceiveHB = make(chan bool, 1)
	rf.BecomeFollwerFromLeader = make(chan bool, 1)
	rf.peerAppendBuffer = make([]chan bool, len(peers))
	rf.NextIndex = map[int]int{}
	rf.MatchIndex = map[int]int{}
	rf.PeerAlive = map[int]bool{}
	//rf.PeerCommit = false
	rf.OpenCommit = map[int]bool{}
	rf.ApplyChan = applyCh
	rf.CommitIndex = 0
	rf.LastApply = 0
	for i := 0; i < len(rf.Peers); i++ {
		server := i
		rf.NextIndex[server] = rf.getLastLogEntryWithoutLock().Index + 1
		rf.MatchIndex[server] = rf.NextIndex[server] - 1
		rf.PeerAlive[server] = true
	}
	go rf.startElection()
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) startElection() {
	for !rf.killed() {
		ticker := time.NewTicker(time.Duration(generateTime()) * time.Millisecond)
		electionResult := make(chan int, 1)
	Loop:
		for !rf.killed() {
			select {
			case <-ticker.C:
				interval := generateTime()
				ticker = time.NewTicker(time.Duration(interval) * time.Millisecond)
				go func() {
					electionResult <- rf.startAsCand(interval)
				}()
			case <-rf.ReceiveHB:
				ticker = time.NewTicker(time.Duration(generateTime()) * time.Millisecond)
			case a := <-electionResult:
				if a == Win {
					break Loop
				}
			default:
			}
		}

		ticker.Stop()
		go rf.startAsLeader()
		<-rf.BecomeFollwerFromLeader
	}
}

func (rf *Raft) startAsCand(interval int) int {
	cond := sync.NewCond(&rf.mu)
	var needReturn bool
	needReturn = false
	go func(needReturn *bool, cond *sync.Cond) {
		time.Sleep(time.Duration(interval-20) * time.Millisecond)
		rf.mu.Lock()
		*needReturn = true
		rf.mu.Unlock()
		cond.Signal()
	}(&needReturn, cond)

	//setup args and rf
	hearedBack := 1
	hearedBackSuccess := 1
	votes := 1
	args := RequestVoteArgs{}
	rf.mu.Lock()
	rf.State = Cand
	rf.Term = rf.Term + 1
	rf.VotedFor = rf.Me
	args.Term = rf.Term
	args.PeerId = rf.Me
	args.LastLogIndex = rf.getLastLogEntryWithoutLock().Index
	args.LastLogTerm = rf.termForLog(args.LastLogIndex)
	rf.persist()
	rf.mu.Unlock()
	//fmt.Println(rf.Me, "start election with lastIndex", args.LastLogIndex, "and lastlongTerm", args.LastLogTerm)
	for s := 0; s < len(rf.Peers); s++ {
		server := s
		if server == rf.Me {
			continue
		}
		reply := RequestVoteReply{}

		go func() {
			ok := rf.sendRequestVote(server, &args, &reply)
			//Handle Reply
			if !ok || needReturn {
				rf.mu.Lock()
				hearedBack++
				rf.mu.Unlock()
				cond.Signal()
				return
			}
			rf.mu.Lock()
			hearedBack++
			hearedBackSuccess++
			if reply.Term > rf.Term && rf.State == Cand {
				rf.ReceiveHB <- true
				rf.setFollwer()
				rf.Term = reply.Term
				rf.persist()
				rf.mu.Unlock()
				cond.Signal()
				return
			}

			if reply.VoteGranted == true && rf.State == Cand {
				votes++
			}
			rf.mu.Unlock()
			cond.Signal()
		}()
	}
	//wait
	rf.mu.Lock()
	for hearedBack != len(rf.Peers) && votes <= len(rf.Peers)/2 && needReturn == false && rf.State == Cand {
		cond.Wait()
	}
	//decide
	if votes > len(rf.Peers)/2 && rf.State == Cand && needReturn == false {
		rf.mu.Unlock()
		return Win
	} else {
		rf.mu.Unlock()
		return DidNotWin
	}
}

func (rf *Raft) startAsLeader() {
	rf.mu.Lock()
	for i := 0; i < len(rf.Peers); i++ {
		server := i
		rf.NextIndex[server] = rf.getLastLogEntryWithoutLock().Index + 1
		rf.MatchIndex[server] = rf.NextIndex[server] - 1
		rf.PeerAlive[server] = true
		rf.OpenCommit[server] = false
		rf.peerAppendBuffer[server] = make(chan bool)
		go func() {
			rf.peerAppendBuffer[server] <- true
		}()
	}
	rf.setLeader()
	rf.mu.Unlock()
	rf.Start(nil)
	for !rf.killed() {
		go rf.sendHeartBeat()
		if rf.getState() != Leader {
			return
		}
		time.Sleep(time.Duration(120) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat() {
	if rf.getState() == Leader {
		hearedBack := 1
		hearedBackSuccess := 1
		for s := 0; s < len(rf.Peers); s++ {
			server := s
			if server == rf.Me {
				continue
			}

			args := AppendEntriesArgs{}
			args.LeaderId = rf.Me
			args.Entries = []Entry{}
			args.Job = CommitAndHeartBeat
			rf.mu.Lock()
			args.LeaderCommit = rf.CommitIndex
			args.Term = rf.Term
			args.Job = HeartBeat
			if rf.OpenCommit[server] {
				args.Job = CommitAndHeartBeat
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}

			go func() {
				ok := rf.sendAppendEntries(server, &args, &reply)
				//Handle Reply
				if !ok {
					rf.mu.Lock()
					hearedBack++
					rf.PeerAlive[server] = false
					rf.OpenCommit[server] = false
					rf.mu.Unlock()
					return
				}
				rf.mu.Lock()
				hearedBack++
				hearedBackSuccess++
				if reply.Term > rf.Term && rf.State == Leader {
					//fmt.Println(rf.Me, " become follwer from Term ", rf.Term, " to ", reply.Term)
					rf.Term = reply.Term
					rf.BecomeFollwerFromLeader <- true
					rf.setFollwer()
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if !rf.PeerAlive[server] && rf.State == Leader {
					rf.PeerAlive[server] = true
					go func() {
						rf.StartOnePeerAppend(server)
						rf.mu.Lock()
						if rf.updateCommitForLeader() && rf.IsLeader {
							rf.startApply()
						}
						rf.mu.Unlock()
					}()
				}
				rf.mu.Unlock()
			}()
		}
	}
}

func (rf *Raft) Start(Command interface{}) (int, int, bool) {
	Index := -1
	Term := -1
	IsLeader := rf.getState() == Leader
	//check if ID exist
	if IsLeader {
		<-rf.peerAppendBuffer[rf.Me]
		hearedBack := 1
		hearedBackSuccess := 1
		cond := sync.NewCond(&rf.mu)
		rf.mu.Lock()
		Term = rf.Term
		newE := Entry{}
		if Command != nil {
			newE.Command = Command
			newE.Index = rf.getLastLogEntryWithoutLock().Index + 1
			newE.Term = rf.Term
			rf.Log = append(rf.Log, newE)
		} else {
			if (len(rf.Log)) > 0 {
				rf.Log[len(rf.Log)-1].Term = rf.Term
			}
		}
		Index = rf.getLastLogEntryWithoutLock().Index
		rf.persist()
		rf.mu.Unlock()
		for i := 0; i < len(rf.Peers); i++ {
			server := i
			if server == rf.Me {
				continue
			}
			go func() {
				ok := rf.StartOnePeerAppend(server)
				rf.mu.Lock()
				hearedBack++
				if ok {
					hearedBackSuccess++
				}
				rf.mu.Unlock()
				cond.Signal()
			}()
		}

		//wait
		rf.mu.Lock()
		for hearedBack != len(rf.Peers) && hearedBackSuccess <= len(rf.Peers)/2 && rf.IsLeader {
			cond.Wait()
		}

		//decide
		if hearedBackSuccess <= len(rf.Peers)/2 && rf.IsLeader {
			rf.mu.Unlock()
			go func() {
				rf.peerAppendBuffer[rf.Me] <- true
			}()
			return -1, -1, false
		} else {
			if rf.updateCommitForLeader() && rf.IsLeader {
				rf.startApply()
				rf.mu.Unlock()
				go func() {
					rf.peerAppendBuffer[rf.Me] <- true
				}()
				return Index, Term, IsLeader
			} else {
				rf.mu.Unlock()
				go func() {
					rf.peerAppendBuffer[rf.Me] <- true
				}()
				return -1, -1, false
			}
		}
	}
	return -1, -1, false
}

func (rf *Raft) StartOnePeerAppend(server int) bool {
	<-rf.peerAppendBuffer[server]
	result := false
	if rf.getState() == Leader {
		//set up sending log
		entries := []Entry{}
		rf.mu.Lock()
		for i := rf.MatchIndex[server] + 1; i <= rf.getLastLogEntryWithoutLock().Index; i++ {
			entry, find := rf.getLogAtIndexWithoutLock(i)
			if !find {
				entries = []Entry{}
				break
			}
			entries = append(entries, entry)
		}
		args := AppendEntriesArgs{}
		args.LeaderId = rf.Me
		args.Term = rf.Term
		args.PrevLogIndex = rf.MatchIndex[server]
		args.PrevLogTerm = rf.termForLog(args.PrevLogIndex)
		args.Entries = entries
		args.LeaderCommit = rf.CommitIndex
		args.Job = Append
		rf.mu.Unlock()
		for rf.getState() == Leader && !rf.killed() {
			reply := AppendEntriesReply{}
			rf.mu.Lock()
			if rf.PeerAlive[server] && rf.IsLeader {
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					rf.mu.Lock()
					rf.PeerAlive[server] = false
					rf.OpenCommit[server] = false
					rf.mu.Unlock()
					result = false
					break
				}
			} else {
				rf.mu.Unlock()
				result = false
				break
			}

			if reply.Success {
				//update
				rf.mu.Lock()
				rf.MatchIndex[server] = len(args.Entries) + args.PrevLogIndex
				rf.NextIndex[server] = rf.MatchIndex[server] + 1
				rf.OpenCommit[server] = true
				rf.PeerAlive[server] = true
				rf.mu.Unlock()
				result = true
				break
			} else {
				//resend
				rf.mu.Lock()
				rf.PeerAlive[server] = true
				args.Term = rf.Term
				args.LeaderCommit = rf.CommitIndex
				if reply.LastIndex != -1 {
					//if server's log size bigger than rflog size
					args.PrevLogIndex = reply.LastIndex
				} else {
					args.PrevLogIndex = args.PrevLogIndex - 1
				}
				args.PrevLogTerm = rf.termForLog(args.PrevLogIndex)
				args.Entries = rf.Log[indexInLog(args.PrevLogIndex+1):]
				rf.mu.Unlock()
			}
		}
	}
	go func() {
		rf.peerAppendBuffer[server] <- true
	}()
	return result
}

func (rf *Raft) updateCommitForLeader() bool {
	beginIndex := rf.CommitIndex + 1
	lastCommittedIndex := -1
	updated := false
	for ; beginIndex <= rf.getLastLogEntryWithoutLock().Index; beginIndex++ {
		granted := 1

		for Server, ServerMatchIndex := range rf.MatchIndex {
			if Server == rf.Me || !rf.PeerAlive[Server] {
				continue
			}
			if ServerMatchIndex >= beginIndex {
				granted++
			}
		}

		if granted >= len(rf.Peers)/2+1 {
			lastCommittedIndex = beginIndex
		}
	}
	if lastCommittedIndex > rf.CommitIndex && rf.IsLeader {
		rf.CommitIndex = lastCommittedIndex
		updated = true
	}
	return updated
}

func (rf *Raft) startApply() {
	for rf.CommitIndex > rf.LastApply && !rf.killed() {
		rf.LastApply = rf.LastApply + 1
		am := ApplyMsg{}
		am.Command = rf.Log[indexInLog(rf.LastApply)].Command
		am.CommandIndex = rf.LastApply
		am.CommandValid = true
		rf.ApplyChan <- am
	}
}

func (rf *Raft) getState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.State
}

func (rf *Raft) getTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Term
}

func (rf *Raft) setLeader() {
	rf.IsLeader = true
	rf.State = Leader
	//fmt.Println("Become Leader with Term", rf.Term)
}

func (rf *Raft) setFollwer() {
	rf.State = Follwer
	rf.IsLeader = false
	//fmt.Println("Become Follwer with Term", rf.Term)
}

//for log
func (rf *Raft) getLogAtIndexWithoutLock(index int) (Entry, bool) {
	if index == 0 {
		return Entry{}, true
	} else if len(rf.Log) == 0 {
		return Entry{}, false
	} else if (index < -1) || (index > rf.getLastLogEntryWithoutLock().Index) {
		return Entry{}, false
	} else {
		localIndex := index - rf.Log[0].Index
		return rf.Log[localIndex], true
	}
}

func (rf *Raft) getLogAtIndex(index int) (Entry, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLogAtIndexWithoutLock(index)
}

func (rf *Raft) getLastLogEntryWithoutLock() Entry {
	entry := Entry{}
	if len(rf.Log) == 0 {
		entry.Term = 0
		entry.Index = 0
	} else {
		entry = rf.Log[len(rf.Log)-1]
	}
	return entry
}

func (rf *Raft) getLastLogEntry() Entry {
	entry := Entry{}
	rf.mu.Lock()
	entry = rf.getLastLogEntryWithoutLock()
	rf.mu.Unlock()
	return entry
}

func (rf *Raft) termForLog(index int) int {
	entry, ok := rf.getLogAtIndexWithoutLock(index)
	if ok {
		return entry.Term
	} else {
		return -1
	}
}

func indexInLog(index int) int {
	if index > 0 {
		return index - 1
	} else {
		println("ERROR")
		return -1
	}
}
