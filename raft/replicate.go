package raft

//HeartBeat
func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// leader will try to send heartbeat constantly
			go rf.replicateOneRound(peer)
		} else {
			// activate replicator thread for this peer
			rf.replicatorCond[peer].Signal()
		}
	}
}

//One peer fix, sending RPC
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.raftLog.dummyIndex() {
		// only snapshot can catch up
		// request := rf.genInstallSnapshotRequest()
		// rf.mu.RUnlock()
		// response := new(InstallSnapshotResponse)
		// if rf.sendInstallSnapshot(peer, request, response) {
		// 	rf.mu.Lock()
		// 	rf.handleInstallSnapshotResponse(peer, request, response)
		// 	rf.mu.Unlock()
		// }
	} else {
		// just entries can catch up
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			// Here, we might activate more replicateOneRound depend on
			// whether we can fix this peer's log in this round
			rf.mu.Lock()
			rf.processAppendEntriesReply(peer, request, response)
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) processAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.ChangeState(StateFollower)
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.persist()
	} else if reply.Term == rf.currentTerm {
		if reply.Success {
			rf.nextIndex[peer] = len(args.Entries) + args.PrevLogIndex + 1
			rf.matchIndex[peer] = len(args.Entries) + args.PrevLogIndex // if newnext > rf.nextIndex(From MIT)
			rf.advanceCommitIndexForLeader()
		} else if reply.ConflictIndex != 0 {
			// we find conflict, and then step back one by one
			rf.nextIndex[peer] = reply.ConflictIndex - 1
			rf.replicatorCond[peer].Signal()
		}
	}
}
func (rf *Raft) advanceCommitIndexForLeader() {
	// find smallest match, and start from there
	latestGrantedIndex := rf.matchIndex[0]
	for _, ServerMatchIndex := range rf.matchIndex {
		latestGrantedIndex = min(latestGrantedIndex, ServerMatchIndex)
	}
	// move forward, until lost the majority
	for i := latestGrantedIndex + 1; i <= rf.raftLog.lastIndex(); i++ {
		granted := 1
		for Server, ServerMatchIndex := range rf.matchIndex {
			if Server != rf.me && ServerMatchIndex >= i {
				granted++
			}
		}
		if granted < len(rf.peers)/2+1 {
			break
		}
		latestGrantedIndex = i
	}
	//from raft paper (Rules for Servers, leader, last bullet point)
	if latestGrantedIndex > rf.commitIndex &&
		rf.raftLog.getEntry(latestGrantedIndex).Term == rf.currentTerm &&
		rf.state == StateLeader {
		rf.commitIndex = latestGrantedIndex
		rf.applyCond.Signal()
	}
}

//Handle the received RPC
func (rf *Raft) HandleAppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.raftLog.dummyIndex(), rf.raftLog.lastIndex(), request, response)
	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if request.PrevLogIndex < rf.raftLog.dummyIndex() {
		response.Term, response.Success = 0, false
		DPrintf1("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.raftLog.dummyIndex())
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.raftLog.lastIndex()
		if request.PrevLogIndex > lastIndex {
			response.ConflictIndex = lastIndex + 1
		} else {
			response.ConflictIndex = request.PrevLogIndex
		}
		return
	}
	// we connect entries with logs, by minimize the delete of rf.logs
	rf.raftLog.trunc(request.PrevLogIndex + 1)
	rf.raftLog.append(request.Entries...)
	// raft paper (AppendEntries RPC, 5)
	rf.commitIndex = min(request.LeaderCommit, rf.raftLog.lastIndex())
	rf.applyCond.Signal()

	response.Term, response.Success = rf.currentTerm, true
}
func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesArgs {
	args := new(AppendEntriesArgs)
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.raftLog.getEntry(prevLogIndex).Term
	args.Entries = make([]Entry, rf.raftLog.lastIndex()-prevLogIndex)
	args.LeaderCommit = rf.commitIndex
	copy(args.Entries, rf.raftLog.sliceFrom(prevLogIndex+1))
	return args
}

// raft paper (search log match)
func (rf *Raft) matchLog(requestPrevTerm int, requestPrevIndex int) bool {
	// there is no such entry exist because there is no such index
	if requestPrevIndex > rf.raftLog.lastIndex() {
		return false
	}
	// check the index, if the term is the same
	targetLog := rf.raftLog.getEntry(requestPrevIndex)
	if requestPrevIndex == targetLog.Index && requestPrevTerm == targetLog.Term {
		return true
	}
	return false
}
