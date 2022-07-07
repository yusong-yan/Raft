package raft

import "time"

//Sending election RPC
func (rf *Raft) StartElection() {
	//Yusong
	rf.ChangeState(StateCandidate)
	rf.currentTerm += 1
	request := new(RequestVoteArgs)
	lastLog := rf.raftLog.lastEntry()
	request.Term = rf.currentTerm
	request.CandidateId = rf.me
	request.LastLogIndex = lastLog.Index
	request.LastLogTerm = lastLog.Term
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	// use Closure
	grantedVotes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)
				// check if the term is equal to make sure that we are still in current round
				// check Candiate status to make sure we don't process following code if we are leader
				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader)
							rf.BroadcastHeartbeat(true)
							rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())
						}
					} else if response.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = response.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

//Handle received RPC
func (rf *Raft) HandleRequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.raftLog.dummyIndex(), rf.raftLog.lastEntry(), request, response)

	if request.Term < rf.currentTerm {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	response.Term = rf.currentTerm
	//from raft paper (Figure 2, RequestVote RPC, 2)
	if (rf.votedFor == -1 || rf.votedFor == request.CandidateId) &&
		rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		rf.votedFor = request.CandidateId
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		response.VoteGranted = true
		return
	}
	response.VoteGranted = false
}

// raft paper (5.41 in the end)
func (rf *Raft) isLogUpToDate(requestLastTerm int, requestLastIndex int) bool {
	mylastLog := rf.raftLog.lastEntry()
	//from raft paper
	return (requestLastTerm > mylastLog.Term ||
		mylastLog.Term == requestLastTerm && requestLastIndex >= mylastLog.Index)
}
