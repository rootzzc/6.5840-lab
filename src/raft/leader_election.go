package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

// --- 请求投票 --- //

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.currentTerm == args.Term {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// --- leader发送heartbeat信号 --- //

type RequestAppendArgs struct {
	Term     int
	LeaderId int
}

type RequestAppendReply struct {
	Term    int
	Success bool
}

// 响应leader的heartbeat
func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.currentTerm > args.Term {
		return
	} else if rf.currentTerm == args.Term {
		reply.Success = true
		rf.electionTimer.Reset(rf.getRandomElectionTimeout())
	} else {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.electionTimer.Reset(rf.getRandomElectionTimeout())
	}

	DPrintf("%v recevie heartbeat from %v term %v", rf.me, args.LeaderId, rf.currentTerm)
}

// leader send append to server
func (rf *Raft) sendAppendEntries(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if reply.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.mu.Unlock()
	}
	return ok
}

// leader持续向其他peers发送heartbeat信号
// 如果hearbeat返回term高于自身，则转变为follower
func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false && rf.isLeader() == true {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				args := &RequestAppendArgs{}
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				reply := &RequestAppendReply{}

				rf.sendAppendEntries(i, args, reply)
			}(i)
		}
		time.Sleep(time.Duration(25) * time.Millisecond)
	}
}

// -- 选举 -- //

func (rf *Raft) isLeader() bool {
	z := atomic.LoadInt64(&rf.state)
	return z == Leader
}

// get random number between timeoutBase ~ timeoutBase + 300
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	ms := rf.timeoutBase + rand.Int63()%300
	return time.Duration(ms) * time.Millisecond
}

// start leader election
func (rf *Raft) leaderElection() {
	DPrintf("start election from id:%v\n", rf.me)

	// 修改node状态，递增term，为自己投票
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// 向自身之外的peers请求vote
	vote := 1
	votes := []int{rf.me}
	n := 0
	c := sync.NewCond(&sync.Mutex{})
	vote_lk := sync.Mutex{}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		n++

		// 必须并行执行投票请求
		// 同步等待网络断开花销时间久，无法及时发送heartbeat建立leader权威，导致多个follower election time out，此时可能出现split votes的问题，导致无法选举出leader
		go func(i int) {
			arg := &RequestVoteArgs{}
			arg.Term = rf.currentTerm
			arg.CandidateId = rf.me
			rep := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, arg, rep)
			if rep.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = rep.Term
				rf.state = Follower
				rf.mu.Unlock()
			} else if ok == true && rep.VoteGranted == true {
				vote_lk.Lock()
				votes = append(votes, i)
				vote++

				if rf.isLeader() == false && vote > len(rf.peers)/2 {
					DPrintf("establish election from id:%v: votes:%v\n", rf.me, votes)
					rf.mu.Lock()
					rf.state = Leader
					n = 0
					rf.mu.Unlock()
					go rf.sendHeartbeat()

					c.L.Lock()
					n = 0
					c.L.Unlock()
				}
				vote_lk.Unlock()
			}

			c.L.Lock()
			n--
			c.L.Unlock()
			c.Broadcast()
		}(i)
	}

	// 条件变量，能满足majorty votes时，提前返回
	// 不能满足majorty votes时，等待所有请求结束后执行后续步骤
	c.L.Lock()
	for n > 0 {
		c.Wait()
	}
	c.L.Unlock()

	if vote <= len(rf.peers)/2 {
		DPrintf("fail establish election from id:%v: votes:%v\n", rf.me, votes)
		rf.electionTimer.Reset(rf.getRandomElectionTimeout())
	}
}
