package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"reflect"
	"sync"
)
import "labrpc"
import "time"

// import "bytes"
// import "encoding/gob"

type ServerType int32

const (
	Follower    ServerType = 0
	Leader      ServerType = 1
	Candidate   ServerType = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Entry struct{
	command interface{}
	term	int
}

type Log struct{
	mu        		sync.Mutex
	entries	[] 		*Entry
}

func (log *Log) getLastLogIndex() int {
	log.mu.Lock()
	defer log.mu.Unlock()
	return len(log.entries)
}

func (log *Log) getLastLogTerm() int {
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.entries) == 0{
		return 0
	}
	return log.entries[len(log.entries)-1].term
}

type Raft struct {
	mu        		sync.Mutex
	peers     		[]*labrpc.ClientEnd
	persister 		*Persister
	me        		int // index into peers[]
	log        		*Log
	currentTerm		int
	votedFor		int
	lastHeartBeat	int64
	Type 			ServerType
	wg 				sync.WaitGroup


}

func (rf *Raft) GetType() ServerType{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Type
}

func (rf *Raft) SetType(serverType ServerType){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Type = serverType
}

func (rf *Raft) getMajorityCount() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.peers)/2+1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	isLeader = rf.Type==Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int
	CandidateID		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term			int
	VoteGranted		bool
}

func (rf *Raft) handleRequestVote(args RequestVoteArgs, replyChan chan RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := RequestVoteReply{}
	reply.Term = rf.currentTerm
 	if rf.currentTerm > args.Term{
 		reply.VoteGranted = false
	} else if rf.votedFor == args.CandidateID || rf.votedFor == -1 {
		if args.LastLogTerm > rf.log.getLastLogTerm(){
			reply.VoteGranted = true
		}else if args.LastLogTerm == rf.log.getLastLogTerm() && args.LastLogIndex >= rf.log.getLastLogIndex(){
			reply.VoteGranted = true
		}
	}else {
		reply.VoteGranted = false
	}
	replyChan <- reply
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, replyChan chan RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.handleRequestVote", args, replyChan)
	return ok
}

type AppendEntriesArgs struct{
	Term			int
	LeaderID		int
	PrevLogIndex	int
	PrevLogTerm		int
	entries			[] interface{}
	LeaderCommit	int
}

type AppendEntriesReply struct{
	Term			int
	Success 		int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	//isLeader := true


	return index, term, rf.GetType() == Leader
}

func (rf *Raft) Follower(){

	for rf.Type == Follower {
		timer := time.NewTimer(300 *time.Millisecond)
		select{


			case  <-timer.C:
				go rf.Candidate()
				return
		}
	}
}

func (rf *Raft) Candidate(){
	rf.currentTerm++
	rf.Type = Candidate
	resChan := make(chan RequestVoteReply, len(rf.peers)-1)
	votesCount := 0
	continueVote := true
	timer := time.NewTimer(300 *time.Millisecond)
	for rf.Type == Candidate {
		if continueVote {
			votesCount = 1
			continueVote = false
			rf.currentTerm++
			rf.votedFor = rf.me

			for idx, peer := range rf.peers {
				if idx == rf.me {
					continue
				}
				rf.wg.Add(1)
				arg := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: rf.log.getLastLogIndex(),
					LastLogTerm:  rf.log.getLastLogTerm(),
				}
				go rf.sendRequestVote(idx,arg, resChan)
			}

			rf.wg.Done()

		}

		if votesCount >= rf.getMajorityCount() {
			go rf.Leader()
			return
		}

		select {
		case res := <-resChan:
			if res.VoteGranted {
				votesCount++
			}
		case <-timer.C:
			// 如果再一次超时了，重新发起选主请求
			continueVote = true
		}
	}
}

func (rf *Raft) Leader(){
	rf.SetType(Leader)
	for rf.GetType() == Leader {
		for i := 0; i < len(rf.peers);i++{
			if i != rf.me{
				args := AppendEntriesArgs{}
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					go rf.Follower()
					return
				}
			}
		}
		time.Sleep(time.Millisecond*150)
	}
}
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.Type = Follower



	// Your initialization code here.
	go rf.Follower()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
