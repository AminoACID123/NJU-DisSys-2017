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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)
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
	Command interface{}
	Term	int
}

type Log struct{
	mu        		sync.Mutex
	entries			[]Entry
}

func (log *Log) appendEntry(term int, cmd interface{}){
	log.mu.Lock()
	defer log.mu.Unlock()
	entry := Entry{
		Command: cmd,
		Term:term,
	}
	log.entries = append(log.entries, entry)
}

func (log *Log) appendEntries(entries []Entry){
	log.mu.Lock()
	defer log.mu.Unlock()

	log.entries = append(log.entries, entries...)
}

func (log *Log) getEntry(i int) Entry{
	log.mu.Lock()
	log.mu.Unlock()
	if i < 1{
		return Entry{}
	}
	return log.entries[i-1]
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
	return log.entries[len(log.entries)-1].Term
}

func (log *Log) clear() {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.entries = []Entry{}
}

func (log *Log) deleteEntries(i int) {
	log.mu.Lock()
	defer log.mu.Unlock()
	n := len(log.entries)
	if i > n{
		return
	}
	if i == 0{

		log.entries = []Entry{}
		return
	}
	log.entries = log.entries[0: i]
}

func (log *Log) getEntries(i int) []Entry {
	log.mu.Lock()
	defer log.mu.Unlock()
	n := len(log.entries)
	if i > n{
		return []Entry{}
	}

	return log.entries[i-1: n]
}

type Raft struct {
	t				sync.Mutex
	my            	sync.Mutex
	mu            	sync.Mutex
	cm				sync.Mutex
	vt 				sync.Mutex
	hb 				sync.Mutex
	peers         	[]*labrpc.ClientEnd
	persister     	*Persister
	me            	int // index into peers[]
	log           	*Log
	currentTerm   	int
	votedFor      	int
	lastHeartBeat 	int64
	Type          	ServerType
	wg            	sync.WaitGroup
	voteCount		int
	nextIndex		[]int
	matchIndex		[]int
	commitIndex		int
	lastApplied		int
	msgChan			chan ApplyMsg
	ready				bool
}

func (rf *Raft) GetType() ServerType{
	rf.t.Lock()
	defer rf.t.Unlock()
	return rf.Type
}

func (rf *Raft) SetType(serverType ServerType){
	rf.t.Lock()
	defer rf.t.Unlock()
	rf.Type = serverType
}

func (rf *Raft) getMajorityCount() int{
	rf.my.Lock()
	defer rf.my.Unlock()
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


func (rf *Raft) resetVoteCount() {
	rf.my.Lock()
	rf.voteCount = 0
	rf.my.Unlock()
}

func (rf *Raft) incVoteCount() {
	rf.my.Lock()
	rf.voteCount++
	rf.my.Unlock()
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

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.my.Lock()
	defer rf.my.Unlock()

	//fmt.Println(args.Term)
	//fmt.Println(rf.currentTerm)
	//fmt.Print("\n")
	//reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term{
 		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}else if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.Type = Follower
	}
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		if args.LastLogTerm > rf.log.getLastLogTerm(){
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		}else if args.LastLogTerm == rf.log.getLastLogTerm() && args.LastLogIndex >= rf.log.getLastLogIndex(){
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		}
	}else {
		reply.VoteGranted = false
	}

	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.Type = Follower
	}

}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	//reply := &RequestVoteReply{Term: -1,VoteGranted: false}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok == false {
		return false
	}
	//fmt.Printf("%d %d\n", rf.me, server)

	if reply.VoteGranted{
		rf.vt.Lock()
		rf.voteCount++
		rf.vt.Unlock()
	}else if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.Type = Follower
	}

	return ok
}



type AppendEntriesArgs struct{
	Term			int
	LeaderID		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[] Entry
	LeaderCommit	int
	Ready			bool
}

type AppendEntriesReply struct{
	Term			int
	Success 		bool
}

func (rf *Raft) followerCommit(args AppendEntriesArgs) {
	rf.cm.Lock()
	defer rf.cm.Unlock()
	fmt.Printf("%+v   %d -> %d %+v\n\n\n",args.Entries, args.LeaderID, rf.me, rf.log.entries)
	//fmt.Printf("%d %+v\n\n",rf.me, args)
	if  args.Ready == false{
		return
	}
	s := rf.commitIndex
	var e int

	if args.LeaderCommit < rf.log.getLastLogIndex(){
		e = args.LeaderCommit
	}else {
		e = rf.log.getLastLogIndex()
	}

	for k := s+1; k<=e;k++{
	//	if args.Entries[k-1].Term != args.Term{
	//		return
	//	}
		msg := ApplyMsg{Command: rf.log.getEntry(k).Command,Index: k}
		fmt.Printf("%+v  %d\n", msg, len(rf.log.entries))
		//fmt.Printf("%+v\n",msg)
		rf.msgChan <- msg
	}

	rf.commitIndex = e
	rf.ready = false

}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.my.Lock()
	defer rf.my.Unlock()


	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term{
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}else{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if len(args.Entries) == 0 {
		//rf.heartBeatChan <- args.Term
		rf.SetType(Follower)
		reply.Success = true
		rf.lastHeartBeat = time.Now().UnixNano()
		if args.LeaderCommit > rf.commitIndex{
			go rf.followerCommit(args)
		}
	}else{
		//fmt.Printf("%+v\n", rf.log.entries)
		if args.PrevLogIndex > rf.log.getLastLogIndex(){
			reply.Success = false
			return
		}
		if args.PrevLogIndex == 0{

			rf.log.clear()
			rf.log.appendEntries(args.Entries)
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex{
				go rf.followerCommit(args)
			}
			return
		}else if rf.log.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm{
			reply.Success = false
			return
		}else if rf.log.getEntry(args.PrevLogIndex).Term == args.PrevLogTerm {
			reply.Success = true
			rf.log.deleteEntries(args.PrevLogIndex)
			rf.log.appendEntries(args.Entries)
			if args.LeaderCommit > rf.commitIndex{
				go rf.followerCommit(args)
			}
			return
		}else{
			reply.Success = false
			return
		}
	}
}

func (rf *Raft) repLog(server int){
	//fmt.Printf("%d\n",server)
	for {

		//fmt.Printf("%+v\n\n",rf)
		reply := &AppendEntriesReply{}
		reply.Term = -1
		reply.Success = false
		args := AppendEntriesArgs{
			Term:			rf.currentTerm,
			LeaderID:		rf.me,
			LeaderCommit:	rf.commitIndex,
			PrevLogIndex: 	rf.nextIndex[server]-1,
			PrevLogTerm: 	rf.log.getEntry(rf.nextIndex[server]-1).Term,
			Entries: 		rf.log.getEntries(rf.nextIndex[server]),
			Ready: 			false,
		}
		//fmt.Printf("%+v\n",args)


		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)


		if !ok{
			continue
		}

		//fmt.Printf("%v\n\n\n\n", reply.Success)
		if rf.currentTerm < reply.Term{
			rf.Type = Follower
	//		rf.my.Unlock()
			return
		}else if reply.Success == false{
			rf.nextIndex[server]--
	//		rf.my.Unlock()
			continue
		}else if reply.Success == true{
			rf.nextIndex[server] = rf.log.getLastLogIndex()+1
			rf.matchIndex[server] = rf.nextIndex[server]-1
			//fmt.Printf("%+v\n", rf.matchIndex)
	//		rf.my.Unlock()
			return
		}

	//	rf.my.Unlock()
	}
}

// HeartBeat Thread for a single follower
func (rf *Raft) heartBeat(server int){

		args := AppendEntriesArgs{
			Term:	rf.currentTerm,
			PrevLogIndex: 0,
			PrevLogTerm: 0,
			LeaderCommit: rf.commitIndex,
			LeaderID: rf.me,
			Ready: rf.ready,
		}

		//rf.sendAppendEntries(server,args)
	reply := &AppendEntriesReply{Term: -1,Success: false}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok == false{
		//fmt.Printf("Fail %d %d\n", rf.me, server)
		return
	}

	//rf.my.Lock()
	if reply.Term > rf.currentTerm{
		rf.Type = Follower
	//	rf.my.Unlock()
		return
	}
}


func (rf *Raft) StartAgree() {
	//var waitgroup sync.WaitGroup

	rf.ready =false
	for idx,_ := range rf.peers{
		if idx == rf.me{
			continue
		}

		if rf.log.getLastLogIndex() >= rf.nextIndex[idx] {
			//waitgroup.Add(1)
			go rf.repLog(idx)
		}
	}
	//waitgroup.Done()
	time.Sleep(time.Millisecond*100)
	for N:=rf.log.getLastLogIndex();N>rf.commitIndex;N--{
		cnt := 1
		for _,v := range rf.matchIndex{
			if v >= N{
				cnt ++
			}
		}

		if cnt >= rf.getMajorityCount() && rf.log.getEntry(N).Term == rf.currentTerm{
			//fmt.Printf("N %d\n\n\n\n",N)
			for i:=rf.commitIndex+1;i<=N;i++{
				e := rf.log.getEntry(i)
				msg := ApplyMsg{Command: e.Command, Index: i}
				//if e.Term != rf.currentTerm{
				//	break
				//}
				//fmt.Printf("%+v\n\n",msg)
				rf.msgChan <- msg
			}
			rf.commitIndex = N
			rf.nextIndex[rf.me] = N + 1
			rf.ready = true
			//rf.matchIndex[rf.me] = rf.commitIndex
			break
		}
	}
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



	term = rf.currentTerm

	//time.Sleep(time.Millisecond*20)
	isLeader := rf.Type == Leader
	if isLeader {

		rf.log.appendEntry(rf.currentTerm, command)
		index = rf.log.getLastLogIndex()

		go rf.StartAgree()
	}
	return index, term, isLeader
}

func (rf *Raft) FollowerLoop(){

	rf.SetType(Follower)
	t := int64(rand.Intn(5)*100+500)

	for rf.Type == Follower {

		if (time.Now().UnixNano() - rf.lastHeartBeat)/1000000 > t{
			fmt.Printf("%d     aaa\n\n\n",rf.me)
			go rf.CandidateLoop()
			return
		}
		//time.Sleep(time.Millisecond*50)
	}
}

func (rf *Raft) CandidateLoop(){
	//fmt.Println("aaa")
	//fmt.Println(len(rf.peers))
	rf.SetType(Candidate)
	//rf.Type = CandidateLoop
	//resChan := make(chan RequestVoteReply, len(rf.peers)-1)
	continueVote := true
	t := int64(rand.Intn(10)*10+100)
	timer := time.NewTimer(time.Duration(t) * time.Millisecond)

	for rf.Type == Candidate {
		if continueVote {
			continueVote = false
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.resetVoteCount()
			rf.incVoteCount()
			for idx, _ := range rf.peers {
				if idx == rf.me {
					continue
				}
				arg := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: rf.log.getLastLogIndex(),
					LastLogTerm:  rf.log.getLastLogTerm(),
				}
				//fmt.Println(idx)
				reply := &RequestVoteReply{Term: -1,VoteGranted: false}
				go rf.sendRequestVote(idx, arg,reply)
			}
		}
		select {
		case <-timer.C:
			continueVote = true
			timer.Reset(time.Duration(rand.Intn(150)+100 ) * time.Millisecond)
			continue
		default:
			if rf.voteCount >= rf.getMajorityCount(){
				rf.Type = Leader
				go rf.LeaderLoop()
				return
			} else if rf.Type == Follower {
				//fmt.Println(rf.me)
				go rf.FollowerLoop()
				return
			}
			continue
		}
	}
}


func (rf *Raft) LeaderLoop() {
	rf.SetType(Leader)
	rf.ready =false
	//stopChan := make(chan bool, len(rf.peers)-1)
	fmt.Printf("%d aaaaaa\n\n\n\n", rf.me)
	for idx,_ := range rf.peers{
		rf.nextIndex[idx] = rf.log.getLastLogIndex() + 1
		//fmt.Println(rf.nextIndex[idx])
		rf.matchIndex[idx] = 0
	}

	timer := time.NewTimer(100 * time.Millisecond)
	flag := true
	for rf.Type == Leader {

		if flag == true {
			flag = false
			for idx, _ := range rf.peers {
				if idx == rf.me {
					continue
				}
				go rf.heartBeat(idx)
			}
		}
		select {
			case <-timer.C:
				flag = true
				timer.Reset(100*time.Millisecond)

		default:
			if rf.Type == Follower{
				fmt.Printf("avasga\n\n\n\n")
				go rf.FollowerLoop()
				return
			}
		}
	}
	go rf.FollowerLoop()

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

func (rf *Raft) Report(){
	for {
		rf.my.Lock()
		fmt.Printf("ID: %d, TYPE: %d \n", rf.me, rf.Type)
		rf.my.Unlock()
		time.Sleep(time.Millisecond*200)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.Type = Follower
	rf.votedFor = -1
	rf.lastHeartBeat = time.Now().UnixNano()
	rf.log = &Log{}
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.msgChan = applyCh
	rf.ready = false
	// Your initialization code here.
	go rf.FollowerLoop()
//	go rf.Report()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
