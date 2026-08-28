package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

/*
	Todo:
	[x]		init all peers as followers
	[x]		figure out what inital term value is| 0
	[x]		figure out where inital term value should be set | Raft server struct (server maintains state periodically persists)
	[x]		figure out how to trigger election (setting self to candidate) now - (lastHB) > some period
	[x]		set followers -> candidates if election period lasped
	[x]		track votedFor state
	[ ]		handleVote
	[ ]		handleElection
	[x]		cast vote for self to all peer
	[x]		handle already casted votes
	[ ]		handle races
*/

/*
	modeling
	states/modes [Leader, Follower, Candidate]
	transitions [
		Follower -> Candidate,
		Follower -> Follower
		Candidate -> Leader,
		Candidate -> Follower,
		Leader -> Follower
	]
	conditionals [
		Follower -> isTimedOut-> Candidate
		Follower -> isNotTimedOut -> Follower
		Candidate -> isElected -> Leader
		Candidate -> isNotElected -> Follower
		Leader -> isStateStale -> Follower
		Leader -> isStateFresh -> Follower
	]
*/

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
//Raft peers can act as all three states [leader, candidate, follower]
type Mode int
const (
	Leader = iota
	Candidate
	Follower
)


type Raft struct {
	me        	int      			   // the peer's index into peers[]
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*tester.Persister   // Object to hold this peer's persisted state
	Mode	  	Mode
	CurrentTerm	int
	VotedFor	int
	LastUpdated	time.Time
	electionInProgress	bool
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var currentTerm int
	currentTerm = rf.CurrentTerm

	var isLeader bool
	isLeader = rf.Mode == Leader

	return currentTerm, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx 
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//Create voter algorithm 3A. 
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term		int
	VoteGranted	bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	votedFor := rf.VotedFor
	// fmt.Printf("me: %v, currentTerm: %v, electionTerm: %v, votedFor: %v \n", rf.me, currentTerm, args.Term, votedFor)
	rf.mu.Unlock()

	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

	if (votedFor == -1 || votedFor == args.CandidateId) {
		rf.mu.Lock()
		rf.VotedFor = args.CandidateId
		rf.mu.Unlock()

		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = max(args.Term, currentTerm)
}

type AppendLogRequest struct{
	Term	int
}
type AppendLogResponse struct{
	Term int
}

func (rf *Raft) AppendLogEntry(args *AppendLogRequest, reply *AppendLogResponse){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.CurrentTerm {
		rf.Mode = Follower
		rf.electionInProgress = false
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}
	
	rf.LastUpdated = time.Now()
	fmt.Printf("Handled ping {me:%v, mode:%v} \n", rf.me, rf.Mode)
	// fmt.Printf("me: %v is alive {mode: %v, votedFor: %v, currentTerm: %v}\n", rf.me, rf.Mode, rf.VotedFor, rf.CurrentTerm)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
}

//3 states [follower, candidate, leader]
//follower -> candidate [when no request received after a period of time]
//candidate -> leader [when receives majority]
//candidate -> follower [when leader elected]
//leader -> crash [will lead to an election, but doesn't directly trigger it]
//each election starts a new term
//leader election triggered by heartbeat -> no responses following an [election timeout period] will trig election
//TODO
//Implement heartbeat monitor for followers -> leader 
//Implement election timeout period
//Implement election algo
//** Self vote by followers 
//** First come first serve canidacy
//** election time period 
//New leader is established by heartbeat response to all other candidates -> follower

func (rf *Raft) ticker() {
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Lock()
		fmt.Printf("ticking... {me: %v, mode: %v}\n", rf.me, rf.Mode)
		rf.mu.Unlock()

		rf.isTimedOut()
		rf.isElected()
		rf.isLeading()

		ms := (rand.Int63() % 231)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//all followers heartbeat 
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{Mode: Follower, CurrentTerm: 0, VotedFor:  -1, LastUpdated: time.Now()}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) isTimedOut(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Mode != Follower {
		return
	}

	elapsed := time.Since(rf.LastUpdated)
	ms := 100 + rand.Intn(101)
	timeout := 2*time.Second + time.Duration(ms)*time.Millisecond
	if elapsed > timeout {
		rf.Mode = Candidate
	} else {
		rf.Mode = Follower
	}
}

func (rf *Raft) isElected(){
	rf.mu.Lock()
	if rf.Mode != Candidate || rf.electionInProgress {
		rf.mu.Unlock()
		return
	}
	rf.electionInProgress = true

	// rf.VotedFor = rf.me
	rf.CurrentTerm += 1

	currentTerm := rf.CurrentTerm
	me := rf.me
	peers := rf.peers
	rf.mu.Unlock()

	var wg sync.WaitGroup
	replies := make([]RequestVoteReply, len(peers))
	for i := range peers {
		if i == me {
			continue
		}

		wg.Add(1)
		go func(i int){
			defer wg.Done()

			args := RequestVoteArgs{Term: currentTerm, CandidateId: me}
			reply := RequestVoteReply{}
			peers[i].Call("Raft.RequestVote", args, &reply)

			replies[i] = reply
		}(i)
	}
	wg.Wait()
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if(rf.Mode != Candidate){
		return
	}

	votes := 1
	//Anytime we findout a node has a term ahead of ours we become followers
	for _, r := range replies {
		if r.Term > rf.CurrentTerm {
			rf.CurrentTerm = r.Term
			rf.Mode = Follower
			rf.VotedFor = -1
			return
		} else if r.VoteGranted {
			votes += 1
		}
	}

	mid := (len(rf.peers)/2) + 1
	if votes >= mid {
		rf.Mode = Leader
	} else {
		rf.Mode = Follower
	}
	rf.VotedFor = -1
	rf.electionInProgress = false
	fmt.Printf("Election complete me:%v is %v \n", rf.me, rf.Mode)
}

func (rf *Raft) isLeading(){
	rf.mu.Lock()
	me := rf.me
	mode := rf.Mode
	peers := rf.peers
	currentTerm := rf.CurrentTerm
	rf.mu.Unlock()

	if mode != Leader {
		return
	}

	var wg sync.WaitGroup
	replies := make([]AppendLogResponse, len(peers))
	for i := range peers {
		if i == me {
			continue
		}

		wg.Add(1)
		go func(i int, wg *sync.WaitGroup){
			defer wg.Done()
		fmt.Printf("me: %v, ping:%v \n", rf.me, i)
			args := AppendLogRequest{Term: currentTerm}
			reply := AppendLogResponse{}
			peers[i].Call("Raft.AppendLogEntry", args, &reply)
			
			replies[i] = reply
		}(i, &wg)
	}
	wg.Wait()
	//Anytime we findout a node has a term ahead of ours we become followers
	for _, r := range replies {
		if r.Term > rf.CurrentTerm {
			rf.CurrentTerm = r.Term
			rf.Mode = Follower
			rf.VotedFor = -1
		}
	}
}