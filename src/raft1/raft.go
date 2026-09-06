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

/*
Log Append
	* Log Entry [term, command]
	* Leader will retry appending log entries to followers until they succeed
	* AppendLogEntry(term, leaderId, prevLogIndex, entries[], leaderCommitIndex)
		* False – if term < currentTerm
		* False – if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	* Leader forces consistency on followers by overwriting conflicting log entries with leader logs
		* Find the last common index and overwrite all entries after that index with leader entries
		* Send AppendLogEntry with this paylod to successfully append log
		* Decrement term/index until success
	* Implement election restriction check for vote requests
		* If candidate’s log is at least as up-to-date as receiver’s log, grant vote
		* If candidate’s log is less up-to-date than receiver’s log, deny vote
	[ ] Implement election restriction check for vote requests
	[ ] Implement log append and consistency check
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

type LogEntry struct {
	Term    int
	Command interface{}
}


type Raft struct {
	me        			int      			   // the peer's index into peers[]
	mu        			sync.Mutex          // Lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd // RPC end points of all peers
	persister 			*tester.Persister   // Object to hold this peer's persisted state
	Mode	  			Mode
	CurrentTerm			int
	VotedFor			int
	ElectionTimeout		time.Time
	logs				[]LogEntry
	//last log commited index
	commitIndex			int
	//last log entry ran through state machine
	lastAppliedIndex	int
	electionInProgress	bool
	applyChan			chan raftapi.ApplyMsg
	pNextIndex			[]int  
	pMatchIndex			[]int 
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
	Term			int
	VoteGranted		bool
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	me := rf.me
	currentTerm := rf.CurrentTerm
	votedFor := rf.VotedFor
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	// fmt.Printf("me: %v, currentTerm: %v, electionTerm: %v, votedFor: %v \n", rf.me, currentTerm, args.Term, votedFor)
	rf.mu.Unlock()

	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	} else if args.Term > currentTerm {
		rf.mu.Lock()
		rf.VotedFor = -1
		votedFor = -1

		rf.CurrentTerm = args.Term
		currentTerm = args.Term

		rf.Mode = Follower
		rf.mu.Unlock()
	}

	if ((votedFor == -1 || votedFor == args.CandidateId) && 
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex))) {
		rf.mu.Lock()
		rf.VotedFor = args.CandidateId
		rf.ElectionTimeout = generateElectionTimeout()
		fmt.Printf("Vote Logged| me: %v| votedFor: %v | candidateId: %v | lastLogIndex: %v | lastLogTerm: %v\n", me, votedFor, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
		rf.mu.Unlock()

		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = currentTerm
}

type AppendLogRequest struct{
	Term			int
	LeaderId		int	
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}
type AppendLogResponse struct{
	Term 	int
	Success bool
}

func (rf *Raft) AppendLogEntry(args *AppendLogRequest, reply *AppendLogResponse){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.CurrentTerm

	if args.Term >= currentTerm {
		rf.setToFollower(args.Term)
	} else {
		reply.Success = false
		reply.Term = currentTerm
		return
	}


	if len(args.Entries) > 0 {
		prevLog := rf.logs[args.PrevLogIndex]
		fmt.Printf("prevLog:%v, prevTerm:%v \n",prevLog, args.PrevLogTerm)
		if prevLog.Term != args.PrevLogTerm {
			reply.Success = false
			return
		}

		rf.logs = append(rf.logs, args.Entries...)
		reply.Success = true
		fmt.Printf("Replicated logs me:%v, logs:%v \n", rf.me, rf.logs)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.emitEntries(args.LeaderCommit, rf.logs[args.LeaderCommit:])
		rf.commitIndex = args.LeaderCommit
	}

	reply.Term = currentTerm
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	lastLogIndex, _ := rf.getLastLogIndexAndTerm()
	currentTerm := rf.CurrentTerm
	isLeader := rf.Mode == Leader
	me := rf.me
	rf.mu.Unlock()

	//append command to log if leader
	if isLeader {
		rf.mu.Lock()
		le := LogEntry{Term: currentTerm, Command: command}
		rf.logs = append(rf.logs, le)
		fmt.Printf("Appended me:%v, log: %v\n", me, le)
		//should we send after replication? 
		// rf.applyChan <- raftapi.ApplyMsg{CommandValid: true, Command: command, CommandIndex: rf.commitIndex}
		rf.mu.Unlock()
	}


	return lastLogIndex+1, currentTerm, isLeader
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
		
		rf.isTimedOut()
		rf.isElected()
		rf.isLeading()

		time.Sleep(time.Duration(125) * time.Millisecond)

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
	rf := &Raft{Mode: Follower, CurrentTerm: 0, VotedFor:  -1, ElectionTimeout: generateElectionTimeout(), commitIndex: 0, lastAppliedIndex: 0, electionInProgress: false}
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{Term: 0, Command: nil})
	rf.applyChan = applyCh
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

	if time.Now().After(rf.ElectionTimeout) {
		rf.Mode = Candidate
	}
}

/*
	Only candidates with up-to-date logs are eligible to win elections.
*/
func (rf *Raft) isElected(){
	rf.mu.Lock()
	mode := rf.Mode
	electionInProgress := rf.electionInProgress
	rf.mu.Unlock()

	if mode != Candidate || electionInProgress {
		return
	}

	rf.mu.Lock()
	rf.electionInProgress = true
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()

	// rf.VotedFor = rf.me
	rf.CurrentTerm += 1

	currentTerm := rf.CurrentTerm
	me := rf.me
	peers := rf.peers
	rf.mu.Unlock()

	go func () {
		time.Sleep(time.Until(generateElectionTimeout()))
		rf.mu.Lock()
		rf.electionInProgress = false
		rf.mu.Unlock()
	}()

	fmt.Printf("me: %v commencing vote term: %v -------------------------------------\n", me, currentTerm)
	votes := 1
	for i := range peers {
		if i == me {
			continue
		}

		go func(i int){
			rf.mu.Lock()
			args := RequestVoteArgs{Term: currentTerm, CandidateId: me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			rf.mu.Unlock()
			reply := RequestVoteReply{}

			ok := peers[i].Call("Raft.RequestVote", args, &reply)

			rf.mu.Lock()
			if(rf.Mode != Candidate){
				rf.electionInProgress = false
				rf.mu.Unlock()
				return
			}
			fmt.Printf("me: %v to peer: %v - ok: %v, vote: %v \n", me, i, ok, reply.VoteGranted)

			//Anytime we findout a node has a term ahead of ours we become followers
			if reply.Term > currentTerm {
				rf.setToFollower(reply.Term)
				rf.mu.Unlock()
				return
			} else {
				if reply.VoteGranted {
					votes += 1
				}
				count := votes

				if rf.isMajority(count) {
					rf.setToLeader()
					fmt.Printf("Election won me:%v term: %v \n", me, reply.Term)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

/*
	lastCommitIndex is incremented once a majority of the log entries have been replicated.
	lastAppliedIndex is incremented once the log entry has been applied to the state machine.
*/
func (rf *Raft) isLeading(){
	rf.mu.Lock()
	me := rf.me
	mode := rf.Mode
	peers := rf.peers
	currentTerm := rf.CurrentTerm
	lastLogIndex, _ := rf.getLastLogIndexAndTerm()
	rf.mu.Unlock()

	if mode != Leader {
		return
	}

	for i := range peers {
		if i == me {
			continue
		}
		rf.mu.Lock()
		pNextIndex :=  rf.pNextIndex[i]
		rf.mu.Unlock()

		go func(i int){
			args := AppendLogRequest{}
			//When do new logs need to be appended
			//detect when new entries are added to leader log
			rf.mu.Lock()
			if lastLogIndex >= pNextIndex {
				args = rf.buildAppendLogArgs(pNextIndex)
			} else {
				args = rf.buildHeartbeatArgs()
			}
			rf.mu.Unlock()
			reply := AppendLogResponse{}

			peers[i].Call("Raft.AppendLogEntry", args, &reply)
			
			rf.mu.Lock()
			if reply.Term > currentTerm {
				rf.setToFollower(reply.Term)
			}
			
			mode = rf.Mode
			
			if currentTerm == reply.Term && mode == Leader {
				if reply.Success {
					rf.pNextIndex[i] = pNextIndex + len(args.Entries)
					rf.pMatchIndex[i] = len(rf.logs)  - 1

					if qr, cmi := rf.isQuorumReached(); qr {
						fmt.Printf("Quorum reached me:%v, commitIndex:%v\n", me, cmi)
						fmt.Printf("Logs:%v \n", rf.logs)
						rf.commitIndex = cmi
						rf.emitEntries(cmi, args.Entries)
					}
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

func generateElectionTimeout() time.Time {
	ms := 325 + rand.Intn(150)
	return time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
  if len(rf.logs) > 0 {
    lastIndex := len(rf.logs) - 1
    return lastIndex, rf.logs[lastIndex].Term
  } else {
    return -1, -1
  }
}

func (rf *Raft) buildHeartbeatArgs() AppendLogRequest {
	args := AppendLogRequest{
		Term:          rf.CurrentTerm,
		LeaderId:      rf.me,
		PrevLogIndex:  -1,
		PrevLogTerm:   -1,
		Entries:       []LogEntry{},
		LeaderCommit:  rf.commitIndex,
	}
	return args
}

func (rf *Raft) buildAppendLogArgs(pIndex int) AppendLogRequest {
	entries := rf.logs[pIndex:]
	prevLogIndex := pIndex - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	args := AppendLogRequest{
		Term:          rf.CurrentTerm,
		LeaderId:      rf.me,
		PrevLogIndex:  prevLogIndex,
		PrevLogTerm:   prevLogTerm,
		Entries:       entries,
		LeaderCommit:  rf.commitIndex,
	}
	return args
}

func (rf *Raft) setToFollower(term int) {
	rf.Mode = Follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.ElectionTimeout = generateElectionTimeout()
}

func (rf *Raft) setToLeader() {
	lastLogIndex, _ := rf.getLastLogIndexAndTerm()
	rf.Mode = Leader
	rf.electionInProgress = false
	rf.pNextIndex = make([]int, len(rf.peers))
	rf.pMatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.pNextIndex[i] = lastLogIndex + 1
		rf.pMatchIndex[i] = 0
	}
}

	//when commit index matches pNextIndex
	/*
		init 
		L: leader
			term = 1
			commit index = 0
			pNextIndex [_, 1, 1]
			logs [{0 nil}]
		L: submit {100}
			logs [{0 nil}, {1, 100}]
			determine when applyChan message should be sent
		L: appendLogEntry
			prevLogIndex = pNextIndex[i] - 1
			Term:          1,
			LeaderId:      rf.me,
			PrevLogIndex:  prevLogIndex,
			PrevLogTerm:   logs[prevLogIndex].Term,
			Entries:       logs[1:],
			LeaderCommit:  0,
		P: handleAppend
			//at some point an election happended and this is a bad term
			if term < peer.Term
				false
			//our logs up to this point aren't matched up so reject
			if peer.logs[prevLogIndex].Term != prevLogTerm (0 != 0)
				false
			
			if leaderCommit > peer.commitIndex
				peer.commitIndex = min(leaderCommit, len(peer.logs) - 1)

			peer.logs = append(peer.logs, entries...)

			true
		L: handleSuccess
			pNextIndex[i] = pNextIndex[i] + entries
		L: checkQuorum
			check all matchIndexes == len(leader.logs)
	*/

func (rf *Raft) isQuorumReached() (bool, int) {
	matchCount := 1
	for p := range rf.peers {
		if p == rf.me {
			continue
		}

		if rf.pMatchIndex[p] == len(rf.logs)-1{
			matchCount++
		}
	}
	mid := (len(rf.peers)/2) + 1

	return mid >= matchCount, len(rf.logs) - 1
}

func (rf *Raft) isMajority(n int) bool {
	mid := (len(rf.peers)/2) + 1

	return n >= mid
}

func (rf *Raft) emitEntries(commitIndex int, entries []LogEntry){
	for _, e := range entries {
		rf.applyChan <- raftapi.ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: commitIndex}
	}
}