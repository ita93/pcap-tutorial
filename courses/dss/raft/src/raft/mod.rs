use std::cell::RefCell;
use std::default;
use std::sync::mpsc::{channel, sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc::UnboundedSender;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(300);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(500);
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

struct RaftServerLeaderState {
    // index of next log entry to send to a server (each entry per followers)
    next_idx: Vec<u64>,
    // index of highest log entry known to be replicated on server
    match_idx: Vec<u64>,
}

#[derive(Clone, PartialEq)]
enum Role {
    Leader,
    Candidate,
    Follower,
}

struct RaftServerState {
    // candidate ids that received vote in current turn (or empty)
    voted_for: Option<usize>,
    // log entries, each entry contains command for state machine, and term
    // when entry was received by leader
    logs: Vec<(String, u64)>,
    // index of highest log entry known to be commited (default 0)
    commit_idx: u64,
    // index of highest log entry applied to state machine
    last_applied: u64,
    // only when server is in leader Role
    leader_state: Option<RaftServerLeaderState>,
    // role of server
    role: Role,
    // we don't need term here, because it was store in Arc<State>
}

impl Default for RaftServerState {
    fn default() -> Self {
        // section 5.3: Node always start in follower mode
        Self::new(None, Vec::new(), 0, 0, None, Role::Follower)
    }
}

impl RaftServerState {
    fn new(
        voted_for: Option<usize>,
        logs: Vec<(String, u64)>,
        commit_idx: u64,
        last_applied: u64,
        leader_state: Option<RaftServerLeaderState>,
        role: Role,
    ) -> Self {
        Self {
            voted_for,
            logs,
            commit_idx,
            last_applied,
            leader_state,
            role,
        }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // state a Raft server must maintain.
    state: State,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    raft_server_state: RaftServerState,
    // Don't know what is this yet :(
    apply_ch: UnboundedSender<ApplyMsg>,
    // the time when this server received the last heartbeat from current leader
    last_heartbeat: std::time::Instant,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: State::default(),
            raft_server_state: RaftServerState::default(),
            apply_ch,
            last_heartbeat: std::time::Instant::now(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        //crate::your_code_here((rf, apply_ch))
        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        // this channel service the connection between this main thread and the rpc
        // thread of peer. Note that: the rpc handler may have anotther channel
        // to communicate with the other end on rpc socket
        let (tx, rx) = channel();
        if self.raft_server_state.role != Role::Candidate {
            let _ = tx.send(Err(Error::Rpc(labrpc::Error::Other(
                "not a candidate".to_string(),
            ))));
            return rx;
        }

        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel();
        if self.raft_server_state.role != Role::Leader {
            let _ = tx.send(Err(Error::Rpc(labrpc::Error::Other(
                "not a leader".to_string(),
            ))));
            return rx;
        }

        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let node = Self {
            raft: Arc::new(Mutex::new(raft)),
        };
        let node_clone = node.clone();
        // spawn a election timer thread for this node
        std::thread::spawn(move || {
            node_clone.run_election_timer();
        });

        let hb_clone = node.clone();
        // spawn a election timer thread for this node
        std::thread::spawn(move || {
            hb_clone.heartbeat();
        });

        node
    }

    fn heartbeat(&self) {
        loop {
            std::thread::sleep(HEARTBEAT_INTERVAL);
            let (term, id, commit_id) = {
                let raft = self.raft.lock().unwrap();

                if raft.raft_server_state.role != Role::Leader {
                    continue;
                }
                (raft.state.term, raft.me, raft.raft_server_state.commit_idx)
            };

            // Create receiver and release lock before sending RPCs
            let mut receivers = vec![];
            {
                let raft = self.raft.lock().unwrap();
                for i in 0..raft.peers.len() {
                    if i == raft.me {
                        continue;
                    }
                    let args = AppendEntriesArgs {
                        term: raft.state.term,
                        leader_id: raft.me as u32,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![],
                        leader_commit: raft.raft_server_state.commit_idx,
                    };
                    receivers.push(raft.send_append_entries(i, args));
                }
            }

            for rx in receivers {
                if let Ok(Ok(reply)) = rx.recv_timeout(RECEIVE_TIMEOUT) {
                    if !reply.success && reply.term > term {
                        // Higher term seen, step down
                        let mut raft = self.raft.lock().unwrap();
                        if reply.term > raft.state.term {
                            raft.state.term = reply.term;
                            raft.raft_server_state.role = Role::Follower;
                            raft.state.is_leader = false;
                        }
                        break;
                    }
                }
            }
        }
    }
    fn run_election_timer(&self) {
        let mut rng = rand::thread_rng();
        loop {
            let timeout = Duration::from_millis(rng.gen_range(
                ELECTION_TIMEOUT_MIN.as_millis() as u64,
                ELECTION_TIMEOUT_MAX.as_millis() as u64,
            ));
            std::thread::sleep(timeout);

            // Collect info needed for election, then DROP the lock
            let election_info = {
                let mut raft = self.raft.lock().unwrap();
                if raft.raft_server_state.role == Role::Leader
                    || raft.last_heartbeat.elapsed() < timeout
                {
                    continue;
                }

                // Start election
                raft.raft_server_state.role = Role::Candidate;
                raft.state.term += 1;
                raft.raft_server_state.voted_for = None;
                let my_id = raft.me;
                raft.raft_server_state.voted_for = Some(my_id);

                let term = raft.state.term;
                let me = raft.me;
                let last_log_index = raft.raft_server_state.logs.len() as u64;
                let last_log_term = raft.raft_server_state.logs.last().map(|l| l.1).unwrap_or(0);
                let peer_count = raft.peers.len();

                (term, me, last_log_index, last_log_term, peer_count)
            }; // Lock released here!

            let (term, me, last_log_index, last_log_term, peer_count) = election_info;
            let mut granted_votes = 1; // Count self-vote!

            // Send RPCs WITHOUT holding the lock
            let mut receivers = vec![];
            {
                let raft = self.raft.lock().unwrap();
                for i in 0..peer_count {
                    if i == me {
                        continue;
                    }
                    let args = RequestVoteArgs {
                        term,
                        candidate_id: me as u32,
                        last_log_index,
                        last_log_term,
                    };
                    receivers.push(raft.send_request_vote(i, args));
                }
            } // Lock released!

            // Collect results WITHOUT holding the lock
            for rx in receivers {
                if let Ok(Ok(reply)) = rx.recv_timeout(RECEIVE_TIMEOUT) {
                    if reply.vote_granted {
                        granted_votes += 1;
                    } else if reply.term > term {
                        // Higher term seen, step down
                        // lock is only used if we see a higher term, so it won't cause much
                        // contention
                        let mut raft = self.raft.lock().unwrap();
                        if reply.term > raft.state.term {
                            raft.state.term = reply.term;
                            raft.raft_server_state.role = Role::Follower;
                            raft.state.is_leader = false;
                        }
                        break;
                    }
                }
            }

            // Check if we won
            if granted_votes > peer_count / 2 {
                let mut raft = self.raft.lock().unwrap();
                // Verify we're still candidate in the same term
                if raft.raft_server_state.role == Role::Candidate && raft.state.term == term {
                    raft.raft_server_state.role = Role::Leader;
                    raft.state.is_leader = true;
                    debug!("Node {} becomes leader in term {}", me, term);
                }
            }
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.lock().unwrap().state.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.lock().unwrap().state.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

// This is the receiver side of the RPC handler.
#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let mut raft = self.raft.lock().unwrap();
        match raft.raft_server_state.role {
            Role::Leader if args.term > raft.state.term() => {
                // step down to follower if receive a RequestVote RPC with higher term
                raft.raft_server_state.role = Role::Follower;
                raft.state.term = args.term;
                raft.state.is_leader = false;
                // vote for this candidate
                raft.raft_server_state.voted_for = Some(args.candidate_id as usize);
                raft.last_heartbeat = std::time::Instant::now();
                Ok(RequestVoteReply {
                    term: raft.state.term,
                    vote_granted: true,
                })
            }
            Role::Leader => {
                // term is not higher than us
                return Ok(RequestVoteReply {
                    term: raft.state.term,
                    vote_granted: false,
                });
            }
            Role::Candidate | Role::Follower => {
                if args.term > raft.state.term() {
                    // update term if receive a RequestVote RPC with higher term
                    raft.state.term = args.term;
                    // step down to follower if receive a RequestVote RPC with higher term
                    raft.raft_server_state.role = Role::Follower;
                    // clear current vote
                    raft.raft_server_state.voted_for = None;
                } else {
                    raft.last_heartbeat = std::time::Instant::now();
                    // term is not higher than us
                    return Ok(RequestVoteReply {
                        term: raft.state.term,
                        vote_granted: false,
                    });
                }

                match raft.raft_server_state.voted_for {
                    Some(voted_id) => {
                        if voted_id == args.candidate_id as usize {
                            // already voted for this candidate in this term
                            raft.last_heartbeat = std::time::Instant::now();
                            Ok(RequestVoteReply {
                                term: raft.state.term,
                                vote_granted: true,
                            })
                        } else {
                            // already voted for another candidate in this term
                            Ok(RequestVoteReply {
                                term: raft.state.term,
                                vote_granted: false,
                            })
                        }
                    }
                    None => {
                        raft.raft_server_state.voted_for = Some(args.candidate_id as usize);
                        raft.last_heartbeat = std::time::Instant::now();
                        Ok(RequestVoteReply {
                            term: raft.state.term,
                            vote_granted: true,
                        })
                    }
                }
            }
        }
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        if args.term < raft.state.term() {
            // reply false if term < currentTerm (§5.1)
            Ok(AppendEntriesReply {
                term: raft.state.term,
                success: false,
            })
        } else {
            // reset election timer
            raft.last_heartbeat = std::time::Instant::now();
            if args.term > raft.state.term() {
                // update term if receive a AppendEntries RPC with higher term
                raft.state.term = args.term;
                // step down to follower if receive a AppendEntries RPC with higher term
                raft.raft_server_state.role = Role::Follower;
                raft.state.is_leader = false;
                raft.raft_server_state.voted_for = None;
            }
            Ok(AppendEntriesReply {
                term: raft.state.term,
                success: false,
            })
        }
    }
}
