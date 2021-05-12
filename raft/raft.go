// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout  int
	electionInterval int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hs, _, _ := c.Storage.InitialState()

	r := Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress, len(c.peers)),
		votes:            make(map[uint64]bool, len(c.peers)),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	for _, id := range c.peers {
		r.Prs[id] = &Progress{}
		r.votes[id] = false
	}
	r.becomeFollower(r.Term, None)
	// Your Code Here (2A).
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	next := r.Prs[to].Next
	ents, err := r.RaftLog.getEntries(next, r.RaftLog.LastIndex())
	if err != nil {
		return false
	}
	var pre_idx, pre_term uint64
	var entries []*pb.Entry
	if len(ents) > 0 {
		entries = make([]*pb.Entry, 0, len(ents))
		for _, e := range ents {
			ent := e
			entries = append(entries, &ent)
		}
		pre_idx = entries[0].Index - 1
		pre_term, _ = r.RaftLog.Term(pre_idx)
	} else {
		pre_idx = r.Prs[to].Next - 1
		pre_term, _ = r.RaftLog.Term(pre_idx)
	}

	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, LogTerm: pre_term, Index: pre_idx, Entries: entries, Commit: r.RaftLog.committed})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	pre_idx := r.Prs[to].Next - 1
	pre_term, err := r.RaftLog.Term(pre_idx)
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: to, From: r.id, Term: r.Term, LogTerm: pre_term, Index: pre_idx, Commit: r.RaftLog.committed})
}

func (r *Raft) sendRequestVote(to uint64) {
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, To: to, From: r.id, Term: r.Term, LogTerm: term, Index: r.RaftLog.LastIndex()})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionInterval {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.electionElapsed = 0
	r.electionInterval = r.electionTimeout + rand.Intn(r.electionTimeout)
	if r.Term < term { // A leader with same term cancel the election, don't update Vote for.
		r.Vote = None
	}
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.electionElapsed = 0
	r.electionInterval = r.electionTimeout + rand.Intn(r.electionTimeout)
	for id := range r.votes {
		if id == r.id {
			r.votes[id] = true
		} else {
			r.votes[id] = false
		}
	}
}

func (r *Raft) poll() {
	majority := len(r.votes)/2 + 1
	count := 0
	for _, v := range r.votes {
		if v {
			count++
			if count >= majority {
				r.becomeLeader()
			}
		}
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateCandidate {
		panic("invalid transfer to leader")
	}
	r.State = StateLeader
	for _, prs := range r.Prs {
		prs.Match = 0
		prs.Next = r.RaftLog.LastIndex() + 1
	}
	// r.RaftLog.Append([]pb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1, Data: nil}})

	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: nil}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.becomeCandidate()
			for id := range r.votes {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
			r.poll()
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			for to := range r.Prs {
				if to != r.id {
					r.sendHeartbeat(to)
				}
			}
		}
	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			panic("expect being leader")
		}
		idx := r.RaftLog.LastIndex() + 1
		var ents []pb.Entry = make([]pb.Entry, 0, len(m.Entries))
		for _, e := range m.Entries {
			e.Term = r.Term
			e.Index = idx
			idx += 1
			ents = append(ents, *e)
		}
		r.RaftLog.Append(ents)
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		for to := range r.Prs {
			if to != r.id {
				r.sendAppend(to)
			}
		}
		r.maybeCommit()
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term || (r.State == StateCandidate && m.Term == r.Term) {
			r.becomeFollower(m.Term, m.From)
		}
		term, err := r.RaftLog.Term(m.Index)
		rep := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  r.Term > m.Term || err != nil || term != m.LogTerm,
		}
		if !rep.Reject {
			if len(m.Entries) > 0 {
				entries := make([]pb.Entry, 0, len(m.Entries))
				for _, e := range m.Entries {
					ee := e
					entries = append(entries, *ee)
				}
				r.RaftLog.Append(entries)
				rep.Index = entries[len(entries)-1].Index
				if m.Commit > r.RaftLog.committed {
					r.RaftLog.committed = min(m.Commit, entries[len(entries)-1].Index)
				}
			} else {
				if m.Commit > r.RaftLog.committed && m.Commit <= m.Index {
					r.RaftLog.committed = m.Commit
				}
			}
		}
		if m.Term == r.Term {
			r.electionElapsed = 0
		}
		rep.Commit = r.RaftLog.committed
		r.msgs = append(r.msgs, rep)
	case pb.MessageType_MsgAppendResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		if r.State == StateLeader && m.Term == r.Term { // ensure message come from current term
			if m.Reject {
				r.Prs[m.From].Next -= 1 // TODO: this may optimize later.
			} else if m.Index > 0 {
				r.Prs[m.From].Match = m.Index
				r.Prs[m.From].Next = m.Index + 1
				r.maybeCommit()
			}
			if r.RaftLog.committed > m.Commit || m.Reject {
				r.sendAppend(m.From)
			}
		}
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		idx := r.RaftLog.LastIndex()
		term, _ := r.RaftLog.Term(idx)
		ok := r.Term <= m.Term && (r.Vote == None || r.Vote == m.From) && (m.LogTerm > term || (term == m.LogTerm && idx <= m.Index))
		rep := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  !ok,
		}
		if ok {
			r.Vote = m.From
			r.electionElapsed = 0
		}
		r.msgs = append(r.msgs, rep)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		if r.State == StateCandidate && m.Term == r.Term && !m.Reject {
			r.votes[m.From] = true
			r.poll()
		}
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term || (r.State == StateCandidate && m.Term == r.Term) {
			r.becomeFollower(m.Term, m.From)
		}
		rep := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
		}
		if m.Term == r.Term {
			r.electionElapsed = 0
		}
		term, err := r.RaftLog.Term(m.Index)
		if err != nil && term == m.Term && m.Commit > r.RaftLog.committed && m.Commit <= m.Index {
			r.RaftLog.committed = m.Commit
		}
		r.msgs = append(r.msgs, rep)
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		if r.State == StateLeader && m.Commit < r.RaftLog.committed {
			r.sendHeartbeat(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}

	switch r.State {
	case StateFollower:
	case StateCandidate:
	case StateLeader:
	}
	return nil
}

func (r *Raft) maybeCommit() bool {
	majority := len(r.Prs)/2 + 1
	for i := r.RaftLog.LastIndex(); i > r.RaftLog.committed; i-- {
		if term, err := r.RaftLog.Term(i); err == nil && term == r.Term {
			count := 0
			for _, p := range r.Prs {
				if p.Match >= i {
					count++
				}
			}
			if count >= majority {
				r.RaftLog.committed = i
				return true
			}
		}
	}
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	rep := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  r.Term > m.Term || !r.logMatch(m),
	}
	r.msgs = append(r.msgs, rep)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	rep := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  r.Term > m.Term,
	}
	r.msgs = append(r.msgs, rep)
}

func (r *Raft) logMatch(m pb.Message) bool {
	if r.RaftLog.LastIndex() < m.Index {
		return false
	}
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		return false
	}
	return true
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
