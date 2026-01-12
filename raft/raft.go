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
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
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
	electionTimeout int
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

	randomizedElectionTimeout int
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)

	hardState, ConfState, _ := c.Storage.InitialState()

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: c.HeartbeatTick,
		electionElapsed:  c.ElectionTick,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}

	r.RaftLog.committed = hardState.Commit
	lastIndex := r.RaftLog.LastIndex()
	if r.RaftLog.committed > lastIndex {
		r.RaftLog.committed = lastIndex
		log.Fatal("newRaft: r.RaftLog.committed > lastIndex")
	}

	r.RaftLog.applied = max(r.RaftLog.applied, c.Applied)
	if r.RaftLog.committed < r.RaftLog.applied {
		r.RaftLog.committed = r.RaftLog.applied
		log.Fatal("newRaft: r.RaftLog.committed < r.RaftLog.applied")
	}

	if len(c.peers) == 0 {
		c.peers = ConfState.Nodes
	}
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: r.RaftLog.entries[0].Index,
			Next:  r.RaftLog.entries[0].Index + 1,
		}
	}

	r.resetRandomizedElectionTimeout()

	return r
}

func (r *Raft) send(m pb.Message) bool {
	r.msgs = append(r.msgs, m)
	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)

	if err == ErrCompacted {
		return r.sendSnapshot(to)
	}
	if err == ErrUnavailable {
		panic("sendAppend Err")
	}

	offset := r.RaftLog.entries[0].Index
	entries := r.RaftLog.entries[prevLogIndex+1-offset:]
	entriess := make([]*pb.Entry, len(entries))
	for i := range entries {
		entriess[i] = &entries[i]
	}
	return r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entriess,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) sendAppendResponse(to uint64) bool {
	return r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) sendRequestVote(to uint64) bool {
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	return r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   lastLogIndex,
		LogTerm: lastLogTerm,
	})
}

func (r *Raft) sendRequestVoteResponse(to uint64) bool {
	return r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) sendSnapshot(to uint64) bool {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			return false
		}
		panic(err)
	}
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return r.send(pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	pr := r.Prs[to]
	commit := min(pr.Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64) bool {
	return r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) sendTimeoutNow(to uint64) bool {
	return r.send(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State != StateLeader {
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	} else {
		if r.leadTransferee != None {
			r.electionElapsed++
			if r.electionElapsed >= r.randomizedElectionTimeout {
				r.electionElapsed = 0
				r.leadTransferee = None
			}
		}

		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.Vote = None
	r.State = StateFollower
	r.Lead = lead
	if term > r.Term {
		r.Term = term
		r.Vote = None
	} else {
		r.Term = term
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.State = StateCandidate
	r.Term = r.Term + 1
	r.Vote = r.id
	r.votes = map[uint64]bool{}
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.leadTransferee = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()

	lastLogIndex := r.RaftLog.LastIndex()
	for _, peer := range r.Prs {
		peer.Match = 0
		peer.Next = lastLogIndex + 1
	}

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.Prs[r.id].Match = lastLogIndex + 1
	r.Prs[r.id].Next = lastLogIndex + 2
	if len(r.Prs) == 1 {
		r.RaftLog.committed = lastLogIndex + 1
	}

	for peer_id := range r.Prs {
		if peer_id != r.id {
			r.sendAppend(peer_id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
	//local message
	case m.Term > r.Term:
		lead := None
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
			})
		}
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.handleHup()
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.handleBeat()
		}
	case pb.MessageType_MsgPropose:
		if r.State == StateLeader {
			if r.leadTransferee != None {
				return ErrProposalDropped
			}
			r.handlePropose(m)
		}
	case pb.MessageType_MsgAppend:
		if r.State != StateLeader {
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgAppendResponse:
		if r.State == StateLeader {
			r.handleAppendResponse(m)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate {
			r.handleRequestVoteResponse(m)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if r.State != StateLeader {
			r.handleHeartbeat(m)
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if r.State == StateLeader {
			r.handleHeartbeatResponse(m)
		}
	case pb.MessageType_MsgTransferLeader:
		if r.State == StateLeader {
			r.handleTransferLeader(m)
		} else if r.Lead != None {
			m.To = r.Lead
			r.send(m)
		}
	case pb.MessageType_MsgTimeoutNow:
		if r.State == StateFollower {
			r.handleTimeoutNow(m)
		}
	}

	return nil
}

func (r *Raft) handleHup() {
	r.becomeCandidate()

	if len(r.Prs) <= 1 {
		r.becomeLeader()
		return
	}

	for peer_id := range r.Prs {
		if peer_id != r.id {
			r.sendRequestVote(peer_id)
		}
	}
}

func (r *Raft) handleBeat() {
	for peer_id := range r.Prs {
		if peer_id != r.id {
			r.sendHeartbeat(peer_id)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, entry := range m.Entries {
		entry.Index = r.RaftLog.LastIndex() + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	lastLogIndex := r.RaftLog.LastIndex()
	r.Prs[r.id].Match = lastLogIndex
	r.Prs[r.id].Next = lastLogIndex + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = lastLogIndex
	}
	for peer_id := range r.Prs {
		if peer_id != r.id {
			r.sendAppend(peer_id)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}

	r.Lead = m.From
	r.electionElapsed = 0

	offset := r.RaftLog.entries[0].Index

	sendReject := func(hintIndex uint64, hintTerm uint64) {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
			Index:   hintIndex,
			LogTerm: hintTerm,
		})
	}

	lastLogIndex := r.RaftLog.LastIndex()
	if m.Index < offset || m.Index > lastLogIndex {
		sendReject(lastLogIndex+1, None)
		return
	}
	if r.RaftLog.entries[m.Index-offset].Term != m.LogTerm {
		term := r.RaftLog.entries[m.Index-offset].Term
		index := m.Index - offset
		for ; index > 0; index-- {
			if r.RaftLog.entries[index].Term != term {
				break
			}
		}
		sendReject(index+1+offset, term)
		return
	}

	if m.Index > lastLogIndex || r.RaftLog.entries[m.Index-offset].Term != m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	i := m.Index + 1
	j := 0
	for ; i <= lastLogIndex && j < len(m.Entries); i, j = i+1, j+1 {
		if r.RaftLog.entries[i-offset].Term != m.Entries[j].Term {
			r.RaftLog.entries = r.RaftLog.entries[:i-offset]
			break
		}
	}

	if lastLogIndex != r.RaftLog.LastIndex() {
		r.RaftLog.stabled = min(r.RaftLog.stabled, i-1)
	}

	for ; j < len(m.Entries); j++ {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   m.Index + uint64(len(m.Entries)),
		Reject:  false,
	})
}

func (r *Raft) Quorum() { //Leader update the committed
	var quorum [7]uint64
	var quorumm []uint64
	n := len(r.Prs)
	if n <= len(quorum) {
		quorumm = quorum[:n]
	} else {
		quorumm = make([]uint64, n)
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	i := 0
	for _, peer := range r.Prs {
		quorumm[i] = peer.Match
		i++
	}
	sort.Slice(quorumm, func(i, j int) bool {
		return quorumm[i] > quorumm[j]
	})
	idx := quorumm[n/2]
	term, _ := r.RaftLog.Term(idx)
	if idx > r.RaftLog.committed && term == r.Term {
		r.RaftLog.committed = idx
		for peer_id := range r.Prs {
			if peer_id != r.id {
				r.sendAppend(peer_id)
			}
		}
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		if m.LogTerm == None {
			r.Prs[m.From].Next = m.Index
		} else {
			offset := r.RaftLog.entries[0].Index
			term := m.LogTerm
			var index uint64 = 0 //prevent underflow
			if m.Index > offset {
				index = m.Index - offset
			}

			for ; index > 0; index-- {
				if r.RaftLog.entries[index-1].Term == term {
					break
				}
			}
			if index <= 0 {
				r.Prs[m.From].Next = m.Index
			} else {
				r.Prs[m.From].Next = index + offset
			}
		}
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1

		if m.Index > r.RaftLog.committed {
			r.Quorum()
		}

		if r.leadTransferee != None && r.Prs[r.leadTransferee].Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(r.leadTransferee)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	reject := true
	if r.Vote == None || r.Vote == m.From {
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			r.Vote = m.From
			reject = false
			r.electionElapsed = 0
		}
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	majority := len(r.Prs)/2 + 1
	if len(r.votes) < majority {
		return
	}
	granted := 0
	rejected := 0
	for _, res := range r.votes {
		if res == true {
			granted++
		} else {
			rejected++
		}
	}

	if granted >= majority {
		r.becomeLeader()
	} else if rejected >= majority {
		r.becomeFollower(r.Term, None)
	}

}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Snapshot == nil || r.RaftLog.pendingSnapshot != nil {
		return
	}
	snapshot := m.Snapshot
	if r.restore(snapshot) {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Index:   r.RaftLog.LastIndex(),
		})
	} else {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Index:   r.RaftLog.committed,
		})
	}
}

func (r *Raft) restore(snapshot *pb.Snapshot) bool {
	sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
	if sindex <= r.RaftLog.committed {
		return false
	}

	term, err := r.RaftLog.Term(sindex)
	if err == nil && term == sterm {
		r.RaftLog.committed = sindex
		return false
	}

	r.RaftLog.committed = sindex
	r.RaftLog.applied = sindex
	r.RaftLog.stabled = sindex

	r.RaftLog.entries = []pb.Entry{{Term: sterm, Index: sindex}}

	if snapshot.Metadata.ConfState != nil {
		prs := make(map[uint64]*Progress)
		for _, peer := range snapshot.Metadata.ConfState.Nodes {
			prs[peer] = &Progress{
				Match: r.RaftLog.entries[0].Index,
				Next:  r.RaftLog.entries[0].Index + 1,
			}
		}
		r.Prs = prs
	}
	r.RaftLog.pendingSnapshot = snapshot
	return true
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}

	r.Lead = m.From
	r.electionElapsed = 0

	lastLogIndex := r.RaftLog.LastIndex()

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, lastLogIndex)
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	_, ok := r.Prs[m.From]
	if !ok || m.From == r.id || (r.leadTransferee != None && r.leadTransferee == m.From) {
		return
	}

	r.leadTransferee = m.From
	r.electionElapsed = 0

	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	if _, ok := r.Prs[m.To]; !ok {
		return
	}
	r.electionElapsed = 0
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	_, ok := r.Prs[id]
	if ok {
		return
	}

	r.Prs[id] = &Progress{
		Match: r.RaftLog.entries[0].Index,
		Next:  r.RaftLog.LastIndex() + 1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	_, ok := r.Prs[id]
	if !ok {
		return
	}

	delete(r.Prs, id)
	if r.id != id && r.State == StateLeader {
		r.Quorum()
	}
}

func (r *Raft) softState() SoftState {
	return SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
