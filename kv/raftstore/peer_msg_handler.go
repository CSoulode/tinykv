package raftstore

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) MatchProposal(term, index uint64) (*proposal, *raft_cmdpb.RaftCmdResponse) {
	for len(d.proposals) > 0 {
		proposal := d.proposals[0]
		if term < proposal.term {
			return nil, nil
		}
		if term > proposal.term {
			NotifyStaleReq(term, proposal.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		if index < proposal.index {
			return nil, nil
		}
		if index > proposal.index {
			NotifyStaleReq(term, proposal.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		return proposal, &raft_cmdpb.RaftCmdResponse{Header: newCmdResp().Header}
	}
	return nil, nil
}

func (d *peerMsgHandler) HandleProposal(proposal *proposal, resp *raft_cmdpb.RaftCmdResponse) {
	if proposal != nil {
		if resp != nil {
			proposal.cb.Done(resp)
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) WriteEnt(kvWb *engine_util.WriteBatch) {
	//d.peerStorage.applyState.AppliedIndex = index
	kvWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	kvWb.WriteToDB(d.peerStorage.Engines.Kv)
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) HandleAdminReq(kvWb *engine_util.WriteBatch, adminReq *raft_cmdpb.AdminRequest, cc *eraftpb.ConfChange, req *raft_cmdpb.RaftCmdRequest, proposal *proposal, resp *raft_cmdpb.RaftCmdResponse) uint64 {
	if resp != nil {
		resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: adminReq.CmdType}
	}
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		region := d.Region()
		index := -1
		for i, peer := range region.Peers {
			if peer.Id == adminReq.ChangePeer.Peer.Id && peer.StoreId == adminReq.ChangePeer.Peer.StoreId {
				index = i
				break
			}
		}
		if adminReq.ChangePeer.ChangeType == eraftpb.ConfChangeType_AddNode {
			if index == -1 {
				region.Peers = append(region.Peers, adminReq.ChangePeer.Peer)
				d.insertPeerCache(adminReq.ChangePeer.Peer)
			}
		} else {
			if index != -1 {
				if d.PeerId() == adminReq.ChangePeer.Peer.Id && d.storeID() == adminReq.ChangePeer.Peer.StoreId {
					d.destroyPeer()
					return math.MaxInt64
				}
				region.Peers[index] = region.Peers[len(region.Peers)-1]
				region.Peers = region.Peers[:len(region.Peers)-1]
				d.removePeerCache(adminReq.ChangePeer.Peer.Id)
			}
		}
		region.RegionEpoch.ConfVer++
		meta.WriteRegionState(kvWb, region, rspb.PeerState_Normal)
		d.WriteEnt(kvWb)
		d.RaftGroup.ApplyConfChange(*cc)
		d.notifyHeartbeatScheduler(region, d.peer)
		if resp != nil {
			clonedRegion := new(metapb.Region)
			if err := util.CloneMsg(d.Region(), clonedRegion); err != nil {
				log.Errorf("%s clone region failed: %v", d.Tag, err)
			}
			resp.AdminResponse.ChangePeer = &raft_cmdpb.ChangePeerResponse{Region: clonedRegion}
		}

	case raft_cmdpb.AdminCmdType_CompactLog:
		var compactToSchedule uint64 = 0
		if adminReq.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
			d.peerStorage.applyState.TruncatedState.Index = adminReq.CompactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = adminReq.CompactLog.CompactTerm
			compactToSchedule = adminReq.CompactLog.CompactIndex
		}
		d.WriteEnt(kvWb)
		if compactToSchedule != 0 {
			d.ScheduleCompactLog(compactToSchedule)
		}
		if resp != nil {
			if compactToSchedule == 0 {
				proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				return math.MaxInt64 - 1
			}
			resp.AdminResponse.CompactLog = &raft_cmdpb.CompactLogResponse{}
		}

	case raft_cmdpb.AdminCmdType_Split:
		region := d.Region()
		spilt := adminReq.Split
		if err := util.CheckRegionEpoch(req, region, true); err != nil {
			if proposal != nil {
				proposal.cb.Done(ErrResp(err))
			}
			return math.MaxInt64 - 1
		}
		if len(spilt.NewPeerIds) != len(region.Peers) {
			if proposal != nil {
				proposal.cb.Done(ErrResp(&util.ErrEpochNotMatch{Message: "Error peer nums", Regions: []*metapb.Region{region}}))
			}
			return math.MaxInt64 - 1
		}

		leftRegion := new(metapb.Region)
		rightRegion := new(metapb.Region)

		if err := util.CloneMsg(region, leftRegion); err != nil {
			log.Errorf("CloneRegion error %v", err)
		}
		if err := util.CloneMsg(region, rightRegion); err != nil {
			log.Errorf("CloneRegion error %v", err)
		}

		leftRegion.RegionEpoch.Version++
		rightRegion.RegionEpoch.Version++

		leftRegion.EndKey = spilt.SplitKey
		rightRegion.Id = spilt.NewRegionId
		rightRegion.StartKey = spilt.SplitKey
		rightRegion.Peers = make([]*metapb.Peer, len(region.Peers))
		for i, peerID := range spilt.NewPeerIds {
			rightRegion.Peers[i] = &metapb.Peer{
				StoreId: region.Peers[i].StoreId,
				Id:      peerID,
			}
		}

		d.peerStorage.SetRegion(leftRegion)
		meta.WriteRegionState(kvWb, leftRegion, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWb, rightRegion, rspb.PeerState_Normal)
		d.WriteEnt(kvWb)

		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.peerStorage.Engines, rightRegion)
		if err != nil {
			log.Errorf("Create newPeer %v error %v", rightRegion, err)
		}

		d.ctx.router.register(newPeer)
		d.ctx.router.send(rightRegion.GetId(), message.Msg{Type: message.MsgTypeStart})

		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regionRanges.Delete(&regionItem{region: region})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})
		storeMeta.setRegion(leftRegion, d.peer)
		storeMeta.setRegion(rightRegion, newPeer)
		storeMeta.Unlock()

		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		d.notifyHeartbeatScheduler(leftRegion, d.peer)
		d.notifyHeartbeatScheduler(rightRegion, newPeer)

		if resp != nil {
			clonedLeftRegion := new(metapb.Region)
			clonedRightRegion := new(metapb.Region)
			if err := util.CloneMsg(leftRegion, clonedLeftRegion); err != nil {
				log.Errorf("%s clone region failed: %v", d.Tag, err)
			}
			if err := util.CloneMsg(rightRegion, clonedRightRegion); err != nil {
				log.Errorf("%s clone region failed: %v", d.Tag, err)
			}
			resp.AdminResponse.Split = &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{clonedLeftRegion, clonedRightRegion}}
		}
	}

	return 0
}

func (d *peerMsgHandler) HandleRaftReq(kvWb *engine_util.WriteBatch, raftReqs []*raft_cmdpb.Request, proposal *proposal, resp *raft_cmdpb.RaftCmdResponse) {
	if resp != nil {
		for _, req := range raftReqs {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
				val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: val},
				})
			case raft_cmdpb.CmdType_Put:
				kvWb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
				d.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value))
			case raft_cmdpb.CmdType_Delete:
				kvWb.DeleteCF(req.Delete.Cf, req.Delete.Key)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
				d.SizeDiffHint -= uint64(len(req.Delete.Key))
			case raft_cmdpb.CmdType_Snap:
				clonedRegion := new(metapb.Region)
				if err := util.CloneMsg(d.Region(), clonedRegion); err != nil {
					log.Errorf("%s clone region failed: %v", d.Tag, err)
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: clonedRegion},
				})
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			case raft_cmdpb.CmdType_Invalid:
			}
		}
	} else {
		for _, req := range raftReqs {
			switch req.CmdType {
			case raft_cmdpb.CmdType_Get:
			case raft_cmdpb.CmdType_Put:
				kvWb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			case raft_cmdpb.CmdType_Delete:
				kvWb.DeleteCF(req.Delete.Cf, req.Delete.Key)
			case raft_cmdpb.CmdType_Snap:
			case raft_cmdpb.CmdType_Invalid:
			}
		}
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}

	rd := d.RaftGroup.Ready()

	applyRes, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		log.Fatalf("%s SaveReadyState failed: %v", d.Tag, err)
		return
	}
	if applyRes != nil {
		meta := d.ctx.storeMeta
		meta.Lock()
		if len(applyRes.PrevRegion.GetPeers()) > 0 {
			meta.regionRanges.Delete(&regionItem{region: applyRes.PrevRegion})
		}
		meta.regionRanges.ReplaceOrInsert(&regionItem{region: applyRes.Region})
		meta.regions[d.regionId] = applyRes.Region
		meta.Unlock()
	}

	if len(rd.Messages) > 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}

	for _, ent := range rd.CommittedEntries {
		kvWb := &engine_util.WriteBatch{}
		d.peerStorage.applyState.AppliedIndex = ent.Index

		proposal, resp := d.MatchProposal(ent.Term, ent.Index)

		if len(ent.Data) == 0 {
			d.WriteEnt(kvWb)
			if proposal != nil {
				d.HandleProposal(proposal, resp)
			}
			continue
		}

		var request raft_cmdpb.RaftCmdRequest
		var cc eraftpb.ConfChange
		if ent.EntryType == eraftpb.EntryType_EntryConfChange {
			if err := cc.Unmarshal(ent.Data); err != nil {
				log.Fatalf("%s unmarshal committed entry at index %d failed: %v", d.Tag, ent.Index, err)
				return
			}
			if err := request.Unmarshal(cc.Context); err != nil {
				log.Fatalf("%s unmarshal committed entry at index %d failed: %v", d.Tag, ent.Index, err)
				return
			}
		} else {
			if err := request.Unmarshal(ent.Data); err != nil {
				log.Fatalf("%s unmarshal committed entry at index %d failed: %v", d.Tag, ent.Index, err)
				return
			}
		}

		if err := util.CheckRegionEpoch(&request, d.Region(), true); err != nil {
			if proposal != nil {
				proposal.cb.Done(ErrResp(err))
				d.HandleProposal(proposal, nil)
			}
			d.WriteEnt(kvWb)
			continue
		}

		if adminReq := request.AdminRequest; adminReq != nil {
			flag := d.HandleAdminReq(kvWb, adminReq, &cc, &request, proposal, resp)
			if flag == math.MaxInt64 {
				return
			}
			//d.WriteEnt(kvWb, ent.Index)
			if flag == math.MaxInt64-1 {
				resp = nil
			}
		}
		if raftReq := request.Requests; raftReq != nil {
			d.HandleRaftReq(kvWb, raftReq, proposal, resp)
			d.WriteEnt(kvWb)
		}
		d.HandleProposal(proposal, resp)
	}

	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) spiltKeyInRegion(splitKey []byte) bool {
	if bytes.Compare(splitKey, d.Region().StartKey) < 0 || engine_util.ExceedEndKey(splitKey, d.Region().EndKey) {
		return false
	}
	return true
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if !d.IsLeader() {
		cb.Done(ErrResp(&util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.peerCache[d.LeaderId()],
		}))
		return
	}

	region := d.Region()

	for _, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			if err := util.CheckKeyInRegion(req.Get.Key, region); err != nil {
				cb.Done(ErrResp(err))
				return
			}
		case raft_cmdpb.CmdType_Put:
			if err := util.CheckKeyInRegion(req.Put.Key, region); err != nil {
				cb.Done(ErrResp(err))
				return
			}
		case raft_cmdpb.CmdType_Delete:
			if err := util.CheckKeyInRegion(req.Delete.Key, region); err != nil {
				cb.Done(ErrResp(err))
				return
			}
		}
	}

	data, _ := msg.Marshal()
	prop := proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}

	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_ChangePeer:
			if msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode {
				if len(region.Peers) == 2 && msg.AdminRequest.ChangePeer.Peer.StoreId == d.storeID() && msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() {
					var peerId uint64 = 0
					for _, peer := range region.Peers {
						if peer.Id != d.PeerId() {
							peerId = peer.Id
						}
					}
					d.RaftGroup.TransferLeader(peerId)
					cb.Done(ErrResp(&util.ErrNotLeader{
						RegionId: d.regionId,
						Leader:   d.peerCache[peerId],
					}))
					return
				}
			}

			err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    data,
			})

		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: newCmdResp().Header,
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			})
			return

		case raft_cmdpb.AdminCmdType_Split:
			split := msg.AdminRequest.Split
			if err := util.CheckKeyInRegion(split.SplitKey, region); err != nil {
				cb.Done(ErrResp(&util.ErrKeyNotInRegion{
					Key:    split.SplitKey,
					Region: region,
				}))
				return
			} else {
				err = d.RaftGroup.Propose(data)
			}

		case raft_cmdpb.AdminCmdType_CompactLog:
			if msg.AdminRequest.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index && msg.AdminRequest.CompactLog.CompactIndex < prop.index {
				err = d.RaftGroup.Propose(data)
			} else {
				cb.Done(ErrResp(&util.ErrStaleCommand{}))
				return
			}

		case raft_cmdpb.AdminCmdType_InvalidAdmin:
		}
	} else {
		err = d.RaftGroup.Propose(data)
	}
	if err != nil {
		cb.Done(ErrResp(err))
	} else {
		d.proposals = append(d.proposals, &prop)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
