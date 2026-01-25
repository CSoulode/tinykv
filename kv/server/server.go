package server

import (
	"bytes"
	"context"
	"sort"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	keys := [][]byte{req.Key}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}

	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}

	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
	}

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	keys := make([][]byte, 0, len(req.Mutations))
	for _, e := range req.Mutations {
		keys = append(keys, e.Key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	if len(keys) > 0 {
		slow := 0
		for fast := 1; fast < len(keys); fast++ {
			if !bytes.Equal(keys[slow], keys[fast]) {
				slow++
				keys[slow] = keys[fast]
			}
		}
		keys = keys[:slow+1]
	}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, e := range req.Mutations {
		write, commitTs, err := txn.MostRecentWrite(e.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTs >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: write.StartTS,
					Key:        e.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(e.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(e.Key)})
		}
	}

	if len(resp.Errors) > 0 {
		return resp, nil
	}

	for _, e := range req.Mutations {
		kind := mvcc.WriteKindFromProto(e.Op)
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		}
		if kind == mvcc.WriteKindPut {
			txn.PutValue(e.Key, e.Value)
		}
		txn.PutLock(e.Key, lock)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
	}

	return resp, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	keys := req.Keys
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	if len(keys) > 0 {
		slow := 0
		for fast := 1; fast < len(keys); fast++ {
			if !bytes.Equal(keys[slow], keys[fast]) {
				slow++
				keys[slow] = keys[fast]
			}
		}
		keys = keys[:slow+1]
	}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	toCommit := make([][]byte, 0, len(keys))

	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}

			if write == nil {
				// missing prewrite -> success no-op
				continue
			}
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "abort"}
				return resp, nil
			}
			// already committed -> idempotent success
			continue
		}

		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "retry"}
			return resp, nil
		}

		toCommit = append(toCommit, key)
	}

	for _, key := range toCommit {
		lock, _ := txn.GetLock(key)
		write := &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind}
		txn.PutWrite(key, req.CommitVersion, write)
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
	}
	return resp, err
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	if req.Limit == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	for i := uint32(0); i < req.Limit; i++ {
		k, v, err := scanner.Next()
		if err != nil {
			if mvccError, ok := err.(*mvcc.KeyError); ok {
				resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
					Error: &mvccError.KeyError,
					Key:   k,
				})
				continue
			}
			return nil, err
		}
		if k == nil && v == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		})
	}

	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	keys := [][]byte{req.PrimaryKey}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitTs
			resp.LockTtl = 0
			resp.Action = kvrpcpb.Action_NoAction
		} else {
			resp.CommitVersion = 0
			resp.LockTtl = 0
			resp.Action = kvrpcpb.Action_NoAction
		}
		return resp, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil || lock.Ts != req.LockTs {
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.CommitVersion = 0
		resp.LockTtl = 0
		resp.Action = kvrpcpb.Action_LockNotExistRollback

		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
			}
		}
		return resp, nil
	}

	elapsed := mvcc.PhysicalTime(req.CurrentTs) - mvcc.PhysicalTime(req.LockTs)

	if elapsed >= lock.Ttl {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})

		resp.CommitVersion = 0
		resp.LockTtl = 0
		resp.Action = kvrpcpb.Action_TTLExpireRollback

		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
			}
		}
		return resp, nil
	}

	resp.CommitVersion = 0
	resp.LockTtl = lock.Ttl - elapsed
	resp.Action = kvrpcpb.Action_NoAction

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	keys := req.Keys
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "already committed"}
				return resp, nil
			} else {
				continue
			}
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})

		if lock != nil {
			if lock.Ts != req.StartVersion {
				continue
			}
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
	}

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}

	if req.CommitVersion == 0 {
		for _, lock := range locks {
			key := lock.Key
			txn.DeleteLock(key)
			txn.DeleteValue(key)
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
		}
	} else if req.CommitVersion > 0 {
		for _, lock := range locks {
			key := lock.Key
			txn.DeleteLock(key)
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    lock.Lock.Kind,
			})
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
	}

	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
