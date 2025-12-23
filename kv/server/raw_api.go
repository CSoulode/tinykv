package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, nil
	}
	defer reader.Close()
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if val == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: val}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{Data: put}
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})

	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, nil
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{Data: del}
	err := server.storage.Write(req.Context, []storage.Modify{modify})

	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, nil
	}
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	var num uint32 = 0
	kvPairs := []*kvrpcpb.KvPair{}
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if num >= req.GetLimit() {
			break
		}
		num++
		item := iter.Item()
		key := item.Key()
		val, _ := item.Value()
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: key, Value: val})
	}
	iter.Close()

	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}
