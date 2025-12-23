package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	dbPath string
	Raft   bool
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{dbPath: conf.DBPath, Raft: conf.Raft}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.db = engine_util.CreateDB(s.dbPath, s.Raft)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCFFromTxn(r.txn, cf, key)
	return val, nil
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.txn)
	return iter
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			err := txn.Set(engine_util.KeyWithCF(b.Cf(), b.Key()), b.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := txn.Delete(engine_util.KeyWithCF(b.Cf(), b.Key()))
			if err != nil {
				return err
			}
		}
	}
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}
