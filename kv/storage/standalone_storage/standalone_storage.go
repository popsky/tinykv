package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

// NewStandAloneStorage Create a new Storage.
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &StandAloneStorage{db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{s.db.NewTransaction(false)}, nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

// When the key doesn't exist, return nil for the value
func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(reader.txn, cf, key)
}
func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}
func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			writeBatch.DeleteCF(m.Cf(), m.Key())
		}
	}
	return writeBatch.WriteToDB(s.db)
}
