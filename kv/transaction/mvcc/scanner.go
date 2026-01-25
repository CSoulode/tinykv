package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn       *MvccTxn
	writeIter engine_util.DBIterator
	lockIter  engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn:       txn,
		writeIter: txn.Reader.IterCF(engine_util.CfWrite),
		lockIter:  txn.Reader.IterCF(engine_util.CfLock),
	}

	scanner.writeIter.Seek(EncodeKey(startKey, txn.StartTS))
	scanner.lockIter.Seek(startKey)

	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.writeIter != nil {
		scan.writeIter.Close()
	}
	if scan.lockIter != nil {
		scan.lockIter.Close()
	}
}

func (scan *Scanner) advanceWriteIter(userKey []byte) {
	for scan.writeIter != nil && scan.writeIter.Valid() {
		k := DecodeUserKey(scan.writeIter.Item().Key())
		if !bytes.Equal(k, userKey) {
			break
		}
		scan.writeIter.Next()
	}
}

func (scan *Scanner) readVisibleValueFromWriteCF(userKey []byte) ([]byte, bool, error) {
	for scan.writeIter != nil && scan.writeIter.Valid() {
		item := scan.writeIter.Item()
		k := DecodeUserKey(item.Key())
		if !bytes.Equal(k, userKey) {
			break
		}

		commitTs := decodeTimestamp(item.Key())
		if commitTs > scan.txn.StartTS {
			scan.writeIter.Next()
			continue
		}

		v, err := item.Value()
		if err != nil {
			return nil, false, err
		}
		write, err := ParseWrite(v)
		if err != nil {
			return nil, false, err
		}
		if write == nil {
			scan.writeIter.Next()
			continue
		}

		switch write.Kind {
		case WriteKindPut:
			val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
			if err != nil {
				return nil, false, err
			}
			scan.advanceWriteIter(userKey)
			return val, true, nil

		case WriteKindDelete:
			scan.advanceWriteIter(userKey)
			return nil, false, nil

		case WriteKindRollback:
			scan.writeIter.Next()
			continue

		default:
			scan.writeIter.Next()
			continue
		}
	}

	scan.advanceWriteIter(userKey)
	return nil, false, nil
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for {
		writeIterVaild := scan.writeIter.Valid()
		lockIterVaild := scan.lockIter.Valid()
		if !writeIterVaild && !lockIterVaild {
			return nil, nil, nil
		}

		var (
			userKey   []byte
			fromWrite bool
			fromLock  bool
		)

		var writeKey []byte
		if writeIterVaild {
			writeKey = DecodeUserKey(scan.writeIter.Item().Key())
		}
		var lockKey []byte
		if lockIterVaild {
			lockKey = scan.lockIter.Item().Key()
		}

		switch {
		case writeIterVaild && lockIterVaild:
			cmp := bytes.Compare(writeKey, lockKey)
			if cmp < 0 {
				userKey = writeKey
				fromWrite = true
			} else if cmp > 0 {
				userKey = lockKey
				fromLock = true
			} else {
				userKey = writeKey
				fromWrite = true
				fromLock = true
			}
		case writeIterVaild:
			userKey = writeKey
			fromWrite = true
		default:
			userKey = lockKey
			fromLock = true
		}

		retKey := append([]byte{}, userKey...)

		if fromLock {
			item := scan.lockIter.Item()
			v, err := item.Value()
			if err != nil {
				return nil, nil, err
			}
			lock, err := ParseLock(v)
			if err != nil {
				return nil, nil, err
			}

			scan.lockIter.Next()

			if lock != nil && lock.Ts <= scan.txn.StartTS {
				if fromWrite {
					scan.advanceWriteIter(retKey)
				}
				return retKey, nil, &KeyError{KeyError: kvrpcpb.KeyError{Locked: lock.Info(retKey)}}
			}
		}

		if fromWrite {
			val, ok, err := scan.readVisibleValueFromWriteCF(retKey)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				return retKey, val, nil
			}
		}
	}
}
