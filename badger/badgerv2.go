package badger

import (
	"bytes"

	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
	"github.com/smallstep/nosql/database"
)

// DBv2 is a wrapper over *badger/v2.DB,
type DBv2 struct {
	db *badger.DB
}

// Open opens or creates a BoltDB database in the given path.
func (db *DBv2) Open(dir string, opt ...database.Option) (err error) {
	opts := &database.Options{}
	for _, o := range opt {
		if err := o(opts); err != nil {
			return err
		}
	}

	bo := badger.DefaultOptions(dir)
	if opts.ValueDir != "" {
		bo.ValueDir = opts.ValueDir
	}

	db.db, err = badger.Open(bo)
	return errors.Wrap(err, "error opening Badger database")
}

// Close closes the DB database.
func (db *DBv2) Close() error {
	return errors.Wrap(db.db.Close(), "error closing Badger database")
}

// CreateTable creates a token element with the 'bucket' prefix so that such
// that their appears to be a table.
func (db *DBv2) CreateTable(bucket []byte) error {
	bk, err := badgerEncode(bucket)
	if err != nil {
		return err
	}
	return db.db.Update(func(txn *badger.Txn) error {
		return errors.Wrapf(txn.Set(bk, []byte{}), "failed to create %s/", bucket)
	})
}

// DeleteTable deletes a root or embedded bucket. Returns an error if the
// bucket cannot be found or if the key represents a non-bucket value.
func (db *DBv2) DeleteTable(bucket []byte) error {
	var tableExists bool
	prefix, err := badgerEncode(bucket)
	if err != nil {
		return err
	}
	deleteKeys := func(keysForDelete [][]byte) error {
		if err := db.db.Update(func(txn *badger.Txn) error {
			for _, key := range keysForDelete {
				tableExists = true
				if err := txn.Delete(key); err != nil {
					return errors.Wrapf(err, "error deleting key %s", key)
				}
			}
			return nil
		}); err != nil {
			return errors.Wrapf(err, "update failed")
		}
		return nil
	}

	collectSize := 1000
	err = db.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysForDelete[keysCollected] = key
			keysCollected++
			if keysCollected == collectSize {
				if err := deleteKeys(keysForDelete); err != nil {
					return err
				}
				keysCollected = 0
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete[:keysCollected]); err != nil {
				return err
			}
		}
		if !tableExists {
			return errors.Wrapf(database.ErrNotFound, "table %s does not exist", bucket)
		}

		return nil
	})
	return err
}

// badgerGetV2 is a helper for the Get method.
func badgerGetV2(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	switch {
	case err == badger.ErrKeyNotFound:
		return nil, errors.Wrapf(database.ErrNotFound, "key %s not found", key)
	case err != nil:
		return nil, errors.Wrapf(err, "failed to get key %s", key)
	default:
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, errors.Wrap(err, "error accessing value returned by database")
		}

		// Make sure to return a copy as val is only valid during the
		// transaction.
		return cloneBytes(val), nil
	}
}

// Get returns the value stored in the given bucked and key.
func (db *DBv2) Get(bucket, key []byte) (ret []byte, err error) {
	bk, err := toBadgerKey(bucket, key)
	if err != nil {
		return nil, errors.Wrapf(err, "error converting %s/%s to badgerKey", bucket, key)
	}
	err = db.db.View(func(txn *badger.Txn) error {
		ret, err = badgerGetV2(txn, bk)
		return err
	})
	return
}

// Set stores the given value on bucket and key.
func (db *DBv2) Set(bucket, key, value []byte) error {
	bk, err := toBadgerKey(bucket, key)
	if err != nil {
		return errors.Wrapf(err, "error converting %s/%s to badgerKey", bucket, key)
	}
	return db.db.Update(func(txn *badger.Txn) error {
		return errors.Wrapf(txn.Set(bk, value), "failed to set %s/%s", bucket, key)
	})
}

// Del deletes the value stored in the given bucked and key.
func (db *DBv2) Del(bucket, key []byte) error {
	bk, err := toBadgerKey(bucket, key)
	if err != nil {
		return errors.Wrapf(err, "error converting %s/%s to badgerKey", bucket, key)
	}
	return db.db.Update(func(txn *badger.Txn) error {
		return errors.Wrapf(txn.Delete(bk), "failed to delete %s/%s", bucket, key)
	})
}

// List returns the full list of entries in a bucket.
func (db *DBv2) List(bucket []byte) ([]*database.Entry, error) {
	var (
		entries     []*database.Entry
		tableExists bool
	)
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix, err := badgerEncode(bucket)
		if err != nil {
			return err
		}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			tableExists = true
			item := it.Item()
			bk := item.KeyCopy(nil)
			if isBadgerTable(bk) {
				continue
			}
			_bucket, key, err := fromBadgerKey(bk)
			if err != nil {
				return errors.Wrapf(err, "error converting from badgerKey %s", bk)
			}
			if !bytes.Equal(_bucket, bucket) {
				return errors.Errorf("bucket names do not match; want %v, but got %v",
					bucket, _bucket)
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return errors.Wrap(err, "error retrieving contents from database value")
			}
			entries = append(entries, &database.Entry{
				Bucket: _bucket,
				Key:    key,
				Value:  cloneBytes(v),
			})
		}
		if !tableExists {
			return errors.Wrapf(database.ErrNotFound, "bucket %s not found", bucket)
		}
		return nil
	})
	return entries, err
}

// CmpAndSwap modifies the value at the given bucket and key (to newValue)
// only if the existing (current) value matches oldValue.
func (db *DBv2) CmpAndSwap(bucket, key, oldValue, newValue []byte) ([]byte, bool, error) {
	bk, err := toBadgerKey(bucket, key)
	if err != nil {
		return nil, false, err
	}

	badgerTxn := db.db.NewTransaction(true)
	defer badgerTxn.Discard()

	val, swapped, err := cmpAndSwapV2(badgerTxn, bk, oldValue, newValue)
	switch {
	case err != nil:
		return nil, false, err
	case swapped:
		if err := badgerTxn.Commit(); err != nil {
			return nil, false, errors.Wrapf(err, "failed to commit badger transaction")
		}
		return val, swapped, nil
	default:
		return val, swapped, err
	}
}

func cmpAndSwapV2(badgerTxn *badger.Txn, bk, oldValue, newValue []byte) ([]byte, bool, error) {
	current, err := badgerGetV2(badgerTxn, bk)
	// If value does not exist but expected is not nil, then return w/out swapping.
	if err != nil && !database.IsErrNotFound(err) {
		return nil, false, err
	}
	if !bytes.Equal(current, oldValue) {
		return current, false, nil
	}

	if err := badgerTxn.Set(bk, newValue); err != nil {
		return current, false, errors.Wrapf(err, "failed to set %s", bk)
	}
	return newValue, true, nil
}

// Update performs multiple commands on one read-write transaction.
func (db *DBv2) Update(txn *database.Tx) error {
	return db.db.Update(func(badgerTxn *badger.Txn) (err error) {
		for _, q := range txn.Operations {
			switch q.Cmd {
			case database.CreateTable:
				if err = db.CreateTable(q.Bucket); err != nil {
					return err
				}
				continue
			case database.DeleteTable:
				if err = db.DeleteTable(q.Bucket); err != nil {
					return err
				}
				continue
			}
			bk, err := toBadgerKey(q.Bucket, q.Key)
			if err != nil {
				return err
			}
			switch q.Cmd {
			case database.Get:
				if q.Result, err = badgerGetV2(badgerTxn, bk); err != nil {
					return errors.Wrapf(err, "failed to get %s/%s", q.Bucket, q.Key)
				}
			case database.Set:
				if err := badgerTxn.Set(bk, q.Value); err != nil {
					return errors.Wrapf(err, "failed to set %s/%s", q.Bucket, q.Key)
				}
			case database.Delete:
				if err = badgerTxn.Delete(bk); err != nil {
					return errors.Wrapf(err, "failed to delete %s/%s", q.Bucket, q.Key)
				}
			case database.CmpAndSwap:
				q.Result, q.Swapped, err = cmpAndSwapV2(badgerTxn, bk, q.CmpValue, q.Value)
				if err != nil {
					return errors.Wrapf(err, "failed to CmpAndSwap %s/%s", q.Bucket, q.Key)
				}
			case database.CmpOrRollback:
				return database.ErrOpNotSupported
			default:
				return database.ErrOpNotSupported
			}
		}
		return nil
	})
}
