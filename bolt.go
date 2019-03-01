package nosql

import (
	"strings"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

// BoltDB is a wrapper over bolt.DB,
type BoltDB struct {
	db *bolt.DB
}

type boltBucket interface {
	Bucket(name []byte) *bolt.Bucket
	CreateBucket(name []byte) (*bolt.Bucket, error)
	CreateBucketIfNotExists(name []byte) (*bolt.Bucket, error)
	DeleteBucket(name []byte) error
}

// Open opens or creates a BoltDB database in the given path.
func (db *BoltDB) Open(path string) (err error) {
	db.db, err = bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	return errors.WithStack(err)
}

// Close closes the BoltDB database.
func (db *BoltDB) Close() error {
	return errors.WithStack(db.db.Close())
}

// CreateTable creates a bucket or an embedded bucket if it does not exists.
func (db *BoltDB) CreateTable(bucket string) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return db.createBucket(tx, bucket)
	})
}

// DeleteTable deletes a root or embedded bucket. Returns an error if the
// bucket cannot be found or if the key represents a non-bucket value.
func (db *BoltDB) DeleteTable(bucket string) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		return db.deleteBucket(tx, bucket)
	})
}

// Get returns the value stored in the given bucked and key.
func (db *BoltDB) Get(bucket, key string) (ret []byte, err error) {
	err = db.db.View(func(tx *bolt.Tx) error {
		b, err := db.getBucket(tx, bucket)
		if err != nil {
			return err
		}
		ret = b.Get([]byte(key))
		if ret == nil {
			return errors.WithStack(ErrNotFound)
		}
		// Make sure to return a copy as ret is only valid during the
		// transaction.
		ret = cloneBytes(ret)
		return nil
	})
	return
}

// Set stores the given value on bucket and key.
func (db *BoltDB) Set(bucket, key string, value []byte) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b, err := db.getBucket(tx, bucket)
		if err != nil {
			return err
		}
		return errors.WithStack(b.Put([]byte(key), value))
	})
}

// Del deletes the value stored in the given bucked and key.
func (db *BoltDB) Del(bucket, key string) error {
	return db.db.Update(func(tx *bolt.Tx) error {
		b, err := db.getBucket(tx, bucket)
		if err != nil {
			return err
		}
		return errors.WithStack(b.Delete([]byte(key)))
	})
}

// List returns the full list of entries in a bucket.
func (db *BoltDB) List(bucket string) ([]*Entry, error) {
	var entries []*Entry
	err := db.db.View(func(tx *bolt.Tx) error {
		b, err := db.getBucket(tx, bucket)
		if err != nil {
			return err
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			entries = append(entries, &Entry{
				Bucket: bucket,
				Key:    string(cloneBytes(k)),
				Value:  cloneBytes(v),
			})
		}
		return nil
	})
	return entries, err
}

// Update performs multiple commands on one read-write transaction.
func (db *BoltDB) Update(tx *Tx) error {
	return db.db.Update(func(boltTx *bolt.Tx) (err error) {
		var b *bolt.Bucket
		for _, q := range tx.Operations {
			// create or delete buckets
			switch q.Cmd {
			case CreateTable:
				err = db.createBucket(boltTx, q.Bucket)
				if err != nil {
					return err
				}
				continue
			case DeleteTable:
				err = db.deleteBucket(boltTx, q.Bucket)
				if err != nil {
					return err
				}
				continue
			}

			// For other operations, get bucket and perform operation
			b, err = db.getBucket(boltTx, q.Bucket)
			if err != nil {
				return err
			}

			switch q.Cmd {
			case Get:
				ret := b.Get([]byte(q.Key))
				if ret == nil {
					return errors.WithStack(ErrNotFound)
				}
				q.Value = cloneBytes(ret)
			case Set:
				if err = b.Put([]byte(q.Key), []byte(q.Value)); err != nil {
					return errors.WithStack(err)
				}
			case Delete:
				if err = b.Delete([]byte(q.Key)); err != nil {
					return errors.WithStack(err)
				}
			case CmpAndSwap:
				return errors.Errorf("operation '%s' is not yet implemented", q.Cmd)
			case CmpOrRollback:
				return errors.Errorf("operation '%s' is not yet implemented", q.Cmd)
			default:
				return errors.Errorf("operation '%s' is not supported", q.Cmd)
			}
		}
		return nil
	})
}

// getBucket returns the bucket supporting nested buckets, nested buckets are
// bucket names separated by '/'.
func (db *BoltDB) getBucket(tx *bolt.Tx, name string) (b *bolt.Bucket, err error) {
	buckets := strings.Split(name, "/")
	for i, n := range buckets {
		if i == 0 {
			b = tx.Bucket([]byte(n))
		} else {
			b = b.Bucket([]byte(n))
		}
		if b == nil {
			return nil, errors.Wrapf(ErrNotFound, "bucket %s does not exist", strings.Join(buckets[0:i+1], "/"))
		}
	}
	return
}

// createBucket creates a bucket or a nested bucket in the given transaction.
func (db *BoltDB) createBucket(tx *bolt.Tx, name string) (err error) {
	b := boltBucket(tx)
	buckets := strings.Split(name, "/")
	for _, name := range buckets {
		b, err = b.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return
}

// deleteBucket deletes a bucket or a nested bucked in the given transaction.
func (db *BoltDB) deleteBucket(tx *bolt.Tx, name string) (err error) {
	b := boltBucket(tx)
	buckets := strings.Split(name, "/")
	last := len(buckets) - 1
	for i := 0; i < last; i++ {
		if b = b.Bucket([]byte(buckets[i])); b == nil {
			return errors.Wrapf(ErrNotFound, "bucket %s does not exist", strings.Join(buckets[0:i+1], "/"))
		}
	}
	err = b.DeleteBucket([]byte(buckets[last]))
	if err == bolt.ErrBucketNotFound {
		return errors.Wrapf(ErrNotFound, "bucket %s does not exist", name)
	}
	return
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
