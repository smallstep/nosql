// Package bolt implements a [nosql.Driver] for bolt databases.
package bolt

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"time"

	"go.etcd.io/bbolt"

	"github.com/smallstep/nosql"
)

func init() {
	nosql.Register("bolt", Open)
	nosql.Register("bbolt", Open) // to keep compatibility with earlier versions
}

// Open implements a [nosql.Driver] for boltdb databases.
func Open(ctx context.Context, path string) (nosql.DB, error) {
	var (
		opts = OptionsFromContext(ctx)
		fm   = FileModeFromContext(ctx)
	)

	db, err := bbolt.Open(path, fm, opts)
	if err != nil {
		return nil, err
	}

	return nosql.Constrain(&wrapper{
		db: db,
	}), nil
}

type wrapper struct {
	db *bbolt.DB
}

func (w *wrapper) Close(context.Context) error {
	return w.db.Close()
}

func (w *wrapper) CreateBucket(_ context.Context, bucket []byte) error {
	return w.db.Update(func(tx *bbolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(bucket)

		return
	})
}

func (w *wrapper) DeleteBucket(_ context.Context, bucket []byte) error {
	return w.db.Update(func(tx *bbolt.Tx) (err error) {
		switch err = tx.DeleteBucket(bucket); {
		case err == nil:
			break
		case errors.Is(err, bbolt.ErrBucketNotFound):
			err = nosql.ErrBucketNotFound
		}

		return
	})
}

func (w *wrapper) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	err = w.View(ctx, func(v nosql.Viewer) (err error) {
		value, err = v.Get(ctx, bucket, key)

		return
	})

	return
}

func (w *wrapper) Put(ctx context.Context, bucket, key, value []byte) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.Put(ctx, bucket, key, value)
	})
}

func (w *wrapper) PutMany(ctx context.Context, records ...nosql.Record) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.PutMany(ctx, records...)
	})
}

func (w *wrapper) Delete(ctx context.Context, bucket, key []byte) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.Delete(ctx, bucket, key)
	})
}

func (w *wrapper) List(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
	err = w.View(ctx, func(v nosql.Viewer) (err error) {
		records, err = v.List(ctx, bucket)

		return
	})

	return
}

func (w *wrapper) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
	})
}

func (w *wrapper) View(_ context.Context, fn func(nosql.Viewer) error) error {
	return w.db.View(func(tx *bbolt.Tx) error {
		return fn(&viewer{tx})
	})
}

func (w *wrapper) Mutate(_ context.Context, fn func(nosql.Mutator) error) error {
	return w.db.Update(func(tx *bbolt.Tx) error {
		return fn(&mutator{tx})
	})
}

type viewer struct {
	tx *bbolt.Tx
}

func (v *viewer) Get(_ context.Context, bucket, key []byte) ([]byte, error) {
	return get(v.tx, bucket, key)
}

func (v *viewer) List(_ context.Context, bucket []byte) ([]nosql.Record, error) {
	return list(v.tx, bucket)
}

type mutator struct {
	tx *bbolt.Tx
}

func (m *mutator) Get(_ context.Context, bucket, key []byte) ([]byte, error) {
	return get(m.tx, bucket, key)
}

func (m *mutator) List(_ context.Context, bucket []byte) ([]nosql.Record, error) {
	return list(m.tx, bucket)
}

func (m *mutator) CompareAndSwap(_ context.Context, bucket, key, oldValue, newValue []byte) error {
	b := m.tx.Bucket(bucket)
	if b == nil {
		return nosql.ErrBucketNotFound
	}

	val := b.Get(key)
	if val == nil {
		return nosql.ErrKeyNotFound
	}

	if !bytes.Equal(oldValue, val) {
		return &nosql.ComparisonError{
			Value: slices.Clone(val),
		}
	}

	return b.Put(key, newValue)
}

func (m *mutator) Delete(_ context.Context, bucket, key []byte) error {
	b := m.tx.Bucket(bucket)
	if b == nil {
		return nosql.ErrBucketNotFound
	}

	return b.Delete(key)
}

func (m *mutator) Put(_ context.Context, bucket, key, value []byte) error {
	return put(m.tx, bucket, key, value)
}

func (m *mutator) PutMany(_ context.Context, records ...nosql.Record) (err error) {
	for _, r := range records {
		if err = put(m.tx, r.Bucket, r.Key, r.Value); err != nil {
			break
		}
	}

	return
}

func get(tx *bbolt.Tx, bucket, key []byte) (value []byte, err error) {
	if b := tx.Bucket(bucket); b == nil {
		err = nosql.ErrBucketNotFound
	} else if value = b.Get(key); value == nil {
		err = nosql.ErrKeyNotFound
	}

	return
}

func list(tx *bbolt.Tx, bucket []byte) (records []nosql.Record, err error) {
	b := tx.Bucket(bucket)
	if b == nil {
		err = nosql.ErrBucketNotFound

		return
	}

	err = b.ForEach(func(key, value []byte) error {
		records = append(records, nosql.Record{
			Bucket: slices.Clone(bucket),
			Key:    slices.Clone(key),
			Value:  slices.Clone(value),
		})

		return nil
	})

	return
}

func put(tx *bbolt.Tx, bucket, key, value []byte) error {
	b := tx.Bucket(bucket)
	if b == nil {
		return nosql.ErrBucketNotFound
	}

	return b.Put(key, value)
}

// ContextWithOptions returns a [context.Context] that carries the provided [bbolt.Options].
func ContextWithOptions(ctx context.Context, opts *bbolt.Options) context.Context {
	return context.WithValue(ctx, optionsKey{}, opts)
}

// OptionsFromContext reports the [bbolt.Options] the given [context.Context] carries or sensible
// defaults.
func OptionsFromContext(ctx context.Context) (opts *bbolt.Options) {
	var ok bool
	if opts, ok = ctx.Value(optionsKey{}).(*bbolt.Options); !ok || opts == nil {
		opts = &bbolt.Options{
			Timeout:      5 * time.Second,
			FreelistType: bbolt.FreelistArrayType,
		}
	}

	return opts
}

type optionsKey struct{}

// ContextWithFileMode returns a [context.Context] that carries the provided [bbolt.Options].
func ContextWithFileMode(ctx context.Context, fm os.FileMode) context.Context {
	return context.WithValue(ctx, fileModeKey{}, fm)
}

// FileModeFromContext reports the [os.FileMode] the given [context.Context] carries or 0600.
func FileModeFromContext(ctx context.Context) (fm os.FileMode) {
	var ok bool
	if fm, ok = ctx.Value(fileModeKey{}).(os.FileMode); !ok {
		fm = 0600
	}
	return
}

type fileModeKey struct{}
