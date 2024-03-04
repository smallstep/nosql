// Package badger implements a [nosql.Driver] for badger databases.
package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"

	badgerv1 "github.com/dgraph-io/badger"
	badgerv2 "github.com/dgraph-io/badger/v2"
	badgerv3 "github.com/dgraph-io/badger/v3"

	"github.com/smallstep/nosql"
	"github.com/smallstep/nosql/internal/each"
)

func init() {
	nosql.Register("badger", Open)
}

// ErrNoRewrite is returned when CompactByFactor doesn't result in any rewrites.
var ErrNoRewrite = errors.New("nosql/badger: CompactByFactor didn't result in any cleanup")

// Open implements a [nosql.Driver] for badger databases. In case of an existing database, [Open]
// will use either [OpenV1] or [OpenV2] depending on the version contained in the database's
// MANIFEST file.
//
// When creating new databases, [Open] is a passthrough call to [OpenV2].
func Open(ctx context.Context, dir string) (nosql.DB, error) {
	return determineOpener(dir)(ctx, dir)
}

func determineOpener(dir string) (opener func(context.Context, string) (nosql.DB, error)) {
	opener = OpenV4 // use the latest as the default

	f, err := os.Open(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		return // either no database exists or another error took place; use the default
	}
	defer f.Close()

	if _, _, err = badgerv1.ReplayManifestFile(f); err == nil {
		opener = OpenV1

		return
	} else if _, err = f.Seek(0, io.SeekStart); err != nil {
		return
	}

	if _, _, err = badgerv2.ReplayManifestFile(f); err == nil {
		opener = OpenV2

		return
	} else if _, err = f.Seek(0, io.SeekStart); err == nil {
		return
	}

	if _, _, err = badgerv3.ReplayManifestFile(f); err == nil {
		opener = OpenV3
	}

	return
}

// item defines the item generic constraint.
type item interface {
	Key() []byte
	KeyCopy([]byte) []byte
	Value(func([]byte) error) error
	ValueCopy([]byte) ([]byte, error)
}

// iterator defines the iterator generic constraint.
type iterator[KV item] interface {
	Close()
	Item() KV
	Seek([]byte)
	ValidForPrefix([]byte) bool
	Next()
}

// tx defines the transaction generic constraint.
type tx[IO any, KV item, I iterator[KV]] interface {
	Delete(key []byte) error
	Set(key, value []byte) error
	Get(key []byte) (KV, error)
	NewIterator(opt IO) I
}

// db defines the database generic constraint.
type db[IO any, KV item, I iterator[KV], T tx[IO, KV, I]] interface {
	Close() error
	View(func(T) error) error
	Update(func(T) error) error
	RunValueLogGC(float64) error
}

type wrapper[IO any, KV item, I iterator[KV], TX tx[IO, KV, I]] struct {
	db                           db[IO, KV, I, TX]
	isKeyNotFound                func(error) bool
	isNoRewrite                  func(error) bool
	keysOnlyIteratorOptions      func(prefix []byte) IO
	keysAndValuesIteratorOptions func(prefix []byte) IO
}

func (w *wrapper[_, _, _, _]) CompactByFactor(_ context.Context, factor float64) (err error) {
	if err = w.db.RunValueLogGC(factor); err != nil && w.isNoRewrite(err) {
		err = ErrNoRewrite
	}
	return
}

func (w *wrapper[_, _, _, _]) Close(_ context.Context) error {
	return w.db.Close()
}

func (w *wrapper[_, _, _, TX]) CreateBucket(_ context.Context, bucket []byte) error {
	id := encode(nil, bucket)

	return w.db.Update(func(tx TX) error {
		return tx.Set(id, []byte{})
	})
}

func (w *wrapper[_, _, _, TX]) DeleteBucket(_ context.Context, bucket []byte) error {
	prefix := encode(nil, bucket)

	return w.db.Update(func(tx TX) (err error) {
		it := tx.NewIterator(w.keysOnlyIteratorOptions(prefix))
		defer it.Close()

		var found bool
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			id := it.Item().Key()
			if bytes.Equal(prefix, id) {
				found = true
			}

			if err = tx.Delete(slices.Clone(id)); err != nil {
				return
			}
		}

		if !found {
			err = nosql.ErrBucketNotFound
		}

		return
	})
}

func (w *wrapper[_, _, _, _]) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	err = w.View(ctx, func(v nosql.Viewer) (err error) {
		value, err = v.Get(ctx, bucket, key)

		return
	})

	return
}

func (w *wrapper[_, _, _, _]) Put(ctx context.Context, bucket, key, value []byte) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.Put(ctx, bucket, key, value)
	})
}

func (w *wrapper[_, _, _, _]) PutMany(ctx context.Context, records ...nosql.Record) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.PutMany(ctx, records...)
	})
}

func (w *wrapper[_, _, _, _]) Delete(ctx context.Context, bucket, key []byte) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.Delete(ctx, bucket, key)
	})
}

func (w *wrapper[_, _, _, _]) List(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
	err = w.View(ctx, func(v nosql.Viewer) (err error) {
		records, err = v.List(ctx, bucket)

		return
	})

	return
}

func (w *wrapper[_, _, _, _]) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
	return w.Mutate(ctx, func(m nosql.Mutator) error {
		return m.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
	})
}

func (w *wrapper[IO, KV, S, TX]) View(_ context.Context, fn func(nosql.Viewer) error) error {
	return w.db.View(func(tx TX) error {
		return fn(&viewer[IO, KV, S, TX]{
			tx:                           tx,
			isKeyNotFound:                w.isKeyNotFound,
			keysAndValuesIteratorOptions: w.keysAndValuesIteratorOptions,
		})
	})
}

func (w *wrapper[IO, KV, S, TX]) Mutate(_ context.Context, fn func(nosql.Mutator) error) error {
	return w.db.Update(func(tx TX) error {
		return fn(&mutator[IO, KV, S, TX]{
			tx:                           tx,
			isKeyNotFound:                w.isKeyNotFound,
			keysAndValuesIteratorOptions: w.keysAndValuesIteratorOptions,
			KeysOnlyIteratorOptions:      w.keysOnlyIteratorOptions,
		})
	})
}

type viewer[IO any, KV item, S iterator[KV], TX tx[IO, KV, S]] struct {
	tx                           TX
	isKeyNotFound                func(error) bool
	keysAndValuesIteratorOptions func([]byte) IO
}

func (v *viewer[_, _, _, _]) Get(_ context.Context, bucket, key []byte) ([]byte, error) {
	return get(v.tx, bucket, key, v.isKeyNotFound)
}

func (v *viewer[IO, KV, S, _]) List(_ context.Context, bucket []byte) ([]nosql.Record, error) {
	prefix := encode(nil, bucket)

	it := v.tx.NewIterator(v.keysAndValuesIteratorOptions(prefix))
	defer it.Close()

	return list[IO, KV, S](it, prefix)
}

type mutator[IO any, KV item, S iterator[KV], TX tx[IO, KV, S]] struct {
	tx                           TX
	isKeyNotFound                func(error) bool
	KeysOnlyIteratorOptions      func([]byte) IO
	keysAndValuesIteratorOptions func([]byte) IO
}

func (m *mutator[_, _, _, _]) Get(_ context.Context, bucket, key []byte) ([]byte, error) {
	return get(m.tx, bucket, key, m.isKeyNotFound)
}

func (m *mutator[IO, KV, S, _]) List(_ context.Context, bucket []byte) ([]nosql.Record, error) {
	prefix := encode(nil, bucket)

	it := m.tx.NewIterator(m.keysAndValuesIteratorOptions(prefix))
	defer it.Close()

	return list[IO, KV, S](it, prefix)
}

func (m *mutator[_, _, _, _]) CompareAndSwap(_ context.Context, bucket, key, oldValue, newValue []byte) error {
	id := encode(nil, bucket, key)

	if err := checkPrefix(m.tx, id[:2+len(bucket)], m.isKeyNotFound); err != nil {
		return err
	}

	item, err := m.tx.Get(id)
	if err != nil {
		if m.isKeyNotFound(err) {
			err = nosql.ErrKeyNotFound
		}

		return err
	}

	if err := item.Value(func(current []byte) error {
		if !bytes.Equal(current, oldValue) {
			return &nosql.ComparisonError{
				Value: slices.Clone(current),
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return m.tx.Set(id, newValue)
}

func (m *mutator[_, _, _, _]) Delete(_ context.Context, bucket, key []byte) (err error) {
	id := encode(nil, bucket, key)

	if err = checkPrefix(m.tx, id[:2+len(bucket)], m.isKeyNotFound); err == nil {
		err = m.tx.Delete(id)
	}

	return
}

func (m *mutator[_, _, _, _]) Put(_ context.Context, bucket, key, value []byte) (err error) {
	id := encode(nil, bucket, key)

	if err = checkPrefix(m.tx, id[:2+len(bucket)], m.isKeyNotFound); err == nil {
		err = m.tx.Set(id, value)
	}

	return
}

func (m *mutator[_, _, _, _]) PutMany(_ context.Context, records ...nosql.Record) error {
	if len(records) == 0 {
		return nil
	}

	prefix := make([]byte, 0, 2+nosql.MaxBucketSize)

	return each.Bucket(records, func(bucket []byte, rex []*nosql.Record) (err error) {
		prefix = encode(prefix[:0], bucket)

		if err = checkPrefix(m.tx, prefix, m.isKeyNotFound); err != nil {
			return
		}

		for _, r := range rex {
			id := encode(prefix[:len(prefix):len(prefix)], r.Key)

			if err = m.tx.Set(id, r.Value); err != nil {
				break
			}
		}

		return
	})
}

func get[IO any, KV item, S iterator[KV], TX tx[IO, KV, S]](tx TX, bucket, key []byte, isKeyNotFound func(error) bool) ([]byte, error) {
	id := encode(nil, bucket, key)

	if err := checkPrefix(tx, id[:2+len(bucket)], isKeyNotFound); err != nil {
		return nil, err
	}

	switch itm, err := tx.Get(id); {
	case err == nil:
		return itm.ValueCopy(nil)
	case isKeyNotFound(err):
		return nil, nosql.ErrKeyNotFound
	default:
		return nil, err
	}
}

func list[IO any, KV item, S iterator[KV]](it S, prefix []byte) ([]nosql.Record, error) {
	skip := len(prefix) + 2

	var records []nosql.Record
	var foundBucket bool
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		if bytes.Equal(prefix, item.Key()) {
			foundBucket = true

			continue // found the bucket
		}

		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		records = append(records, nosql.Record{
			Bucket: slices.Clone(prefix[2:]),
			Key:    slices.Clone(item.Key()[skip:]),
			Value:  value,
		})
	}

	if !foundBucket {
		return nil, nosql.ErrBucketNotFound
	}

	// we're delimiting the tokens so a sort is required.
	slices.SortFunc(records, func(a, b nosql.Record) (v int) {
		return bytes.Compare(a.Key, b.Key)
	})

	return records, nil
}

// checkPrefix checks whether the provided prefix (which is an encoded bucket) exists.
func checkPrefix[IO any, KV item, S iterator[KV], TX tx[IO, KV, S]](tx TX, prefix []byte, isKeyNotFound func(error) bool) (err error) {
	if _, err = tx.Get(prefix); err != nil && isKeyNotFound(err) {
		err = nosql.ErrBucketNotFound
	}
	return
}

// encode appends the encoded representation of the given tokens to the given destination buffer
// and returns the result.
func encode(dst []byte, tokens ...[]byte) []byte {
	for _, tok := range tokens {
		if l := len(tok); l > math.MaxUint16 {
			panic(fmt.Errorf("token is too long (%d)", l))
		}
	}

	for _, tok := range tokens {
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(tok)))
		dst = append(dst, tok...)
	}

	return dst
}

// ContextWithOptions returns a copy of the provided [context.Context] that carries the provided
// [Options].
func ContextWithOptions(ctx context.Context, opts Options) context.Context {
	return context.WithValue(ctx, optionsContextKeyType{}, opts)
}

// OptionsFromContext reports the [Options] the provided [context.Context] carries or sensible
// defaults.
func OptionsFromContext(ctx context.Context) (opts Options) {
	opts, _ = ctx.Value(optionsContextKeyType{}).(Options)

	return
}

type optionsContextKeyType struct{}

// Options wraps the set of configuration for badger databases.
type Options struct {
	// ValueDir specifies the directory to use for values. If empty,
	// the database directory will be used in its place.
	ValueDir string

	// Logger specifies the logger to use. If nil, a default one
	// will be used in its place.
	Logger Logger
}

// Logger wraps the set of badger loggers.
type Logger interface {
	Errorf(string, ...any)
	Warningf(string, ...any)
	Infof(string, ...any)
	Debugf(string, ...any)
}
