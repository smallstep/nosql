package nosql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
)

var (
	// ErrNilBucket is contained in the chains of errors returned by functions that accept a bucket
	// when the bucket is nil.
	ErrNilBucket = errors.New(errPrefix + "bucket is nil")

	// ErrEmptyBucket is contained in the chains of errors returned by functions that accept a
	// bucket when the bucket is empty.
	ErrEmptyBucket = errors.New(errPrefix + "bucket is empty")

	// ErrBucketTooLong is contained in the chains of errors returned by functions that accept a
	// bucket when the bucket's length exceeds [MaxBucketSize].
	ErrBucketTooLong = errors.New(errPrefix + "bucket is too long")

	// ErrInvalidBucket is contained in the chains of errors returned by functions that accept a
	// bucket when the bucket contains the zero byte or an invalid UTF-8 sequence.
	ErrInvalidBucket = errors.New(errPrefix + "bucket is invalid")

	// ErrBucketNotFound is contained in the chains of errors returned by functions that expect
	// the presence of a bucket that does not exist.
	ErrBucketNotFound = errors.New(errPrefix + "bucket not found")

	// ErrNilKey is contained in the chains of errors returned by functions that accept a key when
	// the key is nil.
	ErrNilKey = errors.New(errPrefix + "key is nil")

	// ErrEmptyKey is contained in the chains of errors returned by functions that accept a key
	// when the key is empty.
	ErrEmptyKey = errors.New(errPrefix + "key is empty")

	// ErrKeyTooLong is contained in the chains of errors returned by functions that accept a key
	// when the key's length exceeds [MaxKeySize].
	ErrKeyTooLong = errors.New(errPrefix + "key is too long")

	// ErrKeyNotFound is contained in the chains of errors returned by functions that expect
	// the presence of a key that does not exist.
	ErrKeyNotFound = errors.New(errPrefix + "key not found")

	// ErrNilValue is contained in the chains of errors returned by functions that accept a value
	// when the value is nil.
	ErrNilValue = errors.New(errPrefix + "value is nil")

	// ErrValueTooLong is contained in the chains of errors returned by functions that accept a
	// value when the value's length exceeds [MaxValueSize].
	ErrValueTooLong = errors.New(errPrefix + "value is too long")
)

// ComparisonError is a type of error contained in the chains of errors returned by CompareAndSwap
// functions when the swap fails due to the existing value being different than the expected one.
type ComparisonError struct {
	// Value holds a copy of the value the key had at the time of the call.
	Value []byte
}

// Error implements [error] for [ComparisonError].
func (e *ComparisonError) Error() string {
	return errPrefix + "unexpected value"
}

const (
	errPrefix = "nosql: " // error prefix

	// MinBucketSize denotes the minimum allowed byte size for buckets.
	MinBucketSize = 1

	// MaxBucketSize denotes the maximum allowed byte size for buckets.
	MaxBucketSize = 50

	// MinKeySize denotes the minimum allowed byte size for keys.
	MinKeySize = 1

	// MaxKeySize denotes the maximum allowed byte size for keys.
	MaxKeySize = 200

	// MaxValueSize denotes the maximum allowed byte size for values.
	MaxValueSize = 1 << 14
)

var (
	driversMu sync.RWMutex // protects drivers
	drivers   = make(map[string]Driver)
)

// Driver wraps the set of database drivers.
type Driver func(ctx context.Context, dsn string) (DB, error)

// Register registers the named driver.
func Register(name string, driver Driver) {
	if driver == nil {
		panic(errPrefix + "nil driver")
	}

	driversMu.Lock()
	defer driversMu.Unlock()

	if _, dup := drivers[name]; dup {
		panic(fmt.Sprintf(errPrefix+"driver %q is already registered", name))
	}

	drivers[name] = driver
}

// Open opens the database the provided DSN describes, via the named driver.
func Open(ctx context.Context, driverName, dsn string) (DB, error) {
	driversMu.Lock()
	driver := drivers[driverName]
	driversMu.Unlock()

	if driver == nil {
		return nil, fmt.Errorf(errPrefix+"driver %q is not registered", driverName)
	}

	db, err := driver(ctx, dsn)
	if err == nil {
		db = Constrain(db)
	}
	return db, err
}

// Drivers returns a sorted list of the registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()

	l := make([]string, 0, len(drivers))
	for driver := range drivers {
		l = append(l, driver)
	}
	sort.Strings(l)

	return l
}

// DB wraps functionality exported by compatible data stores.
//
// Implementations of this interface returned by [Open], enforce constraints for the 3 types of
// tokens (buckets, keys and values) as follows:
//
//   - For any bucket to be considered valid, it must be a non-nil, non-empty, valid UTF-8 encoded
//     byte slice that does not contain the zero byte and that is up to [MaxBucketSize] bytes long.
//   - Respectively, for any key to be considered valid, it must be a non-nil, non-empty byte slice
//     that is up to [MaxKeySize] bytes long.
//   - And, finally, for any value to be considered valid, it must be a non-nil byte slice that is
//     up to [MaxValueSize] bytes long.
//
// When a token violates the above constraints, one of the sentinel errors this package declares
// (e.x. [ErrNilBucket], [ErrValueTooLong], etc.) will be returned to the caller.
type DB interface {
	Mutator

	// Close closes the underlying connection[s] to the database.
	Close(context.Context) error

	// CreateBucket creates the given bucket. It returns no error when the given bucket already
	// exists.
	//
	// See [DB] for the validations CreateBucket performs on the given bucket.
	CreateBucket(ctx context.Context, bucket []byte) error

	// DeleteBucket deletes the given bucket. It returns an error that contains [ErrBucketNotFound]
	// in its chain when the bucket does not exist.
	//
	// See [DB] for the validations DeleteBucket performs on the given bucket.
	DeleteBucket(ctx context.Context, bucket []byte) error

	// View runs the given read-only transaction against the database and returns its error.
	View(context.Context, func(Viewer) error) error

	// Mutate runs the given read-write transaction against the database and returns its error.
	Mutate(context.Context, func(Mutator) error) error
}

// Viewer is the interface that database views implement.
type Viewer interface {
	// Get returns the value stored in the given bucket for the given key. It returns an error
	// that contains
	//
	//  - [ErrBucketNotFound] in its chain when the given bucket does not exist.
	//  - [ErrKeyNotFound] in its chain when the given key does not exist in the given bucket.
	//
	// See [DB] for the validations Get performs on the given bucket and key.
	Get(ctx context.Context, bucket, key []byte) ([]byte, error)

	// List returns the records of the given bucket, in lexicographicaly sorted order (by key). It
	// returns an error containing [ErrBucketNotFound] in its chain when the given bucket does not
	// exist.
	//
	// See [DB] for the validations List performs on the given bucket.
	List(ctx context.Context, bucket []byte) ([]Record, error)
}

// Mutator is the interface that database mutators implement.
type Mutator interface {
	Viewer

	// CompareAndSwap sets the value stored in the given bucket for the given key, to the given
	// new one (newValue), if and only if the current value of the key equals the given one
	// (oldValue). It returns an error containing in its chain
	//
	//  - [ErrBucketNotFound] when the given bucket does not exist.
	//  - [ErrKeyNotFound] when the given key does not exist in the given bucket.
	//  - a reference to a [ComparisonError] when the value for the key at the time of the attempted
	//    swap differed from the expected one (oldValue).
	//
	// See [DB] for the validations CompareAndSwap performs on the given bucket, key, oldValue and
	// newValue tokens.
	CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error

	// Delete removes any value the given bucket might hold for the given key. It returns an error
	// containing [ErrBucketNotFound] in its chain when the bucket does not exist.
	//
	// See [DB] for the additional validations Delete performs on the provided bucket and key
	// tokens.
	Delete(ctx context.Context, bucket, key []byte) error

	// Put stores the given value in the given bucket for the given key, overwritting any existing
	// value may already be present. It returns an error containing [ErrBucketNotFound] in its chain
	// when the given bucket does not exist.
	//
	// See [DB] for the validations Put performs on the provided bucket, key and value tokens.
	Put(ctx context.Context, bucket, key, value []byte) error

	// PutMany stores the given records, overwritting the values of any existing ones. It returns an
	// error containing [ErrBucketNotFound] in its chain, when the bucket of any given
	// [Record] does not exist.
	//
	// See [DB] for the validations Put performs on the tokens of the given records.
	PutMany(context.Context, ...Record) error
}

// Record wraps the set of database records.
type Record struct {
	// Bucket denotes the bucket the key/value pair belongs to.
	Bucket []byte

	// Key denotes the key/value pair's key.
	Key []byte

	// Key denotes the key/value pair's value.
	Value []byte
}

// String implements fmt.Stringer for [Record].
func (r *Record) String() string {
	return fmt.Sprintf("(%q, %q, %q)", r.Bucket, r.Key, r.Value)
}

// GoString implements fmt.GoStringer for [Record].
func (r *Record) GoString() string {
	return fmt.Sprintf("nosql.Record{%q, %q, %q}", r.Bucket, r.Key, r.Value)
}

// CompactedByFactor is the interface instances of [DB] also implement in case they support they may
// be compacted by a factor.
type CompactedByFactor interface {
	CompactByFactor(context.Context, float64) error
}

// Copy returns a copy of the [Record].
func (r *Record) Clone() Record {
	return Record{
		Bucket: slices.Clone(r.Bucket),
		Key:    slices.Clone(r.Key),
		Value:  slices.Clone(r.Value),
	}
}
