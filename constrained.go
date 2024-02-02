package nosql

import (
	"context"
	"unicode"
	"unicode/utf8"
)

// Constrain wraps the provided [DB] implementation with all applicable constraints.
func Constrain(db DB) DB {
	if _, ok := db.(interface{ constrained() }); ok {
		return db // we've already wrapped this database with applicable constraints
	}

	if compactor, ok := db.(CompactedByFactor); ok {
		return &compactedByFactorConstrained{
			DB: &constrained{DB: db},
			fn: compactor.CompactByFactor,
		}
	}

	return &constrained{DB: db}
}

type compactedByFactorConstrained struct {
	DB
	fn func(context.Context, float64) error
}

func (c *compactedByFactorConstrained) CompactByFactor(ctx context.Context, factor float64) error {
	return c.fn(ctx, factor)
}

type constrained struct {
	DB
}

func (*constrained) constrained() {}

func (c *constrained) Close(ctx context.Context) error {
	return c.DB.Close(ctx)
}

func (c *constrained) CreateBucket(ctx context.Context, bucket []byte) (err error) {
	if err = validateBucket(bucket); err == nil {
		err = c.DB.CreateBucket(ctx, bucket)
	}

	return
}

func (c *constrained) DeleteBucket(ctx context.Context, bucket []byte) (err error) {
	if err = validateBucket(bucket); err == nil {
		err = c.DB.DeleteBucket(ctx, bucket)
	}

	return
}

func (c *constrained) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	if err = validateID(bucket, key); err == nil {
		value, err = c.DB.Get(ctx, bucket, key)
	}

	return
}

func (c *constrained) Put(ctx context.Context, bucket, key, value []byte) (err error) {
	if err = validateRecord(bucket, key, value); err == nil {
		err = c.DB.Put(ctx, bucket, key, value)
	}

	return
}

func (c *constrained) PutMany(ctx context.Context, records ...Record) (err error) {
	if err = validateRecords(records...); err == nil {
		err = c.DB.PutMany(ctx, records...)
	}

	return
}

func (c *constrained) Delete(ctx context.Context, bucket, key []byte) (err error) {
	if err = validateID(bucket, key); err == nil {
		err = c.DB.Delete(ctx, bucket, key)
	}

	return
}

func (c *constrained) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) (err error) {
	if err = validateRecord(bucket, key, oldValue); err == nil {
		if err = validateValue(newValue); err == nil {
			err = c.DB.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
		}
	}

	return
}

func (c *constrained) List(ctx context.Context, bucket []byte) (records []Record, err error) {
	if err = validateBucket(bucket); err == nil {
		records, err = c.DB.List(ctx, bucket)
	}

	return
}

func (c *constrained) View(ctx context.Context, fn func(Viewer) error) error {
	return c.DB.View(ctx, func(v Viewer) error {
		return fn(&constrainedViewer{v})
	})
}

func (c *constrained) Mutate(ctx context.Context, fn func(Mutator) error) error {
	return c.DB.Mutate(ctx, func(m Mutator) error {
		return fn(&constrainedMutator{mut: m})
	})
}

type constrainedViewer struct {
	v Viewer
}

func (cv *constrainedViewer) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	if err = validateID(bucket, key); err == nil {
		value, err = cv.v.Get(ctx, bucket, key)
	}

	return
}

func (cv *constrainedViewer) List(ctx context.Context, bucket []byte) (records []Record, err error) {
	if err = validateBucket(bucket); err == nil {
		records, err = cv.v.List(ctx, bucket)
	}

	return
}

type constrainedMutator struct {
	mut Mutator
}

func (cm *constrainedMutator) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	if err = validateID(bucket, key); err == nil {
		value, err = cm.mut.Get(ctx, bucket, key)
	}

	return
}

func (cm *constrainedMutator) List(ctx context.Context, bucket []byte) (records []Record, err error) {
	if err = validateBucket(bucket); err == nil {
		records, err = cm.mut.List(ctx, bucket)
	}

	return
}

func (cm *constrainedMutator) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) (err error) {
	if err = validateRecord(bucket, key, oldValue); err == nil {
		if err = validateValue(newValue); err == nil {
			err = cm.mut.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
		}
	}

	return
}

func (cm *constrainedMutator) Put(ctx context.Context, bucket, key, value []byte) (err error) {
	if err = validateRecord(bucket, key, value); err == nil {
		err = cm.mut.Put(ctx, bucket, key, value)
	}

	return
}

func (cm *constrainedMutator) PutMany(ctx context.Context, records ...Record) (err error) {
	if err = validateRecords(records...); err == nil {
		err = cm.mut.PutMany(ctx, records...)
	}

	return
}

func (cm *constrainedMutator) Delete(ctx context.Context, bucket, key []byte) (err error) {
	if err = validateID(bucket, key); err == nil {
		err = cm.mut.Delete(ctx, bucket, key)
	}

	return
}

func validateBucket(bucket []byte) error {
	if bucket == nil {
		return ErrNilBucket
	}

	if l := len(bucket); l == 0 {
		return ErrEmptyBucket
	} else if l > MaxBucketSize {
		return ErrBucketTooLong
	}

	// MySQL does not allow for invalid UTF-8 and both postgres & mysql do not allow zeroes even in
	// quoted identifiers.
	//
	// MySQL also does not allow identifiers names to end with space characters, and contain UTF-8
	// supplementary runes (U+10000 and above).
	//
	// Ref: https://dev.mysql.com/doc/refman/8.3/en/identifiers.html

	for {
		r, s := utf8.DecodeRune(bucket)
		switch {
		case r == utf8.RuneError && s == 1:
			return ErrInvalidBucket // contains invalid UTF-8 sequences
		case r < 1, r > '\U0000FFFF':
			return ErrInvalidBucket // contains runes outside the defined range
		}

		bucket = bucket[s:]
		if len(bucket) == 0 {
			if unicode.IsSpace(r) {
				return ErrInvalidBucket // ends with a space character
			}

			return nil
		}
	}
}

func validateKey(key []byte) error {
	if key == nil {
		return ErrNilKey
	}

	if l := len(key); l == 0 {
		return ErrEmptyKey
	} else if l > MaxKeySize {
		return ErrKeyTooLong
	}

	return nil
}

func validateID(bucket, key []byte) (err error) {
	if err = validateBucket(bucket); err == nil {
		err = validateKey(key)
	}
	return
}

func validateValue(value []byte) error {
	if value == nil {
		return ErrNilValue
	}
	if l := len(value); l > MaxValueSize {
		return ErrValueTooLong
	}
	return nil
}

func validateRecord(bucket, key, value []byte) (err error) {
	if err = validateID(bucket, key); err == nil {
		err = validateValue(value)
	}
	return
}

func validateRecords(records ...Record) (err error) {
	for _, r := range records {
		if err = validateRecord(r.Bucket, r.Key, r.Value); err != nil {
			break
		}
	}
	return
}
