package dbtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	mand "math/rand"
	"slices"
	"strconv"
	"sync"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smallstep/nosql"
	"github.com/smallstep/nosql/internal/token"
)

// suite implements state management for the test suite.
type suite struct {
	db nosql.DB

	generatedMu sync.Mutex                   // protects generated
	generated   map[string]map[string][]byte // bucket -> key -> value
}

func (s *suite) testCreateBucketValidations(t *testing.T) {
	t.Parallel()

	s.assertBucketError(t, func(t *testing.T, bucket []byte) error {
		return s.db.CreateBucket(newContext(t), bucket)
	})
}

func (s *suite) testCreateBucket(t *testing.T) {
	t.Parallel()

	cases := []struct {
		bucket []byte // input bucket
		err    error  // expected error
	}{
		0: {s.existingBucket(t), nil},
		1: {s.newBucket(t), nil},
	}

	for caseIndex := range cases {
		kase := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			if err := s.db.CreateBucket(newContext(t), kase.bucket); kase.err == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, kase.err, err)
			}
		})
	}
}

func (s *suite) testDeleteBucketValidations(t *testing.T) {
	t.Parallel()

	s.assertBucketError(t, func(t *testing.T, bucket []byte) error {
		return s.db.DeleteBucket(newContext(t), bucket)
	})
}

func (s *suite) testDeleteBucket(t *testing.T) {
	t.Parallel()

	// create a bucket with a few records
	nonEmpty := s.existingBucket(t)
	var keys [][]byte
	for i := 0; i < 5+mand.Intn(5); i++ { //nolint:gosec // not a sensitive op
		key, _ := s.existingKey(t, nonEmpty)
		keys = append(keys, key)
	}

	cases := []struct {
		bucket []byte
		keys   [][]byte
		err    error
	}{
		0: {s.newBucket(t), nil, nosql.ErrBucketNotFound},
		1: {s.existingBucket(t), nil, nil},
		2: {nonEmpty, keys, nil},
	}

	for caseIndex := range cases {
		kase := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			ctx := newContext(t)

			if err := s.db.DeleteBucket(ctx, kase.bucket); kase.err != nil {
				assert.ErrorIs(t, err, kase.err)

				return
			} else if !assert.NoError(t, err) {
				return
			}

			// deleting a deleted bucket should yield [ErrBucketNotFound]
			require.ErrorIs(t, s.db.DeleteBucket(ctx, kase.bucket), nosql.ErrBucketNotFound)

			// bucket should have deleted all of its keys as well when booted
			for _, key := range kase.keys {
				assert.ErrorIs(t, s.db.Delete(ctx, kase.bucket, key), nosql.ErrBucketNotFound)
			}
		})
	}
}

func (s *suite) testGetValidations(t *testing.T) {
	t.Parallel()

	s.testGetFuncValidations(t, s.db.Get)
}

func (s *suite) testViewerGetValidations(t *testing.T) {
	t.Parallel()

	s.testGetFuncValidations(t, func(ctx context.Context, bucket, key []byte) (value []byte, err error) {
		err = s.db.View(ctx, func(v nosql.Viewer) (err error) {
			value, err = v.Get(ctx, bucket, key)

			return
		})

		return
	})
}

func (s *suite) testMutatorGetValidations(t *testing.T) {
	t.Parallel()

	s.testGetFuncValidations(t, func(ctx context.Context, bucket, key []byte) (value []byte, err error) {
		err = s.db.Mutate(ctx, func(m nosql.Mutator) (err error) {
			value, err = m.Get(ctx, bucket, key)

			return
		})

		return
	})
}

type getFunc func(ctx context.Context, bucket, key []byte) ([]byte, error)

func (s *suite) testGetFuncValidations(t *testing.T, fn getFunc) {
	t.Helper()

	s.assertIDError(t, func(t *testing.T, bucket, key []byte) (err error) {
		_, err = fn(newContext(t), bucket, key)
		return
	})
}

func (s *suite) testGet(t *testing.T) {
	t.Parallel()

	s.testGetFunc(t, s.db.Get)
}

func (s *suite) testViewerGet(t *testing.T) {
	t.Parallel()

	s.testGetFunc(t, func(ctx context.Context, bucket, key []byte) (value []byte, err error) {
		err = s.db.View(ctx, func(v nosql.Viewer) (err error) {
			value, err = v.Get(ctx, bucket, key)

			return
		})
		return
	})
}

func (s *suite) testMutatorGet(t *testing.T) {
	t.Parallel()

	s.testGetFunc(t, func(ctx context.Context, bucket, key []byte) (value []byte, err error) {
		err = s.db.Mutate(ctx, func(m nosql.Mutator) (err error) {
			value, err = m.Get(ctx, bucket, key)

			return
		})
		return
	})
}

func (s *suite) testGetFunc(t *testing.T, get getFunc) {
	t.Helper()

	cases := []func() ([]byte, []byte, []byte, error){
		0: func() (bucket, key, value []byte, err error) {
			// when the bucket does not exist, we expect ErrBucketNotFound
			bucket = s.newBucket(t)
			key = s.anyKey(t)

			err = nosql.ErrBucketNotFound

			return
		},
		1: func() (bucket, key, value []byte, err error) {
			// when the bucket exists but the key does not, we expect nosql.ErrKeyNotFound
			bucket = s.existingBucket(t)
			key = s.anyKey(t)

			err = nosql.ErrKeyNotFound

			return
		},
		2: func() (bucket, key, value []byte, err error) {
			// when the key exists in the bucket, we expect the value the key points to
			bucket = s.existingBucket(t)
			key, value = s.existingKey(t, bucket)

			return
		},
	}

	for caseIndex := range cases {
		setup := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			var (
				ctx                             = newContext(t)
				bucket, key, expValue, expError = setup()
			)

			got, err := get(ctx, bucket, key)
			if expError != nil {
				assert.Same(t, expError, err)
				assert.Nil(t, got)

				return
			}

			// ensure we retrieved what we expected to retrieve
			assert.NoError(t, err)
			assert.Equal(t, expValue, got)
		})
	}
}

func (s *suite) testDeleteValidations(t *testing.T) {
	t.Parallel()

	s.testDeleteFuncValidations(t, s.db.Delete)
}

func (s *suite) testMutatorDeleteValidations(t *testing.T) {
	t.Parallel()

	s.testDeleteFuncValidations(t, func(ctx context.Context, bucket, key []byte) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.Delete(ctx, bucket, key)
		})
	})
}

func (s *suite) testDeleteFuncValidations(t *testing.T, del deleteFunc) {
	t.Helper()

	s.assertIDError(t, func(t *testing.T, bucket, key []byte) (err error) {
		return del(newContext(t), bucket, key)
	})
}

func (s *suite) testDelete(t *testing.T) {
	t.Parallel()

	s.testDeleteFunc(t, s.db.Delete)
}

func (s *suite) testMutatorDelete(t *testing.T) {
	t.Parallel()

	s.testDeleteFunc(t, func(ctx context.Context, bucket, key []byte) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.Delete(ctx, bucket, key)
		})
	})
}

type deleteFunc func(ctx context.Context, bucket, key []byte) error

func (s *suite) testDeleteFunc(t *testing.T, del deleteFunc) {
	t.Helper()

	cases := []func() (bucket, key []byte, err error){
		0: func() (bucket, key []byte, err error) {
			// when the bucket does not exist, we expect ErrBucketNotFound
			bucket = s.newBucket(t)
			key = s.anyKey(t)

			err = nosql.ErrBucketNotFound

			return
		},
		1: func() (bucket, key []byte, err error) {
			// when the bucket exists but the key does not, we expect no error
			bucket = s.existingBucket(t)
			key = s.anyKey(t)

			return
		},
		2: func() (bucket, key []byte, err error) {
			// when the key exists, we expect no error
			bucket = s.existingBucket(t)
			key, _ = s.existingKey(t, bucket)

			return
		},
	}

	for caseIndex := range cases {
		setup := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			var (
				ctx              = newContext(t)
				bucket, key, exp = setup()
			)

			err := del(ctx, bucket, key)
			if exp != nil {
				assert.Same(t, exp, err)

				return
			}

			assert.NoError(t, err)

			// ensure the key is not there any more
			_, err = s.db.Get(ctx, bucket, key)
			assert.Same(t, nosql.ErrKeyNotFound, err)
		})
	}
}

func (s *suite) testPutValidations(t *testing.T) {
	t.Parallel()

	s.testPutFuncValidations(t, s.db.Put)
}

func (s *suite) testMutatorPutValidations(t *testing.T) {
	t.Parallel()

	s.testPutFuncValidations(t, func(ctx context.Context, bucket, key, value []byte) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.Put(ctx, bucket, key, value)
		})
	})
}

func (s *suite) testPutFuncValidations(t *testing.T, put putFunc) {
	t.Helper()

	s.assertRecordError(t, func(t *testing.T, bucket, key, value []byte) error {
		return put(newContext(t), bucket, key, value)
	})
}

type putFunc func(ctx context.Context, bucket, key, value []byte) error

func (s *suite) testPut(t *testing.T) {
	t.Parallel()

	s.testPutFunc(t, s.db.Put)
}

func (s *suite) testMutatorPut(t *testing.T) {
	t.Parallel()

	s.testPutFunc(t, func(ctx context.Context, bucket, key, value []byte) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.Put(ctx, bucket, key, value)
		})
	})
}

func (s *suite) testPutFunc(t *testing.T, put putFunc) {
	t.Helper()

	cases := []func() ([]byte, []byte, []byte, error){
		0: func() (bucket, key, value []byte, err error) {
			// when the bucket does not exist, we expect [nosql.ErrBucketNotFound]
			bucket = s.newBucket(t)
			key = s.anyKey(t)
			value = s.anyValue(t)

			err = nosql.ErrBucketNotFound

			return
		},
		1: func() (bucket, key, value []byte, err error) {
			// when the bucket exists but the key does not, we expect the key to be created
			bucket = s.existingBucket(t)
			key = s.newKey(t, bucket)
			value = s.anyValue(t)

			return
		},
		2: func() (bucket, key, value []byte, err error) {
			// when the key exists in the bucket, and the new value equals the old one, we expect
			// a noop
			bucket = s.existingBucket(t)
			key, value = s.existingKey(t, bucket)

			return
		},
		3: func() (bucket, key, value []byte, err error) {
			// when the key exists in the bucket, and the new value differs from the old one, we
			// expect the value to be ovewritten
			bucket = s.existingBucket(t)
			key, value = s.existingKey(t, bucket)
			value = s.differentValue(t, value)

			return
		},
	}

	for caseIndex := range cases {
		setup := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			var (
				ctx                     = newContext(t)
				bucket, key, value, exp = setup()
			)

			if err := put(ctx, bucket, key, value); exp != nil {
				assert.ErrorIs(t, exp, err)

				return
			} else if !assert.NoError(t, err) {
				return
			}

			// ensure we can now read what we wrote
			if v, err := s.db.Get(ctx, bucket, key); assert.NoError(t, err) {
				assert.Equal(t, value, v)
			}
		})
	}
}

func (s *suite) testCompareAndSwapValidations(t *testing.T) {
	t.Parallel()

	s.testCompareAndSwapFuncValidations(t, s.db.CompareAndSwap)
}

func (s *suite) testMutatorCompareAndSwapValidations(t *testing.T) {
	t.Parallel()

	s.testCompareAndSwapFuncValidations(t, func(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
		})
	})
}

func (s *suite) testCompareAndSwapFuncValidations(t *testing.T, cas compareAndSwapFunc) {
	t.Helper()

	cases := []struct {
		bucket   []byte
		key      []byte
		oldValue []byte
		newValue []byte
	}{
		0: {nil, s.anyKey(t), s.anyValue(t), s.anyValue(t)},
		1: {[]byte{}, s.anyKey(t), s.anyValue(t), s.anyValue(t)},
		2: {s.longBucket(t), s.anyKey(t), s.anyValue(t), s.anyValue(t)},
		3: {s.invalidBucket(t), s.anyKey(t), s.anyValue(t), s.anyValue(t)},

		4: {s.newBucket(t), nil, s.anyValue(t), s.anyValue(t)},
		5: {s.newBucket(t), []byte{}, s.anyValue(t), s.anyValue(t)},
		6: {s.newBucket(t), s.longKey(t), s.anyValue(t), s.anyValue(t)},

		7: {s.newBucket(t), s.anyKey(t), nil, s.anyValue(t)},
		8: {s.newBucket(t), s.anyKey(t), s.longValue(t), s.anyValue(t)},

		9:  {s.newBucket(t), s.anyKey(t), s.anyValue(t), nil},
		10: {s.newBucket(t), s.anyKey(t), s.anyValue(t), s.longValue(t)},
	}

	for caseIndex := range cases {
		kase := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			exp := s.recordError(kase.bucket, kase.key, kase.oldValue)
			if exp == nil {
				exp = s.valueError(kase.newValue)
			}
			err := cas(newContext(t), kase.bucket, kase.key, kase.newValue, kase.oldValue)

			assert.ErrorIs(t, err, exp)
		})
	}
}

type compareAndSwapFunc func(ctx context.Context, bucket, key, oldValue, newValue []byte) error

func (s *suite) testCompareAndSwap(t *testing.T) {
	t.Parallel()

	s.testCompareAndSwapFunc(t, s.db.CompareAndSwap)
}

func (s *suite) testMutatorCompareAndSwap(t *testing.T) {
	t.Parallel()

	s.testCompareAndSwapFunc(t, func(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
		})
	})
}

func (s *suite) testCompareAndSwapFunc(t *testing.T, cas compareAndSwapFunc) {
	t.Helper()

	cases := []func() (bucket, key, oldValue, newValue, foundValue []byte, err error){
		0: func() (bucket, key, oldValue, newValue, _ []byte, err error) {
			// when the bucket does not exist, we expect ErrBucketNotFound
			bucket = s.newBucket(t)
			key = s.anyKey(t)
			oldValue = s.anyValue(t)
			newValue = s.anyValue(t)

			err = nosql.ErrBucketNotFound

			return
		},
		1: func() (bucket, key, oldValue, newValue, _ []byte, err error) {
			// when the key does not exist, we expect ErrKeyNotFound
			bucket = s.existingBucket(t)
			key = s.anyKey(t)
			oldValue = s.anyValue(t)
			newValue = s.anyValue(t)

			err = nosql.ErrKeyNotFound

			return
		},
		2: func() (bucket, key, oldValue, newValue, cmpValue []byte, _ error) {
			// when current != old, we expect a [ComparisonError]
			bucket = s.existingBucket(t)
			key, cmpValue = s.existingKey(t, bucket)

			oldValue = s.differentValue(t, cmpValue)
			newValue = s.anyValue(t)

			return
		},
		3: func() (bucket, key, oldValue, newValue, cmpValue []byte, _ error) {
			// when current != old AND current == newValue, we expect a [ComparisonError]
			bucket = s.existingBucket(t)
			key, cmpValue = s.existingKey(t, bucket)

			oldValue = s.differentValue(t, cmpValue)
			newValue = slices.Clone(cmpValue)

			return
		},
		4: func() (bucket, key, oldValue, newValue, _ []byte, _ error) {
			// when current == old AND old == new, we expect no error
			bucket = s.existingBucket(t)
			key, oldValue = s.existingKey(t, bucket)
			newValue = slices.Clone(oldValue)

			return
		},
		5: func() (bucket, key, oldValue, newValue, _ []byte, _ error) {
			// when current == old AND old != new, we expect no error
			bucket = s.existingBucket(t)
			key, oldValue = s.existingKey(t, bucket)
			newValue = s.differentValue(t, oldValue)

			return
		},
	}

	for caseIndex := range cases {
		setup := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			var (
				ctx                                               = newContext(t)
				bucket, key, oldValue, newValue, cmpValue, expErr = setup()
			)

			switch err := cas(ctx, bucket, key, oldValue, newValue); {
			case expErr == nil && cmpValue == nil:
				if !assert.NoError(t, err) {
					return
				}

				// ensure the change actually happened
				if got, err := s.db.Get(ctx, bucket, key); assert.NoError(t, err) {
					assert.Equal(t, newValue, got)
				}
			case cmpValue != nil && expErr == nil:
				var ce *nosql.ComparisonError
				require.ErrorAs(t, err, &ce)

				assert.Equal(t, cmpValue, ce.Value)
			case expErr != nil && cmpValue == nil:
				assert.ErrorIs(t, err, expErr)
			default:
				t.Fatal("invalid setup")
			}
		})
	}
}

func (s *suite) testPutManyValidations(t *testing.T) {
	t.Parallel()

	s.testPutManyFuncValidations(t, s.db.PutMany)
}

func (s *suite) testMutatorPutManyValidations(t *testing.T) {
	t.Parallel()

	s.testPutManyFuncValidations(t, func(ctx context.Context, records ...nosql.Record) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.PutMany(ctx, records...)
		})
	})
}

func (s *suite) testPutManyFuncValidations(t *testing.T, putMany putManyFunc) {
	t.Helper()

	s.assertRecordsError(t, func(t *testing.T, records ...nosql.Record) error {
		return putMany(newContext(t), records...)
	})
}

type putManyFunc func(ctx context.Context, records ...nosql.Record) error

func (s *suite) testPutMany(t *testing.T) {
	t.Parallel()

	s.testPutManyFunc(t, s.db.PutMany)
}

func (s *suite) testMutatorPutMany(t *testing.T) {
	t.Parallel()

	s.testPutManyFunc(t, func(ctx context.Context, records ...nosql.Record) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.PutMany(ctx, records...)
		})
	})
}

func (s *suite) testPutManyFunc(t *testing.T, putMany putManyFunc) {
	t.Helper()

	// we'll create a few records that already exist
	exp := s.existingRecords(t, 2, 3, 3, 4)
	existing := len(exp)

	// and a few that do not (but for buckets that do)
	for i := 0; i < 5+mand.Intn(5); i++ { //nolint:gosec // not a sensitive op
		bucket := exp[mand.Intn(existing)].Bucket //nolint:gosec // not a sensitive op

		exp = append(exp, nosql.Record{
			Bucket: bucket,
			Key:    s.newKey(t, bucket),
			Value:  s.anyValue(t),
		})
	}

	// for some of the existing records, we'll indicate we want a new value
	for i := 0; i < existing; i++ {
		if mand.Intn(2) == 1 { //nolint:gosec // not a sensitive op
			exp[i].Value = s.differentValue(t, exp[i].Value)
		}
	}

	// and, finally, we'll add a secondary update for one of each records
	di := mand.Intn(len(exp)) //nolint:gosec // not a sensitive op
	dup := exp[di].Clone()
	dup.Value = s.differentValue(t, dup.Value)
	exp = append(exp, dup)

	ctx := newContext(t)

	// then we'll put them in the database, ensuring we get no error
	require.NoError(t, putMany(ctx, exp...))

	// and retrieve them to compare what we read is what we expect
	for i, r := range exp {
		v, err := s.db.Get(ctx, r.Bucket, r.Key)
		if assert.NoError(t, err) {
			if i == di {
				// for this record, we expect the value to equal the one we set last
				assert.Equal(t, exp[len(exp)-1].Value, v)
			} else {
				assert.Equal(t, r.Value, v)
			}
		}
	}
}

func (s *suite) testPutManyError(t *testing.T) {
	t.Parallel()

	s.testPutManyFuncError(t, s.db.PutMany)
}

func (s *suite) testMutatorPutManyError(t *testing.T) {
	t.Parallel()

	s.testPutManyFuncError(t, func(ctx context.Context, records ...nosql.Record) error {
		return s.db.Mutate(ctx, func(m nosql.Mutator) error {
			return m.PutMany(ctx, records...)
		})
	})
}

func (s *suite) testPutManyFuncError(t *testing.T, putMany putManyFunc) {
	t.Helper()

	var (
		exp = s.existingRecords(t, 2, 3, 2, 4)
		src = make([]nosql.Record, 0, len(exp))
	)

	// we'll instruct the database to change the values of about half of the records we created
	// earlier
	for _, r := range exp {
		rr := r.Clone()

		if mand.Intn(2) == 1 { //nolint:gosec // not a sensitive op
			rr.Value = s.differentValue(t, rr.Value)
		}
		src = append(src, rr)
	}

	// we'll also ask the database to save the value of a key in a bucket that doesn't exist
	src = append(src, nosql.Record{
		Bucket: s.newBucket(t),
		Key:    s.anyKey(t),
		Value:  s.anyValue(t),
	})

	ctx := newContext(t)

	// then we'll put them in the database and expect the command to fail
	require.ErrorIs(t, putMany(ctx, src...), nosql.ErrBucketNotFound)

	// then we'll read back the original existing records and expect no changes to them
	var got []nosql.Record
	for _, r := range exp {
		v, err := s.db.Get(ctx, r.Bucket, r.Key)
		require.NoError(t, err)

		rr := r.Clone()
		rr.Value = v
		got = append(got, rr)
	}

	assert.ElementsMatch(t, exp, got)
}

func (s *suite) testListValidations(t *testing.T) {
	t.Parallel()

	s.testListFuncValidations(t, s.db.List)
}

func (s *suite) testViewerListValidations(t *testing.T) {
	t.Parallel()

	s.testListFuncValidations(t, func(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
		err = s.db.View(ctx, func(v nosql.Viewer) (err error) {
			records, err = v.List(ctx, bucket)

			return
		})

		return
	})
}

func (s *suite) testMutatorListValidations(t *testing.T) {
	t.Parallel()

	s.testListFuncValidations(t, func(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
		err = s.db.Mutate(ctx, func(m nosql.Mutator) (err error) {
			records, err = m.List(ctx, bucket)

			return
		})

		return
	})
}

type listFunc func(ctx context.Context, bucket []byte) ([]nosql.Record, error)

func (s *suite) testListFuncValidations(t *testing.T, list listFunc) {
	t.Helper()

	s.assertBucketError(t, func(t *testing.T, bucket []byte) error {
		_, err := list(newContext(t), bucket)
		return err
	})
}

func (s *suite) testList(t *testing.T) {
	t.Parallel()

	s.testListFunc(t, s.db.List)
}

func (s *suite) testViewerList(t *testing.T) {
	t.Parallel()

	s.testListFunc(t, func(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
		err = s.db.View(ctx, func(v nosql.Viewer) (err error) {
			records, err = v.List(ctx, bucket)

			return
		})
		return
	})
}

func (s *suite) testMutatorList(t *testing.T) {
	t.Parallel()

	s.testListFunc(t, func(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
		err = s.db.Mutate(ctx, func(m nosql.Mutator) (err error) {
			records, err = m.List(ctx, bucket)

			return
		})
		return
	})
}

func (s *suite) testCompoundViewer(t *testing.T) {
	t.Parallel()

	var (
		ctx = newContext(t)

		bucket        = s.existingBucket(t)
		key, expValue = s.existingKey(t, bucket)
	)

	var gotValue []byte
	err := s.db.View(ctx, func(v nosql.Viewer) (err error) {
		var (
			b1 = s.newBucket(t) // bucke that does not exist
			k1 = s.anyKey(t)
		)

		if _, err = v.Get(ctx, b1, k1); !errors.Is(err, nosql.ErrBucketNotFound) {
			panic(err)
		}

		if _, err = v.List(ctx, b1); !errors.Is(err, nosql.ErrBucketNotFound) {
			panic(err)
		}

		gotValue, err = v.Get(ctx, bucket, key)

		return
	})
	require.NoError(t, err)
	require.Equal(t, expValue, gotValue)
}

func (s *suite) testCompoundMutator(t *testing.T) {
	t.Parallel()

	var (
		ctx = newContext(t)

		bucket   = s.existingBucket(t)
		key      = s.newKey(t, bucket)
		value    = s.anyValue(t)
		expValue = s.differentValue(t, value)
	)

	err := s.db.Mutate(ctx, func(m nosql.Mutator) error {
		var (
			b1 = s.newBucket(t) // bucke that does not exist
			k1 = s.anyKey(t)
			v1 = s.anyValue(t)
		)
		if err := m.Put(ctx, b1, k1, v1); !errors.Is(err, nosql.ErrBucketNotFound) {
			panic(err)
		}

		if _, err := m.Get(ctx, b1, k1); !errors.Is(err, nosql.ErrBucketNotFound) {
			panic(err)
		}

		if err := m.CompareAndSwap(ctx, b1, k1, v1, s.differentValue(t, v1)); !errors.Is(err, nosql.ErrBucketNotFound) {
			panic(err)
		}

		if _, err := m.List(ctx, b1); !errors.Is(err, nosql.ErrBucketNotFound) {
			panic(err)
		}

		if err := m.Delete(ctx, b1, k1); !errors.Is(err, nosql.ErrBucketNotFound) {
			panic(err)
		}

		// the above errors shouldn't stop the transaction from going through
		return m.PutMany(ctx,
			nosql.Record{Bucket: bucket, Key: key, Value: value},
			nosql.Record{Bucket: bucket, Key: key, Value: expValue},
		)
	})
	require.NoError(t, err)

	got, err := s.db.Get(ctx, bucket, key)
	require.NoError(t, err)
	require.Equal(t, expValue, got)
}

func (s *suite) testListFunc(t *testing.T, list listFunc) {
	t.Helper()

	var (
		exp     = s.existingRecords(t, 2, 3, 2, 3)
		buckets = map[string][]nosql.Record{}
	)
	for _, r := range exp {
		bucket := string(r.Bucket)

		buckets[bucket] = append(buckets[bucket], r)
	}

	for _, bucket := range buckets {
		slices.SortFunc(bucket, compareRecords)
	}
	buckets[string(s.newBucket(t))] = nil // a bucket that doesn't exist

	ctx := newContext(t)

	for bucket, exp := range buckets {
		got, err := list(ctx, []byte(bucket))
		if exp == nil {
			assert.ErrorIs(t, err, nosql.ErrBucketNotFound)
			assert.Nil(t, got)

			return
		}

		require.NoError(t, err)
		require.Len(t, got, len(exp))
		require.Equal(t, exp, got)
	}
}

func (s *suite) existingRecords(t *testing.T, minBuckets, maxBuckets, minPerBucket, maxPerBucket int) (records []nosql.Record) {
	t.Helper()

	for i := 0; i < minBuckets+mand.Intn(maxBuckets-minBuckets); i++ { //nolint:gosec // not a sensitive op
		bucket := s.existingBucket(t)

		for j := 0; j < minPerBucket+mand.Intn(maxPerBucket-minPerBucket); j++ { //nolint:gosec // not a sensitive op
			var r nosql.Record

			r.Bucket = slices.Clone(bucket)
			r.Key, r.Value = s.existingKey(t, bucket)

			records = append(records, r)
		}
	}

	slices.SortFunc(records, compareRecords)

	return
}

// longBucket returns a bucket that's at least [nosql.MaxBucketSize] + 1 bytes long.
func (*suite) longBucket(t *testing.T) []byte {
	t.Helper()

	return token.New(t, nosql.MaxBucketSize+1, nosql.MaxBucketSize+2, true)
}

// invalidBucket returns a bucket that's invalid.
func (*suite) invalidBucket(t *testing.T) (bucket []byte) {
	t.Helper()

	l := nosql.MinBucketSize + mand.Intn(nosql.MaxBucketSize-nosql.MinBucketSize) //nolint:gosec // not a sensitive op
	bucket = make([]byte, l)

	for {
		_, err := rand.Read(bucket)
		require.NoError(t, err)

		if bytes.IndexByte(bucket, 0) > -1 || !utf8.Valid(bucket) {
			return
		}
	}
}

// existingBucket adds a new bucket to the database and returns it.
func (s *suite) existingBucket(t *testing.T) (bucket []byte) {
	t.Helper()

	bucket = s.newBucket(t)
	require.NoError(t, s.db.CreateBucket(newContext(t), bucket))

	s.generatedMu.Lock()
	s.generated[string(bucket)] = map[string][]byte{}
	s.generatedMu.Unlock()

	return bucket
}

// newBucket returns a bucket that's not in the database.
func (s *suite) newBucket(t *testing.T) (bucket []byte) {
	t.Helper()

	for {
		m := nosql.MinBucketSize
		if mand.Intn(100) < 10 { //nolint:gosec // not a sensitive op
			// ensure 10% of the generated buckets have the longest possible size
			m = nosql.MaxBucketSize
		}

		bucket = token.New(t, m, nosql.MaxBucketSize, true)

		s.generatedMu.Lock()
		if _, ok := s.generated[string(bucket)]; ok {
			s.generatedMu.Unlock()

			continue
		}

		s.generated[string(bucket)] = nil
		s.generatedMu.Unlock()

		return
	}
}

// newKey returns a key that's not in the provided bucket. The provided bucket must already exist.
func (s *suite) newKey(t *testing.T, bucket []byte) (key []byte) {
	t.Helper()

	for {
		key = s.anyKey(t)

		s.generatedMu.Lock()
		b := s.generated[string(bucket)]
		if b == nil {
			s.generatedMu.Unlock()

			panic("bucket does not exist")
		}

		if _, ok := b[string(key)]; ok {
			s.generatedMu.Unlock()

			continue // duplicate key
		}

		s.generated[string(bucket)][string(key)] = nil
		s.generatedMu.Unlock()

		return
	}
}

// anyKey returns a random key
func (*suite) anyKey(t *testing.T) []byte {
	t.Helper()

	m := nosql.MinKeySize
	if mand.Intn(100) < 10 { //nolint:gosec // not a sensitive op
		// ensure that 10% of the generated keys are as long as possible
		m = nosql.MaxKeySize
	}

	return token.New(t, m, nosql.MaxKeySize, false)
}

// longKey returns a key that's at least [nosql.MaxKeySize] + 1 bytes long.
func (*suite) longKey(t *testing.T) []byte {
	t.Helper()

	return token.New(t, nosql.MaxKeySize+1, nosql.MaxKeySize+2, false)
}

// existingKey adds a key (pointing to a random value) to the provided bucket and returns it
// along with the value it generated for it.
func (s *suite) existingKey(t *testing.T, bucket []byte) (key, value []byte) {
	t.Helper()

	key = s.newKey(t, bucket)
	value = s.anyValue(t)

	require.NoError(t, s.db.Put(newContext(t), bucket, key, value))

	s.generatedMu.Lock()
	s.generated[string(bucket)][string(key)] = slices.Clone(value)
	s.generatedMu.Unlock()

	return key, value
}

// anyValue returns a random value
func (*suite) anyValue(t *testing.T) []byte {
	t.Helper()

	m := 1
	if mand.Intn(100) < 10 { //nolint:gosec // not a sensitive op
		// ensure that 10% of the generated values are as long as possible
		m = nosql.MaxValueSize
	}

	return token.New(t, m, nosql.MaxValueSize, false)
}

// longValue returns a value that's at least [nosql.MaxValueSize] + 1 bytes long.
func (*suite) longValue(t *testing.T) []byte {
	t.Helper()

	return token.New(t, nosql.MaxValueSize+1, nosql.MaxValueSize+2, false)
}

// anyValue returns a random value different to the given one
func (s *suite) differentValue(t *testing.T, current []byte) (value []byte) {
	t.Helper()

	for {
		if value = s.anyValue(t); !bytes.Equal(current, value) {
			return
		}
	}
}

func (s *suite) assertBucketError(t *testing.T, fn func(*testing.T, []byte) error) {
	t.Helper()

	cases := [][]byte{
		0: nil,
		1: {},
		2: s.invalidBucket(t),
		3: s.longBucket(t),
	}

	for caseIndex := range [][]byte{} {
		bucket := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			exp := s.bucketError(bucket)
			err := fn(t, bucket)

			assert.ErrorIs(t, err, exp)
		})
	}
}

func (s *suite) assertIDError(t *testing.T, fn func(t *testing.T, bucket, key []byte) error) {
	t.Helper()

	cases := []struct {
		bucket []byte
		key    []byte
	}{
		0: {nil, s.anyKey(t)},
		1: {[]byte{}, s.anyKey(t)},
		2: {s.longBucket(t), s.anyKey(t)},
		3: {s.invalidBucket(t), s.anyKey(t)},

		4: {s.newBucket(t), nil},
		5: {s.newBucket(t), []byte{}},
		6: {s.newBucket(t), s.longKey(t)},
	}

	for caseIndex := range cases {
		kase := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			exp := s.idError(kase.bucket, kase.key)
			err := fn(t, kase.bucket, kase.key)

			assert.ErrorIs(t, err, exp)
		})
	}
}

func (s *suite) assertRecordError(t *testing.T, fn func(t *testing.T, bucket, key, value []byte) error) {
	t.Helper()

	cases := []struct {
		bucket, key, value []byte
	}{
		0: {nil, s.anyKey(t), s.anyValue(t)},
		1: {[]byte{}, s.anyKey(t), s.anyValue(t)},
		2: {s.longBucket(t), s.anyKey(t), s.anyValue(t)},
		3: {s.invalidBucket(t), s.anyKey(t), s.anyValue(t)},

		4: {s.newBucket(t), nil, s.anyValue(t)},
		5: {s.newBucket(t), []byte{}, s.anyValue(t)},
		6: {s.newBucket(t), s.longKey(t), s.anyValue(t)},

		7: {s.newBucket(t), s.anyKey(t), nil},
		8: {s.newBucket(t), s.anyKey(t), s.longValue(t)},
	}

	for caseIndex := range cases {
		kase := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			t.Parallel()

			exp := s.recordError(kase.bucket, kase.key, kase.value)
			got := fn(t, kase.bucket, kase.key, kase.value)
			assert.ErrorIs(t, got, exp)
		})
	}
}

func (s *suite) assertRecordsError(t *testing.T, fn func(*testing.T, ...nosql.Record) error) {
	t.Helper()

	cases := []nosql.Record{
		0: {Bucket: nil, Key: s.anyKey(t), Value: s.anyValue(t)},
		1: {Bucket: []byte{}, Key: s.anyKey(t), Value: s.anyValue(t)},
		2: {Bucket: s.longBucket(t), Key: s.anyKey(t), Value: s.anyValue(t)},
		3: {Bucket: s.invalidBucket(t), Key: s.anyKey(t), Value: s.anyValue(t)},

		4: {Bucket: s.newBucket(t), Key: nil, Value: s.anyValue(t)},
		5: {Bucket: s.newBucket(t), Key: []byte{}, Value: s.anyValue(t)},
		6: {Bucket: s.newBucket(t), Key: s.longKey(t), Value: s.anyValue(t)},

		7: {Bucket: s.newBucket(t), Key: s.anyKey(t), Value: nil},
		8: {Bucket: s.newBucket(t), Key: s.anyKey(t), Value: s.longValue(t)},
	}

	for caseIndex := range cases {
		kase := cases[caseIndex]

		t.Run(strconv.Itoa(caseIndex), func(t *testing.T) {
			records := []nosql.Record{kase}
			for len(records) < 10+mand.Intn(90) { //nolint:gosec // not a sensitive op
				records = append(records, nosql.Record{
					Bucket: s.newBucket(t),
					Key:    s.anyKey(t),
					Value:  s.anyValue(t),
				})
			}
			shuffleRecords(records)

			exp := s.recordsError(records...)
			err := fn(t, records...)

			assert.ErrorIs(t, err, exp)
		})
	}
}

// recordsError returns the value we expect for the given records.
func (s *suite) recordsError(records ...nosql.Record) (err error) {
	for _, r := range records {
		if err = s.recordError(r.Bucket, r.Key, r.Value); err != nil {
			break
		}
	}

	return
}

// recordError returns the value we expect for the given Record particulars.
func (s *suite) recordError(bucket, key, value []byte) (err error) {
	if err = s.idError(bucket, key); err == nil {
		err = s.valueError(value)
	}
	return
}

// idError returns the error we expect for the given ID particulars.
func (s *suite) idError(bucket, key []byte) (err error) {
	if err = s.bucketError(bucket); err == nil {
		err = s.keyError(key)
	}
	return
}

// bucketError returns the error we expect for the given bucket.
func (*suite) bucketError(bucket []byte) error {
	if bucket == nil {
		return nosql.ErrNilBucket
	}
	if l := len(bucket); l == 0 {
		return nosql.ErrEmptyBucket
	} else if l > nosql.MaxBucketSize {
		return nosql.ErrBucketTooLong
	}

	for {
		r, s := utf8.DecodeRune(bucket)
		switch {
		case r == utf8.RuneError && s == 1:
			return nosql.ErrInvalidBucket
		case r < 1, r > '\U0000FFFF':
			return nosql.ErrInvalidBucket
		}

		if bucket = bucket[s:]; len(bucket) == 0 {
			if unicode.IsSpace(r) {
				return nosql.ErrInvalidBucket
			}

			return nil
		}
	}
}

// keyError returns the error we expect for the given key.
func (*suite) keyError(key []byte) error {
	if key == nil {
		return nosql.ErrNilKey
	}
	if l := len(key); l == 0 {
		return nosql.ErrEmptyKey
	} else if l > nosql.MaxKeySize {
		return nosql.ErrKeyTooLong
	}
	return nil
}

// valueError returns the error we expect for the given value.
func (*suite) valueError(value []byte) error {
	if value == nil {
		return nosql.ErrNilValue
	} else if l := len(value); l > nosql.MaxValueSize {
		return nosql.ErrValueTooLong
	}
	return nil
}

func compareRecords(a, b nosql.Record) (comparison int) {
	if comparison = bytes.Compare(a.Bucket, b.Bucket); comparison == 0 {
		if comparison = bytes.Compare(a.Key, b.Key); comparison == 0 {
			comparison = bytes.Compare(a.Value, b.Value)
		}
	}

	return
}

func shuffleRecords(records []nosql.Record) {
	mand.Shuffle(len(records), func(i, j int) {
		records[i], records[j] = records[j], records[i]
	})
}
