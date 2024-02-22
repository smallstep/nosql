// Package dbtest implements a test suite for [nosql.DB] implementations.
package dbtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/smallstep/nosql"
)

// Test tests the provided [nosql.DB].
//
// The provided [nosql.DB] will be closed before Run returns.
//
// The given races flag denotes whether the given [nosql.DB] implementation should be tested
// for race-y conditions.
func Test(t *testing.T, db nosql.DB) {
	t.Helper()

	t.Cleanup(func() {
		assert.NoError(t, db.Close(newContext(t)))
	})

	s := &suite{
		db:        db,
		generated: map[string]map[string][]byte{},
	}

	tests := map[string]func(*testing.T){
		"CreateBucketValidations": s.testCreateBucketValidations,
		"CreateBucket":            s.testCreateBucket,
		"DeleteBucketValidations": s.testDeleteBucketValidations,
		"DeleteBucket":            s.testDeleteBucket,

		"GetValidations":        s.testGetValidations,
		"Get":                   s.testGet,
		"ViewerGetValidations":  s.testViewerGetValidations,
		"ViewerGet":             s.testViewerGet,
		"MutatorGetValidations": s.testMutatorGetValidations,
		"MutatorGet":            s.testMutatorGet,

		"DeleteConstraints":        s.testDeleteValidations,
		"Delete":                   s.testDelete,
		"MutatorDeleteConstraints": s.testMutatorDeleteValidations,
		"MutatorDelete":            s.testMutatorDelete,

		"PutValidations":        s.testPutValidations,
		"Put":                   s.testPut,
		"MutatorPutValidations": s.testMutatorPutValidations,
		"MutatorPut":            s.testMutatorPut,

		"CompareAndSwapValidations":        s.testCompareAndSwapValidations,
		"MutatorCompareAndSwapValidations": s.testMutatorCompareAndSwapValidations,
		"CompareAndSwap":                   s.testCompareAndSwap,
		"MutatorCompareAndSwap":            s.testMutatorCompareAndSwap,

		"PutManyValidations":        s.testPutManyValidations,
		"MutatorPutManyValidations": s.testMutatorPutManyValidations,
		"PutMany":                   s.testPutMany,
		"MutatorPutMany":            s.testMutatorPutMany,
		"PutManyError":              s.testPutManyError,
		"MutatorPutManyError":       s.testMutatorPutManyError,

		"ListValidations":        s.testListValidations,
		"List":                   s.testList,
		"ViewerListValidations":  s.testViewerListValidations,
		"ViewerList":             s.testViewerList,
		"MutatorListValidations": s.testMutatorListValidations,
		"MutatorList":            s.testMutatorList,

		"CompoundViewer":  s.testCompoundViewer,
		"CompoundMutator": s.testCompoundMutator,
	}

	for name, test := range tests {
		t.Run(name, test)
	}
}

func newContext(t *testing.T) (ctx context.Context) {
	t.Helper()

	var cancel context.CancelFunc
	if dl, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(context.Background(), dl)
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	}
	t.Cleanup(cancel)

	return
}
