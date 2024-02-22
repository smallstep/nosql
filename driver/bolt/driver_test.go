package bolt

import (
	"context"
	mand "math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/smallstep/nosql/dbtest"
)

func Test(t *testing.T) {
	db, err := Open(newContext(t), filepath.Join(t.TempDir(), "bolt.db"))
	require.NoError(t, err)

	dbtest.Test(t, db, false)
}

func TestContextWithOptions(t *testing.T) {
	var (
		exp = new(bbolt.Options)
		got = OptionsFromContext(ContextWithOptions(context.Background(), exp))
	)

	assert.Same(t, exp, got)
}

func TestContextWithOptionsOnContextWithoutOptions(t *testing.T) {
	var (
		exp = &bbolt.Options{
			Timeout:      5 * time.Second,
			NoGrowSync:   false,
			FreelistType: bbolt.FreelistArrayType,
		}
		got = OptionsFromContext(context.Background())
	)

	assert.Equal(t, exp, got)
}

func TestContextWithFileMode(t *testing.T) {
	var (
		exp = os.FileMode(mand.Uint32())
		got = FileModeFromContext(ContextWithFileMode(context.Background(), exp))
	)

	assert.Equal(t, exp, got)
}

func TestContextWithFileModeOnContextWithoutFileMode(t *testing.T) {
	var (
		exp os.FileMode = 0600
		got             = FileModeFromContext(context.Background())
	)

	assert.Equal(t, exp, got)
}

func newContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}
