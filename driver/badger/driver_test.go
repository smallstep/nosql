package badger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	badgerv1 "github.com/dgraph-io/badger"
	badgerv2 "github.com/dgraph-io/badger/v2"
	badgerv3 "github.com/dgraph-io/badger/v3"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smallstep/nosql"
	"github.com/smallstep/nosql/dbtest"
)

var versions = map[string]nosql.Driver{
	"Default": Open,
	"V1":      OpenV1,
	"V2":      OpenV2,
	"V3":      OpenV3,
	"V4":      OpenV4,
}

func Test(t *testing.T) {
	t.Parallel()

	for v := range versions {
		open := versions[v]

		t.Run(v, func(t *testing.T) {
			t.Parallel()

			db, err := open(context.Background(), t.TempDir())
			require.NoError(t, err)

			dbtest.Test(t, db)
		})
	}
}

func TestCompactByFactor(t *testing.T) {
	t.Parallel()

	for v := range versions {
		open := versions[v]

		t.Run(v, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			db, err := open(ctx, t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, db.Close(ctx)) })

			assert.ErrorIs(t, db.(nosql.CompactedByFactor).CompactByFactor(ctx, 0.7), ErrNoRewrite)
		})
	}
}

func TestOpenExisting(t *testing.T) {
	t.Parallel()

	cases := map[string]string{}

	dir1 := t.TempDir()
	db1, err := badgerv1.Open(badgerv1.DefaultOptions(dir1))
	require.NoError(t, err)
	require.NoError(t, db1.Close())
	cases["v1"] = dir1

	dir2 := t.TempDir()
	db2, err := badgerv2.Open(badgerv2.DefaultOptions(dir2))
	require.NoError(t, err)
	require.NoError(t, db2.Close())
	cases["v2"] = dir2

	dir3 := t.TempDir()
	db3, err := badgerv3.Open(badgerv3.DefaultOptions(dir3))
	require.NoError(t, err)
	require.NoError(t, db3.Close())
	cases["v3"] = dir3

	dir4 := t.TempDir()
	db4, err := badgerv4.Open(badgerv4.DefaultOptions(dir4))
	require.NoError(t, err)
	require.NoError(t, db4.Close())
	cases["v4"] = dir4

	for v := range cases {
		dir := cases[v]

		t.Run(v, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db, err := Open(ctx, dir)
			require.NoError(t, err)
			require.NoError(t, db.Close(ctx))
		})
	}
}

func TestOpenNew(t *testing.T) {
	var (
		ctx = context.Background()
		dir = t.TempDir()
	)

	db, err := Open(ctx, dir)
	require.NoError(t, err)
	initDB(ctx, t, db)
	require.NoError(t, db.Close(ctx))

	db, err = OpenV4(ctx, dir)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close(ctx)) })

	checkDB(ctx, t, db)
}

func initDB(ctx context.Context, t *testing.T, db nosql.DB) {
	t.Helper()

	require.NoError(t, db.CreateBucket(ctx, []byte{1}))
	require.NoError(t, db.Put(ctx, []byte{1}, []byte{2}, []byte{3}))
}

func checkDB(ctx context.Context, t *testing.T, db nosql.DB) {
	t.Helper()

	got, err := db.Get(ctx, []byte{1}, []byte{2})
	require.NoError(t, err)
	require.Equal(t, []byte{3}, got)
}

func TestLogger(t *testing.T) {
	t.Parallel()

	for v := range versions {
		open := versions[v]

		t.Run(v, func(t *testing.T) {
			t.Parallel()

			l, b := newLogger(t)

			ctx := ContextWithOptions(context.Background(), Options{
				Logger: l,
			})
			db, err := open(ctx, t.TempDir())
			require.NoError(t, err)
			require.NoError(t, db.Close(ctx))

			assert.NotEmpty(t, b.String())
		})
	}
}

func newLogger(t *testing.T) (l Logger, b *bytes.Buffer) {
	t.Helper()

	b = new(bytes.Buffer)
	l = &testLogger{b}

	return
}

type testLogger struct {
	w io.Writer
}

func (l *testLogger) Errorf(format string, v ...any) {
	_, _ = fmt.Fprintf(l.w, format, v...)
}

func (l *testLogger) Warningf(format string, v ...any) {
	_, _ = fmt.Fprintf(l.w, format, v...)
}

func (l *testLogger) Infof(format string, v ...any) {
	_, _ = fmt.Fprintf(l.w, format, v...)
}

func (l *testLogger) Debugf(format string, v ...any) {
	_, _ = fmt.Fprintf(l.w, format, v...)
}
