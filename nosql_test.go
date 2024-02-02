package nosql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/smallstep/nosql/internal/token"
)

func TestRegisterPanics(t *testing.T) {
	assert.PanicsWithValue(t, "nosql: nil driver", func() {
		Register("driver", nil)
	})
}

func TestRegister(t *testing.T) {
	var (
		gotCtx context.Context
		gotDSN string
	)
	drv := func(ctx context.Context, dsn string) (DB, error) {
		gotCtx = ctx
		gotDSN = dsn

		return nil, assert.AnError
	}

	const (
		name = "testdriver"
		dsn  = "some random dsn"
		val  = "some context value"
	)

	Register(name, drv)
	t.Cleanup(func() { unregister(name) })

	type contextKeyType struct{}
	ctx := context.WithValue(context.Background(), contextKeyType{}, val)

	got, err := Open(ctx, name, dsn)
	assert.Same(t, assert.AnError, err)
	assert.Nil(t, got)
	assert.Equal(t, val, gotCtx.Value(contextKeyType{}).(string))
	assert.Equal(t, dsn, gotDSN)
}

func TestDrivers(t *testing.T) {
	names := []string{
		"testdriver1",
		"testdriver3",
		"testdriver2",
	}
	t.Cleanup(func() {
		for _, name := range names {
			unregister(name)
		}
	})

	for _, name := range names {
		Register(name, func(context.Context, string) (DB, error) {
			return nil, assert.AnError
		})
	}

	assert.Equal(t, []string{
		"testdriver1",
		"testdriver2",
		"testdriver3",
	}, Drivers())
}

func unregister(name string) {
	driversMu.Lock()
	defer driversMu.Unlock()

	delete(drivers, name)
}

func TestRecordClone(t *testing.T) {
	var (
		r = Record{
			Bucket: token.New(t, 0, 50, false),
			Key:    token.New(t, 0, 100, false),
			Value:  token.New(t, 0, 200, false),
		}
		d = r.Clone()
	)

	assert.Equal(t, r, d)
}
