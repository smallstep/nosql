package postgresql

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smallstep/nosql/dbtest"
)

func Test(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("$TEST_POSTGRES_DSN is missing or empty; test skipped")
	}

	// tear down the test database if it already exists
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dropTestDatabase(ctx, t, dsn)

	db, err := Open(ctx, dsn)
	require.NoError(t, err)

	dbtest.Test(t, db, true)
}

func dropTestDatabase(ctx context.Context, t *testing.T, dsn string) {
	t.Helper()

	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)

	connConfig := cfg.ConnConfig.Copy()
	dbName := determineDatabaseName(connConfig)
	connConfig.Database = "postgres"

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close(ctx)) })

	sql := fmt.Sprintf( /* sql */ `
		DROP DATABASE IF EXISTS %s;
	`, quote([]byte(dbName)))

	_, err = conn.Exec(ctx, sql)
	require.NoError(t, err)
}
