package postgresql

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/smallstep/nosql/dbtest"
)

func TestDB(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("$TEST_POSTGRES_DSN is missing or empty; test skipped")
	}

	// tear down the test database if it already exists
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	dbName := determineDatabaseName(cfg)
	cfg.Database = "postgres"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(ctx)

	sql := fmt.Sprintf( /* sql */ `
		DROP DATABASE IF EXISTS %s;
	`, quote([]byte(dbName)))

	_, err = conn.Exec(ctx, sql)
	require.NoError(t, err)

	// run the test suite
	db, err := Open(ctx, dsn)
	require.NoError(t, err)

	dbtest.Test(t, db)
}
