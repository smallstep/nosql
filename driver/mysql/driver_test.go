package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smallstep/nosql/dbtest"
)

func Test(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"MySQL", "MariaDB"} {
		name := name

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			key := fmt.Sprintf("TEST_%s_DSN", strings.ToUpper(name))
			dsn := os.Getenv(key)
			if dsn == "" {
				t.Skipf("$%s is missing or empty; test skipped", key)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)

			dropTestDatabase(ctx, t, dsn)

			db, err := Open(ctx, dsn)
			require.NoError(t, err)

			dbtest.Test(t, db)
		})
	}
}

func dropTestDatabase(ctx context.Context, t *testing.T, dsn string) {
	t.Helper()

	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)

	cfg = cfg.Clone()
	dbName := cfg.DBName
	cfg.DBName = ""

	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	query := fmt.Sprintf( /* sql */ `
		DROP DATABASE IF EXISTS %s;
	`, quote(dbName))

	_, err = db.ExecContext(ctx, query)
	assert.NoError(t, err)
}
