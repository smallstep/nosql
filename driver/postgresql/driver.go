// Package postgresql implements a [nosql.Driver] for Postgres databases.
package postgresql

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/smallstep/nosql"
)

func init() {
	nosql.Register("postgresql", Open)
}

// Open implements a [nosql.Driver] for Postgres databases.
func Open(ctx context.Context, dsn string) (nosql.DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		if !isPostgresErrorCode(err, pgerrcode.InvalidCatalogName) {
			return nil, err
		}

		// attempt to create the catalog, since it does not already exist
		if err := createCatalog(ctx, pool); err != nil {
			return nil, err
		}
	}

	return nosql.Constrain(&db{
		p: pool,
		w: &wrapper[*pgxpool.Pool]{ds: pool},
	}), nil
}

func createCatalog(ctx context.Context, pool *pgxpool.Pool) error {
	// we'll be working on a new connection, so get a copy of the config we can mutate
	cfg := pool.Config().ConnConfig
	db := determineDatabaseName(cfg)
	cfg.Database = "postgres"

	var conn *pgx.Conn
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	sql := fmt.Sprintf( /* sql */ `
		CREATE DATABASE %s;
	`, quote(db))

	if _, err = conn.Exec(ctx, sql); isPostgresErrorCode(err, pgerrcode.DuplicateDatabase) {
		err = nil // the database was created while we were also trying to create it
	}

	return err
}

func determineDatabaseName(cfg *pgx.ConnConfig) (db string) {
	if db = cfg.Database; db == "" {
		db = os.Getenv("PGDATABASE")
	}
	if db == "" {
		db = cfg.User
	}
	if db == "" {
		if u, err := user.Current(); err == nil {
			db = u.Username
		}
	}
	return
}

type db struct {
	p *pgxpool.Pool
	w *wrapper[*pgxpool.Pool]
}

func (db *db) Close(context.Context) error {
	db.p.Close()
	return nil
}

func (db *db) CreateBucket(ctx context.Context, bucket []byte) error {
	sql := fmt.Sprintf( /* sql */ `
		CREATE TABLE IF NOT EXISTS %s (
			nkey BYTEA NOT NULL CHECK ( octet_length(nkey) BETWEEN %d AND %d ),
			nvalue BYTEA NOT NULL CHECK ( octet_length(nvalue) <= %d ),
			
			PRIMARY KEY (nkey)
		);
	`, quote(bucket), nosql.MinKeySize, nosql.MaxKeySize, nosql.MaxValueSize)

	_, err := db.p.Exec(ctx, sql)

	return err
}

func (db *db) DeleteBucket(ctx context.Context, bucket []byte) (err error) {
	sql := fmt.Sprintf( /* sql */ `
		DROP TABLE %s;
	`, quote(bucket))

	switch _, err = db.p.Exec(ctx, sql); {
	case err == nil:
		break
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func (db *db) Delete(ctx context.Context, bucket, key []byte) error {
	return db.Mutate(ctx, func(m nosql.Mutator) error {
		return m.Delete(ctx, bucket, key)
	})
}

func (db *db) PutMany(ctx context.Context, records ...nosql.Record) error {
	return db.Mutate(ctx, func(m nosql.Mutator) error {
		return m.PutMany(ctx, records...)
	})
}

func (db *db) Put(ctx context.Context, bucket, key, value []byte) error {
	return db.Mutate(ctx, func(m nosql.Mutator) error {
		return m.Put(ctx, bucket, key, value)
	})
}

func (db *db) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	err = db.View(ctx, func(v nosql.Viewer) (err error) {
		value, err = v.Get(ctx, bucket, key)
		return
	})
	return
}

var readOnlyOpts = pgx.TxOptions{
	IsoLevel:   pgx.RepeatableRead,
	AccessMode: pgx.ReadOnly,
}

func (db *db) View(ctx context.Context, fn func(nosql.Viewer) error) error {
	return pgx.BeginTxFunc(ctx, db.p, readOnlyOpts, func(tx pgx.Tx) error {
		return fn(&wrapper[pgx.Tx]{
			ds: tx,
		})
	})
}

var readWriteOpts = pgx.TxOptions{
	IsoLevel:   pgx.RepeatableRead,
	AccessMode: pgx.ReadWrite,
}

func (db *db) Mutate(ctx context.Context, fn func(nosql.Mutator) error) error {
	return pgx.BeginTxFunc(ctx, db.p, readWriteOpts, func(tx pgx.Tx) error {
		return fn(&wrapper[pgx.Tx]{
			ds: tx,
		})
	})
}

type dataSource interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

type wrapper[DS dataSource] struct {
	ds dataSource
}

func (w *wrapper[DS]) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	sql := fmt.Sprintf( /* sql */ `
		SELECT nvalue
		FROM %s
		WHERE nkey = $1;
	`, quote(bucket))

	switch err = w.ds.QueryRow(ctx, sql, key).Scan(&value); {
	case isNoRows(err):
		err = nosql.ErrKeyNotFound
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func (w *wrapper[DS]) Put(ctx context.Context, bucket, key, value []byte) (err error) {
	sql := insertQuery(bucket)

	switch _, err = w.ds.Exec(ctx, sql, key, value); {
	case err == nil:
		break
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func insertQuery(bucket []byte) string {
	return fmt.Sprintf( /* sql */ `
		INSERT INTO %s ( nkey, nvalue )
		VALUES ( $1, $2 )
		ON CONFLICT ( nkey ) DO UPDATE SET nvalue = $2;
	`, quote(bucket))
}

func (w *wrapper[DS]) Delete(ctx context.Context, bucket, key []byte) (err error) {
	sql := fmt.Sprintf( /* sql */ `
		DELETE FROM %s
		WHERE nkey = $1
		RETURNING TRUE;
	`, quote(bucket))

	var deleted bool
	switch err = w.ds.QueryRow(ctx, sql, key).Scan(&deleted); {
	case err == nil:
		break
	case isNoRows(err):
		err = nil
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func (w *wrapper[DS]) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) (err error) {
	table := quote(bucket)

	sql := fmt.Sprintf( /* sql */ `
		WITH current AS (
			SELECT
				nvalue
			FROM %s 
			WHERE nkey = $1
		), updated AS (
			UPDATE %s SET
				nvalue = $3
			WHERE nkey = $1 AND nvalue = $2
			RETURNING TRUE AS updated
		)
		SELECT 
			COALESCE(u.updated, FALSE),
			c.nvalue
		FROM current c
		LEFT JOIN updated u ON TRUE;
	`, table, table)

	var updated bool
	var current []byte
	switch err = w.ds.QueryRow(ctx, sql, key, oldValue, newValue).Scan(&updated, &current); {
	case err == nil:
		if !updated {
			err = &nosql.ComparisonError{
				Value: current,
			}
		}
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	case isNoRows(err):
		err = nosql.ErrKeyNotFound
	}

	return
}

func (w *wrapper[DS]) PutMany(ctx context.Context, records ...nosql.Record) (err error) {
	if len(records) == 0 {
		return // save the round trip
	}

	// TODO: this may be optimized

	var b pgx.Batch
	for _, r := range records {
		sql := insertQuery(r.Bucket)

		_ = b.Queue(sql, r.Key, r.Value)
	}

	switch err = w.ds.SendBatch(ctx, &b).Close(); {
	case err == nil:
		break
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func (w *wrapper[DS]) List(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
	sql := fmt.Sprintf( /* sql */ `
		SELECT
			nkey AS Key,
			nvalue AS Value
		FROM %s
		ORDER BY nkey;
	`, quote(bucket))

	var rows pgx.Rows
	switch rows, err = w.ds.Query(ctx, sql); {
	case err == nil:
		if records, err = pgx.CollectRows(rows, pgx.RowToStructByNameLax[nosql.Record]); err == nil {
			for i := range records {
				records[i].Bucket = slices.Clone(bucket)
			}
		}
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func (db *db) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
	return db.Mutate(ctx, func(m nosql.Mutator) error {
		return m.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
	})
}

func (db *db) List(ctx context.Context, bucket []byte) (kvs []nosql.Record, err error) {
	err = db.View(ctx, func(v nosql.Viewer) (err error) {
		kvs, err = v.List(ctx, bucket)

		return
	})
	return
}

// --- helpers

func quote[T ~string | ~[]byte](id T) string {
	var sb strings.Builder
	sb.Grow(2*len(id) + 2)

	sb.WriteByte('"')

	for i := 0; i < len(id); i++ {
		c := id[i]

		if c == '"' {
			sb.WriteByte(c)
		}
		sb.WriteByte(c)
	}

	sb.WriteByte('"')

	return sb.String()
}

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func isUndefinedTable(err error) bool {
	return isPostgresErrorCode(err, pgerrcode.UndefinedTable)
}

func isPostgresErrorCode(err error, code string) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == code
}
