// Package mysql implements a [nosql.Driver] for MySQL databases.
package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/smallstep/nosql"
	"github.com/smallstep/nosql/internal/each"
)

// Open implements a [nosql.Driver] for MySQL databases.
func Open(ctx context.Context, dsn string) (nosql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	if err := setup(ctx, cfg); err != nil {
		return nil, err
	}

	mdb, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return nosql.Constrain(&db{
		pool: mdb,
	}), nil
}

// setup ensures that the database exists
func setup(ctx context.Context, cfg *mysql.Config) (err error) {
	cfg = cfg.Clone() // work on a clone of the config

	dbName := cfg.DBName
	cfg.DBName = ""

	var db *sql.DB
	if db, err = sql.Open("mysql", cfg.FormatDSN()); err != nil {
		return err
	}
	defer func() {
		if e := db.Close(); err == nil {
			err = e
		}
	}()

	const checkSQL = /* sql */ `
		SELECT TRUE
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME = ?
	`

	var exists bool
	switch err = db.QueryRowContext(ctx, checkSQL, dbName).Scan(&exists); {
	case err == nil:
		return // database exists
	case isNoRows(err):
		// database does not exist; create it
		createSQL := fmt.Sprintf( /* sql */ `
			CREATE DATABASE IF NOT EXISTS %s;
		`, quote(dbName))

		_, err = db.ExecContext(ctx, createSQL)
	}

	return
}

type db struct {
	pool *sql.DB
}

func (db *db) Close(context.Context) error {
	return db.pool.Close()
}

func (db *db) tx(ctx context.Context, opts *sql.TxOptions, fn func(tx *sql.Tx) error) (err error) {
	var conn *sql.Conn
	if conn, err = db.pool.Conn(ctx); err != nil {
		return
	}
	defer conn.Close()

	var tx *sql.Tx
	if tx, err = conn.BeginTx(ctx, opts); err != nil {
		return
	}

	if err = fn(tx); err != nil {
		_ = tx.Rollback()
	} else {
		err = tx.Commit()
	}

	return
}

func (db *db) CreateBucket(ctx context.Context, bucket []byte) error {
	table := quote(bucket)

	const checkQuery = /* sql */ `
		SELECT TRUE
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND table_name = ?;
	`

	var exists bool
	if err := db.pool.QueryRowContext(ctx, checkQuery, table).Scan(&exists); !isNoRows(err) {
		return err // either nil (which means the table is there) or another error
	}

	createQuery := fmt.Sprintf( /* sql */ `
		CREATE TABLE IF NOT EXISTS %s (
			nkey VARBINARY(%d) PRIMARY KEY NOT NULL CHECK ( octet_length(nkey) >= %d ),
			nvalue BLOB NOT NULL CHECK ( octet_length(nvalue) <= %d )
		);
	`, table, nosql.MaxKeySize, nosql.MinKeySize, nosql.MaxValueSize)

	_, err := db.pool.ExecContext(ctx, createQuery)

	return err
}

func (db *db) DeleteBucket(ctx context.Context, bucket []byte) (err error) {
	query := fmt.Sprintf( /* sql */ `
		DROP TABLE %s;
	`, quote(bucket))

	switch _, err = db.pool.ExecContext(ctx, query); {
	case err == nil:
		break
	case isTableNotFound(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func (db *db) Delete(ctx context.Context, bucket, key []byte) error {
	return del(ctx, db.pool, bucket, key)
}

func (db *db) PutMany(ctx context.Context, records ...nosql.Record) error {
	return db.Mutate(ctx, func(m nosql.Mutator) error {
		return m.PutMany(ctx, records...)
	})
}

func (db *db) Put(ctx context.Context, bucket, key, value []byte) error {
	return put(ctx, db.pool, bucket, key, value)
}

func (db *db) Get(ctx context.Context, bucket, key []byte) ([]byte, error) {
	return get(ctx, db.pool, bucket, key)
}

func (db *db) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
	return db.Mutate(ctx, func(m nosql.Mutator) error {
		return m.CompareAndSwap(ctx, bucket, key, oldValue, newValue)
	})
}

func (db *db) List(ctx context.Context, bucket []byte) ([]nosql.Record, error) {
	return list(ctx, db.pool, bucket)
}

var viewOpts = sql.TxOptions{
	Isolation: sql.LevelRepeatableRead,
	ReadOnly:  true,
}

func (db *db) View(ctx context.Context, fn func(nosql.Viewer) error) (err error) {
	if err = db.tx(ctx, &viewOpts, func(tx *sql.Tx) error {
		return fn(&wrapper{db, tx})
	}); err != nil && isRace(err) {
		err = nosql.ErrRace
	}

	return
}

var mutationOpts = sql.TxOptions{
	Isolation: sql.LevelRepeatableRead,
	ReadOnly:  false,
}

func (db *db) Mutate(ctx context.Context, fn func(nosql.Mutator) error) (err error) {
	if err = db.tx(ctx, &mutationOpts, func(tx *sql.Tx) error {
		return fn(&wrapper{db, tx})
	}); err != nil && isRace(err) {
		err = nosql.ErrRace
	}
	return
}

type wrapper struct {
	db *db
	tx *sql.Tx
}

func (w *wrapper) Get(ctx context.Context, bucket, key []byte) ([]byte, error) {
	return get(ctx, w.tx, bucket, key)
}

func (w *wrapper) Put(ctx context.Context, bucket, key, value []byte) error {
	return put(ctx, w.tx, bucket, key, value)
}

func (w *wrapper) Delete(ctx context.Context, bucket, key []byte) (err error) {
	return del(ctx, w.tx, bucket, key)
}

func (w *wrapper) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
	var (
		table = quote(bucket)

		query = fmt.Sprintf( /* sql */ `
			UPDATE %s SET
				nvalue = ?
			WHERE nkey = ? AND nvalue = ?;
		`, table)
	)

	if ret, err := w.tx.ExecContext(ctx, query, newValue, key, oldValue); err != nil {
		if isTableNotFound(err) {
			err = nosql.ErrBucketNotFound
		}

		return err
	} else if rowsAffected, err := ret.RowsAffected(); err != nil {
		return err
	} else if rowsAffected > 0 {
		return nil // the row was updated
	}

	// the update didn't happen; grab the rows earlier version (if any)

	query = fmt.Sprintf( /* sql */ `
		SELECT nvalue
		FROM %s
		WHERE nkey = ?;
	`, table)

	var current []byte
	if err := w.tx.QueryRowContext(ctx, query, key).Scan(&current); err != nil {
		if isNoRows(err) {
			err = nosql.ErrKeyNotFound
		}

		return err
	} else if bytes.Equal(current, oldValue) {
		// the update didn't happen because the old value is the same with the new value
		return nil
	}

	return &nosql.ComparisonError{
		Value: current,
	}
}

func (w *wrapper) PutMany(ctx context.Context, records ...nosql.Record) error {
	if len(records) == 0 {
		return nil // save the round trip
	}

	var keysAndVals []any // reusable keys/value buffer

	return each.Bucket(records, func(bucket []byte, rex []*nosql.Record) error {
		for _, r := range rex {
			keysAndVals = append(keysAndVals, r.Key, r.Value)
		}

		var (
			suffix = strings.TrimSuffix(strings.Repeat("(?, ?), ", len(keysAndVals)>>1), ", ")

			query = fmt.Sprintf( /* sql */ `
				INSERT INTO %s (nkey, nvalue)
				VALUES %s
				ON DUPLICATE KEY UPDATE nvalue = VALUES(nvalue);
			`, quote(bucket), suffix)
		)

		if _, err := w.tx.ExecContext(ctx, query, keysAndVals...); err != nil {
			if isTableNotFound(err) {
				err = nosql.ErrBucketNotFound
			}

			return err
		}

		keysAndVals = keysAndVals[:0]

		return nil
	})
}

func (w *wrapper) List(ctx context.Context, bucket []byte) ([]nosql.Record, error) {
	return list(ctx, w.tx, bucket)
}

// --- helpers

// generic constraints
// generic constraints
type (
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}

	querier interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}

	rowQuerier interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}
)

func get[RQ rowQuerier](ctx context.Context, rq RQ, bucket, key []byte) (value []byte, err error) {
	query := fmt.Sprintf( /* sql */ `
		SELECT nvalue
		FROM %s
		WHERE nkey = ?;
	`, quote(bucket))

	switch err = rq.QueryRowContext(ctx, query, key).Scan(&value); {
	case isNoRows(err):
		err = nosql.ErrKeyNotFound
	case isTableNotFound(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func list[Q querier](ctx context.Context, q Q, bucket []byte) ([]nosql.Record, error) {
	query := fmt.Sprintf( /* sql */ `
		SELECT nkey, nvalue
		FROM %s
		ORDER BY nkey;
	`, quote(bucket))

	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		if isTableNotFound(err) {
			err = nosql.ErrBucketNotFound
		}

		return nil, err
	}
	defer rows.Close()

	var records []nosql.Record
	var key, value sql.RawBytes
	for rows.Next() {
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}

		records = append(records, nosql.Record{
			Bucket: slices.Clone(bucket),
			Key:    slices.Clone(key),
			Value:  slices.Clone(value),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func del[E executor](ctx context.Context, e E, bucket, key []byte) (err error) {
	query := fmt.Sprintf( /* sql */ `
		DELETE FROM %s
		WHERE nkey = ?;
	`, quote(bucket))

	if _, err = e.ExecContext(ctx, query, key); err != nil && isTableNotFound(err) {
		err = nosql.ErrBucketNotFound
	}

	return
}

func put[E executor](ctx context.Context, e E, bucket, key, value []byte) (err error) {
	query := fmt.Sprintf( /* sql */ `
		INSERT INTO %s ( nkey, nvalue )
		VALUES ( ?, ? )
		ON DUPLICATE KEY UPDATE nvalue = VALUES(nvalue);
	`, quote(bucket))

	if _, err = e.ExecContext(ctx, query, key, value); err != nil && isTableNotFound(err) {
		err = nosql.ErrBucketNotFound
	}

	return
}

func quote[T ~string | ~[]byte](id T) string {
	var sb strings.Builder
	sb.Grow(2*len(id) + 2)

	sb.WriteByte('`')

	for i := 0; i < len(id); i++ {
		c := id[i]

		if c == '`' {
			sb.WriteByte(c)
		}
		sb.WriteByte(c)
	}

	sb.WriteByte('`')

	return sb.String()
}

func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

func isTableNotFound(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) &&
		(me.Number == 1051 || me.Number == 1146)
}

func isRace(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1213
}
