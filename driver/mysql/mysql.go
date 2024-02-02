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
)

// Open implements a [nosql.Driver] for MySQL databases.
func Open(ctx context.Context, dsn string) (nosql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	if err := createDatabaseIfRequired(ctx, cfg); err != nil {
		return nil, err
	}

	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}

	return nosql.Constrain(&db{
		pool: sql.OpenDB(conn),
	}), nil
}

// createDatabaseIfRequired ensures that the database exists
func createDatabaseIfRequired(ctx context.Context, cfg *mysql.Config) (err error) {
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

	query := fmt.Sprintf( /* sql */ `CREATE DATABASE IF NOT EXISTS %s;`, quote(dbName))

	_, err = db.ExecContext(ctx, query)

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
	query := fmt.Sprintf( /* sql */ `
		CREATE TABLE IF NOT EXISTS %s (
			nkey VARBINARY(%d) PRIMARY KEY NOT NULL CHECK ( octet_length(nkey) >= %d ),
			nvalue BLOB NOT NULL CHECK ( octet_length(nvalue) <= %d )
		);
	`, quote(bucket), nosql.MaxKeySize, nosql.MinKeySize, nosql.MaxValueSize)

	_, err := db.pool.ExecContext(ctx, query)

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

var viewOpts = sql.TxOptions{
	Isolation: sql.LevelRepeatableRead,
	ReadOnly:  true,
}

func (db *db) View(ctx context.Context, fn func(nosql.Viewer) error) (err error) {
	return db.tx(ctx, &viewOpts, func(tx *sql.Tx) error {
		return fn(&wrapper{db, tx})
	})
}

var mutationOpts = sql.TxOptions{
	Isolation: sql.LevelRepeatableRead,
	ReadOnly:  false,
}

func (db *db) Mutate(ctx context.Context, fn func(nosql.Mutator) error) error {
	return db.tx(ctx, &mutationOpts, func(tx *sql.Tx) error {
		return fn(&wrapper{db, tx})
	})
}

type wrapper struct {
	db *db
	tx *sql.Tx
}

func (w *wrapper) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	//nolint:gosec // we're escaping the table
	query := fmt.Sprintf( /* sql */ `
		SELECT nvalue
		FROM %s
		WHERE nkey = ?;
	`, quote(bucket))

	switch err = w.tx.QueryRowContext(ctx, query, key).Scan(&value); {
	case isNoRows(err):
		err = nosql.ErrKeyNotFound
	case isTableNotFound(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func (w *wrapper) Put(ctx context.Context, bucket, key, value []byte) (err error) {
	//nolint:gosec // we're escaping the table
	query := fmt.Sprintf( /* sql */ `
		INSERT INTO %s ( nkey, nvalue )
		VALUES ( ?, ? )
		ON DUPLICATE KEY UPDATE nvalue = VALUES(nvalue);
	`, quote(bucket))

	if _, err = w.tx.ExecContext(ctx, query, key, value); err != nil && isTableNotFound(err) {
		err = nosql.ErrBucketNotFound
	}

	return
}

func (w *wrapper) Delete(ctx context.Context, bucket, key []byte) (err error) {
	//nolint:gosec // we're escaping the table
	query := fmt.Sprintf( /* sql */ `
		DELETE FROM %s
		WHERE nkey = ?;
	`, quote(bucket))

	if _, err = w.tx.ExecContext(ctx, query, key); err != nil && isTableNotFound(err) {
		err = nosql.ErrBucketNotFound
	}

	return
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

func (w *wrapper) PutMany(ctx context.Context, records ...nosql.Record) (err error) {
	// TODO: this can be optimized

	for _, r := range records {
		if err = w.Put(ctx, r.Bucket, r.Key, r.Value); err != nil {
			break
		}
	}

	return
}

func (w *wrapper) List(ctx context.Context, bucket []byte) ([]nosql.Record, error) {
	//nolint:gosec // we're escape the table name
	query := fmt.Sprintf( /* sql */ `
		SELECT
			nkey,
			nvalue
		FROM %s
		ORDER BY nkey;
	`, quote(bucket))

	rows, err := w.tx.QueryContext(ctx, query)
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

// --- helpers

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
