// Package postgresql implements a [nosql.Driver] for Postgres databases.
package postgresql

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"os"
	"os/user"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/smallstep/nosql"
	"github.com/smallstep/nosql/internal/each"
)

func init() {
	nosql.Register("postgresql", Open)
}

// Open implements a [nosql.Driver] for Postgres databases.
func Open(ctx context.Context, dsn string) (nosql.DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	} else if err := setup(ctx, pool.Config().ConnConfig); err != nil {
		pool.Close()

		return nil, err
	}

	return nosql.Constrain(&db{
		pool: pool,
	}), nil
}

func setup(ctx context.Context, cfg *pgx.ConnConfig) (err error) {
	db := determineDatabaseName(cfg)
	cfg.Database = "postgres"

	var conn *pgx.Conn
	if conn, err = pgx.ConnectConfig(ctx, cfg); err != nil {
		return
	}
	defer conn.Close(ctx)

	// check if the database already exists
	const checkSQL = /* sql */ `
		SELECT TRUE
		FROM pg_catalog.pg_database
		WHERE datname = $1;
	`
	var exists bool
	switch err = conn.QueryRow(ctx, checkSQL, db).Scan(&exists); {
	case err == nil:
		return // database exists
	case isNoRows(err):
		break // database does not exist; proceed with creating it
	default:
		return // another error occurred
	}

	createSQL := fmt.Sprintf( /* sql */ `
		CREATE DATABASE %s;
	`, quote(db))

	if _, err = conn.Exec(ctx, createSQL); isPostgresErrorCode(err, pgerrcode.DuplicateDatabase) {
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
	pool *pgxpool.Pool
}

func (db *db) Close(context.Context) error {
	db.pool.Close()
	return nil
}

func (db *db) CreateBucket(ctx context.Context, bucket []byte) error {
	return pgx.BeginTxFunc(ctx, db.pool, readWriteOpts, func(tx pgx.Tx) error {
		// we avoid CREATE TABLE IF NOT EXISTS in case the table is there but
		// the permissions are not

		const checkSQL = /* sql */ `
			SELECT EXISTS (
				SELECT FROM pg_tables
				WHERE schemaname = CURRENT_SCHEMA AND tablename  = $1
			);
		`

		var exists bool
		if err := tx.QueryRow(ctx, checkSQL, bucket).Scan(&exists); err != nil {
			return err
		} else if exists {
			return nil
		}

		createSQL := fmt.Sprintf( /* sql */ `
			CREATE TABLE IF NOT EXISTS %s (
				nkey BYTEA NOT NULL CHECK ( octet_length(nkey) BETWEEN %d AND %d ),
				nvalue BYTEA NOT NULL CHECK ( octet_length(nvalue) <= %d ),
				
				PRIMARY KEY (nkey)
			);
		`, quote(bucket), nosql.MinKeySize, nosql.MaxKeySize, nosql.MaxValueSize)

		_, err := db.pool.Exec(ctx, createSQL)
		return err
	})
}

func (db *db) DeleteBucket(ctx context.Context, bucket []byte) (err error) {
	sql := fmt.Sprintf( /* sql */ `
		DROP TABLE %s;
	`, quote(bucket))

	if _, err = db.pool.Exec(ctx, sql); err != nil && isUndefinedTable(err) {
		err = nosql.ErrBucketNotFound
	}

	return
}

func (db *db) Delete(ctx context.Context, bucket, key []byte) error {
	return del(ctx, db.pool, bucket, key)
}

func (db *db) PutMany(ctx context.Context, records ...nosql.Record) error {
	if len(records) == 0 {
		return nil // save the round trip
	}

	return putMany(ctx, db.pool, records...)
}

func (db *db) Put(ctx context.Context, bucket, key, value []byte) error {
	return put(ctx, db.pool, bucket, key, value)
}

func (db *db) Get(ctx context.Context, bucket, key []byte) ([]byte, error) {
	return get(ctx, db.pool, bucket, key)
}

func (db *db) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
	return cas(ctx, db.pool, bucket, key, oldValue, newValue)
}

func (db *db) List(ctx context.Context, bucket []byte) ([]nosql.Record, error) {
	return list(ctx, db.pool, bucket)
}

var readOnlyOpts = pgx.TxOptions{
	IsoLevel:   pgx.RepeatableRead,
	AccessMode: pgx.ReadOnly,
}

func (db *db) View(ctx context.Context, fn func(nosql.Viewer) error) error {
	return pgx.BeginTxFunc(ctx, db.pool, readOnlyOpts, func(tx pgx.Tx) error {
		return fn(&wrapper{tx})
	})
}

var readWriteOpts = pgx.TxOptions{
	IsoLevel:   pgx.RepeatableRead,
	AccessMode: pgx.ReadWrite,
}

func (db *db) Mutate(ctx context.Context, fn func(nosql.Mutator) error) error {
	return pgx.BeginTxFunc(ctx, db.pool, readWriteOpts, func(tx pgx.Tx) error {
		return fn(&wrapper{tx})
	})
}

type wrapper struct {
	tx pgx.Tx
}

func (w *wrapper) Get(ctx context.Context, bucket, key []byte) (value []byte, err error) {
	// we're using a savepoint as if the table is not defined, the whole
	// of the transaction will be aborted instead of carrying on

	err = w.do(ctx, func(tx pgx.Tx) (err error) {
		value, err = get(ctx, tx, bucket, key)

		return
	})

	return
}

func (w *wrapper) Put(ctx context.Context, bucket, key, value []byte) error {
	// we're using a savepoint as if the table is not defined, the whole
	// of the transaction will be aborted instead of carrying on

	return w.do(ctx, func(tx pgx.Tx) error {
		return put(ctx, tx, bucket, key, value)
	})
}

func (w *wrapper) Delete(ctx context.Context, bucket, key []byte) error {
	// we're using a savepoint as if the table is not defined, the whole
	// of the transaction will be aborted instead of carrying on

	return w.do(ctx, func(tx pgx.Tx) error {
		return del(ctx, tx, bucket, key)
	})
}

func (w *wrapper) CompareAndSwap(ctx context.Context, bucket, key, oldValue, newValue []byte) error {
	// we're using a savepoint as if the table is not defined, the whole
	// of the transaction will be aborted instead of carrying on

	return w.do(ctx, func(tx pgx.Tx) error {
		return cas(ctx, tx, bucket, key, oldValue, newValue)
	})
}

func (w *wrapper) PutMany(ctx context.Context, records ...nosql.Record) error {
	if len(records) == 0 {
		return nil // save the round trip
	}

	// we're using a savepoint as if the table is not defined, the whole
	// of the transaction will be aborted instead of carrying on

	return w.do(ctx, func(tx pgx.Tx) error {
		return putMany(ctx, tx, records...)
	})
}

func (w *wrapper) List(ctx context.Context, bucket []byte) (records []nosql.Record, err error) {
	// we're using a savepoint as if the table is not defined, the whole
	// of the transaction will be aborted instead of carrying on

	err = w.do(ctx, func(tx pgx.Tx) (err error) {
		records, err = list(ctx, tx, bucket)

		return
	})

	return
}

func (w *wrapper) do(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, w.tx, fn)
}

// --- helpers

// generic constraints
type (
	executor interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	}

	querier interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}

	rowQuerier interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}

	batcher interface {
		SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	}
)

func get[RQ rowQuerier](ctx context.Context, rq RQ, bucket, key []byte) (value []byte, err error) {
	sql := fmt.Sprintf( /* sql */ `
		SELECT nvalue
		FROM %s
		WHERE nkey = $1;
	`, quote(bucket))

	switch err = rq.QueryRow(ctx, sql, key).Scan(&value); {
	case isNoRows(err):
		err = nosql.ErrKeyNotFound
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

func put[E executor](ctx context.Context, e E, bucket, key, value []byte) (err error) {
	sql := fmt.Sprintf( /* sql */ `
		INSERT INTO %s (nkey, nvalue)
		VALUES ($1, $2)
		ON CONFLICT(nkey) DO UPDATE SET nvalue = EXCLUDED.nvalue;
	`, quote(bucket))

	if _, err = e.Exec(ctx, sql, key, value); isUndefinedTable(err) {
		err = nosql.ErrBucketNotFound
	}

	return
}

func list[QR querier](ctx context.Context, qr QR, bucket []byte) (records []nosql.Record, err error) {
	sql := fmt.Sprintf( /* sql */ `
		SELECT nkey AS Key, nvalue AS Value
		FROM %s
		ORDER BY nkey;
	`, quote(bucket))

	var rows pgx.Rows
	if rows, err = qr.Query(ctx, sql); err == nil {
		if records, err = pgx.CollectRows(rows, pgx.RowToStructByNameLax[nosql.Record]); err == nil {
			for i := range records {
				records[i].Bucket = slices.Clone(bucket)
			}
		}
	} else if isUndefinedTable(err) {
		err = nosql.ErrBucketNotFound
	}

	return
}

func del[E executor](ctx context.Context, e E, bucket, key []byte) (err error) {
	sql := fmt.Sprintf( /* sql */ `
		DELETE FROM %s
		WHERE nkey = $1;
	`, quote(bucket))

	if _, err = e.Exec(ctx, sql, key); err != nil && isUndefinedTable(err) {
		err = nosql.ErrBucketNotFound
	}

	return
}

func putMany[B batcher](ctx context.Context, b B, records ...nosql.Record) error {
	var (
		seed = maphash.MakeSeed()
		km   = map[uint64][]byte{} // keys map, sum(key) -> key
		vm   = map[uint64][]byte{} // values map, sum(key) -> value

		keys [][]byte // reusable keys buffer
		vals [][]byte // reusable values buffer

		batch pgx.Batch
	)

	_ = each.Bucket(records, func(bucket []byte, rex []*nosql.Record) (_ error) {
		// we can't INSERT ... ON CONFLICT DO UPDATE for the same key more than once so we'll
		// ensure that the last key is the only one sent per bucket
		for _, r := range rex {
			id := maphash.Bytes(seed, r.Key)
			km[id] = r.Key
			vm[id] = r.Value
		}

		for id, key := range km {
			keys = append(keys, key)
			vals = append(vals, vm[id])
		}

		sql := fmt.Sprintf( /* sql */ `
			INSERT INTO %s (nkey, nvalue)
			SELECT *
			FROM unnest($1::BYTEA[], $2::BYTEA[])
			ON CONFLICT(nkey) DO UPDATE SET nvalue = EXCLUDED.nvalue;
		`, quote(bucket))

		// we have to pass clones of the keys and the values
		_ = batch.Queue(sql, slices.Clone(keys), slices.Clone(vals))

		// clear the buffers before the next iteration
		clear(km)
		clear(vm)
		keys = keys[:0]
		vals = vals[:0]

		return
	})

	err := b.SendBatch(ctx, &batch).Close()
	if isUndefinedTable(err) {
		err = nosql.ErrBucketNotFound
	}
	return err
}

func cas[RQ rowQuerier](ctx context.Context, rq RQ, bucket, key, oldValue, newValue []byte) (err error) {
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
	switch err = rq.QueryRow(ctx, sql, key, oldValue, newValue).Scan(&updated, &current); {
	case err == nil:
		if !updated {
			err = &nosql.ComparisonError{
				Value: current,
			}
		}
	case isNoRows(err):
		err = nosql.ErrKeyNotFound
	case isUndefinedTable(err):
		err = nosql.ErrBucketNotFound
	}

	return
}

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

func isRace(err error) bool {
	return isPostgresErrorCode(err, pgerrcode.DeadlockDetected, pgerrcode.SerializationFailure)
}

func isPostgresErrorCode(err error, codes ...string) (is bool) {
	var pe *pgconn.PgError
	if is = errors.As(err, &pe); is {
		for _, code := range codes {
			if is = pe.Code == code; is {
				break
			}
		}
	}

	return
}
