//go:build !nomongo
// +build !nomongo

package mongo

import (
	"bytes"
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/smallstep/nosql/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// DB is a wrapper over *sql.DB,
type DB struct {
	db *mongo.Database
}

type Tuple struct {
	Key   []byte `bson:"key" json:"key"`
	Value []byte `bson:"value" json:"value"`
}

// Open creates a Driver and connects to the database with the given address
// and access details.
func (db *DB) Open(uri string, opt ...database.Option) error {
	opts := &database.Options{}
	for _, o := range opt {
		if err := o(opts); err != nil {
			return err
		}
	}

	clientOptions := options.Client().ApplyURI(uri)
	if rs := clientOptions.ReplicaSet; *rs == "" {
		return errors.New("To enable transactions, please provide a replica set name")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return errors.Wrap(err, "error in configuration")
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		return errors.Wrap(err, "error connecting to mongo")
	}

	dbOpts := options.Database()
	db.db = client.Database(opts.Database, dbOpts)

	return nil
}

func (db *DB) Close() error {
	if err := db.db.Client().Disconnect(context.Background()); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (db *DB) CreateTable(bucket []byte) error {
	return db.createTable(bucket, context.Background())
}

func (db *DB) DeleteTable(bucket []byte) error {
	return db.deleteTable(bucket, context.Background())
}

// Get returns the value stored in the given bucked and key.
func (db *DB) Get(bucket, key []byte) (ret []byte, err error) {
	return db.get(bucket, key, context.Background())
}

// Set stores the given value on bucket and key.
func (db *DB) Set(bucket, key, value []byte) error {
	return db.set(bucket, key, value, context.Background())
}

// Del deletes the value stored in the given bucket and key.
func (db *DB) Del(bucket, key []byte) error {
	return db.del(bucket, key, context.Background())
}

func (db *DB) List(bucket []byte) ([]*database.Entry, error) {
	return db.list(bucket, context.Background())
}

// CmpAndSwap modifies the value at the given bucket and key (to newValue)
// only if the existing (current) value matches oldValue.
func (db *DB) CmpAndSwap(bucket, key, oldValue, newValue []byte) ([]byte, bool, error) {
	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.db.Client().StartSession()
	if err != nil {
		return oldValue, false, errors.Wrap(err, "error starting session")
	}
	defer session.EndSession(context.Background())

	val, swapped := []byte{}, false
	err = mongo.WithSession(context.Background(), session, func(ctx mongo.SessionContext) error {
		if err = session.StartTransaction(txnOptions); err != nil {
			return errors.Wrap(err, "error: pending transaction")
		}

		val, swapped, err = db.cmpAndSwap(bucket, key, oldValue, newValue, ctx)
		if err != nil {
			if err = session.AbortTransaction(context.Background()); err != nil {
				return errors.Wrapf(err, "failed to execute CmpAndSwap transaction on %s/%s and failed to rollback transaction", bucket, key)
			}
			return errors.Wrap(err, "error aborting transaction")
		}

		if err = session.CommitTransaction(context.Background()); err != nil {
			return errors.Wrap(err, "error committing transaction")
		}
		return nil
	})

	return val, swapped, err
}

// Update performs multiple commands on one read-write transaction.
func (db *DB) Update(tx *database.Tx) error {
	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.db.Client().StartSession()
	if err != nil {
		return errors.Wrap(err, "error starting session")
	}
	defer session.EndSession(context.Background())

	err = mongo.WithSession(context.TODO(), session, func(ctx mongo.SessionContext) error {
		if err = session.StartTransaction(txnOptions); err != nil {
			return errors.Wrap(err, "error: pending transaction")
		}

		err = db.executeTransactions(tx, ctx, session)
		if err != nil {
			return err
		}

		if err = errors.WithStack(session.CommitTransaction(context.Background())); err != nil {
			return errors.Wrap(err, "error committing transaction")
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// CreateTable creates a bucket or an embedded bucket if it does not exists.
func (db *DB) createTable(bucket []byte, ctx context.Context) error {
	if err := db.db.CreateCollection(ctx, string(bucket)); err != nil {
		return errors.Wrap(err, "error creating collection")
	}

	// create an index on the Key field
	index := mongo.IndexModel{
		Keys:    createFilter("key", 1),
		Options: options.Index().SetUnique(true),
	}

	_, err := db.db.Collection(string(bucket)).Indexes().CreateOne(context.TODO(), index)
	if err != nil {
		return errors.Wrap(err, "error creating collection")
	}

	return nil
}

// DeleteTable deletes a root or embedded bucket. Returns an error if the
// bucket cannot be found or if the key represents a non-bucket value.
func (db *DB) deleteTable(bucket []byte, ctx context.Context) error {
	if !collectionExists(db.db, string(bucket)) {
		return errors.Wrapf(database.ErrNotFound, "bucket %s not found", bucket)
	}

	if err := db.db.Collection(string(bucket)).Drop(ctx); err != nil {
		return errors.Wrapf(err, "error dropping collection: %s", bucket)
	}
	return nil
}

func (db *DB) get(bucket, key []byte, ctx context.Context) (ret []byte, err error) {
	filter := createFilter("key", key)
	res := db.db.Collection(string(bucket)).FindOne(ctx, filter)

	if err := res.Err(); err != nil {
		return nil, errors.Wrapf(database.ErrNotFound, "%s/%s", bucket, key)
	}

	result := Tuple{}
	if err := res.Decode(&result); err != nil {
		return nil, errors.Wrap(err, "error decoding value")
	}

	return []byte(result.Value), nil
}

func (db *DB) set(bucket, key, value []byte, ctx context.Context) error {
	filter := createFilter("key", key)
	update := createUpdate(value)
	opts := options.Update().SetUpsert(true)

	_, err := db.db.Collection(string(bucket)).UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return errors.Wrapf(err, "error setting value %s/%s", bucket, key)
	}
	return nil
}

// List returns the full list of entries in a bucket.
func (db *DB) list(bucket []byte, ctx context.Context) ([]*database.Entry, error) {
	if !collectionExists(db.db, string(bucket)) {
		return nil, errors.Wrapf(database.ErrNotFound, "bucket %s not found", bucket)
	}

	// match all
	filter := bson.D{{}}

	cursor, err := db.db.Collection(string(bucket)).Find(ctx, filter)
	if err != nil {
		return nil, errors.Wrap(err, "error listing values")
	}
	defer cursor.Close(context.Background())

	if err = cursor.Err(); err != nil {
		return nil, errors.Wrap(err, "error listing values")
	}

	var entries []*database.Entry

	for cursor.Next(context.Background()) {
		tuple := Tuple{}
		cursor.Decode(&tuple)
		entries = append(entries, &database.Entry{
			Bucket: bucket,
			Key:    []byte(tuple.Key),
			Value:  []byte(tuple.Value),
		})
	}

	if err = cursor.Err(); err != nil {
		return nil, errors.Wrap(err, "error listing values")
	}

	return entries, nil
}

// Del deletes the value stored in the given bucket and key.
func (db *DB) del(bucket, key []byte, ctx context.Context) error {
	filter := createFilter("key", key)

	mongoRes, err := db.db.Collection(string(bucket)).DeleteOne(ctx, filter)
	if err != nil {
		return errors.Wrapf(err, "failed to delete %s/%s", bucket, key)
	}

	if mongoRes.DeletedCount == 0 {
		return errors.Wrapf(err, "failed to delete: %s/%s. Value not found", bucket, key)
	}

	return nil
}

func (db *DB) cmpAndSwap(bucket, key, target, newValue []byte, ctx context.Context) ([]byte, bool, error) {
	v, err := db.get(bucket, key, ctx)
	if err != nil && !database.IsErrNotFound(err) {
		return nil, false, err
	}

	if !bytes.Equal(v, target) {
		return v, false, nil
	}

	err = db.set(bucket, key, newValue, ctx)
	if err != nil {
		return nil, false, err
	}

	return newValue, true, nil
}

func (db *DB) executeTransactions(tx *database.Tx, ctx mongo.SessionContext, session mongo.Session) error {
	for _, op := range tx.Operations {
		var err error
		switch op.Cmd {
		case database.CreateTable:
			if err := db.CreateTable(op.Bucket); err != nil {
				return abort(session, err)
			}
		case database.DeleteTable:
			if err := db.DeleteTable(op.Bucket); err != nil {
				return abort(session, err)
			}
		case database.Get:
			if op.Result, err = db.get(op.Bucket, op.Key, ctx); err != nil {
				return abort(session, err)
			}
		case database.Set:
			if err := db.set(op.Bucket, op.Key, op.Value, ctx); err != nil {
				return abort(session, err)
			}
		case database.Delete:
			if err := db.del(op.Bucket, op.Key, ctx); err != nil {
				return abort(session, err)
			}
		case database.CmpAndSwap:
			op.Result, op.Swapped, err = db.cmpAndSwap(op.Bucket, op.Key, op.CmpValue, op.Value, ctx)
			if err != nil {
				return abort(session, err)
			}
		case database.CmpOrRollback:
			return abort(session, database.ErrOpNotSupported)
		default:
			return abort(session, database.ErrOpNotSupported)
		}
	}

	return nil
}

func collectionExists(db *mongo.Database, name string) bool {
	filter := createFilter("name", name)
	list, err := db.ListCollectionNames(context.Background(), filter)
	if err != nil {
		return false
	}
	return len(list) > 0
}

func createFilter(key string, value any) bson.D {
	return bson.D{{Key: key, Value: value}}
}

func createUpdate(value []byte) bson.D {
	return bson.D{{Key: "$set", Value: bson.D{{Key: "value", Value: value}}}}
}

func abort(session mongo.Session, err error) error {
	abortError := session.AbortTransaction(context.Background())
	if abortError != nil {
		return errors.Wrap(err, "error aborting transaction")
	}
	return errors.Wrap(err, "UPDATE failed")
}
