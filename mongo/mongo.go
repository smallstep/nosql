//go:build !nomongo
// +build !nomongo

package mongo

import (
	"bytes"
	"context"
	"fmt"
	"time"

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

type tuple struct {
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
		return fmt.Errorf("replica set name is required to enable transactions")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to invalid options %v: %w", clientOptions, err)
	}

	if err = client.Ping(context.Background(), nil); err != nil {
		return fmt.Errorf("failed connecting to MongoDB: %w", err)
	}

	dbOpts := options.Database()
	db.db = client.Database(opts.Database, dbOpts)

	return nil
}

func (db *DB) Close() error {
	if err := db.db.Client().Disconnect(context.Background()); err != nil {
		return fmt.Errorf("failed disconnecting mongo to: %w", err)
	}

	return nil
}

// CreateTable creates a collection or an embedded collection if it does not exists.
func (db *DB) CreateTable(collection []byte) error {
	return db.createTable(context.Background(), collection)
}

// DeleteTable deletes a root or embedded collection. Returns an error if the
// collection cannot be found or if the key represents a non-collection value.
func (db *DB) DeleteTable(collection []byte) error {
	return db.deleteTable(context.Background(), collection)
}

// Get returns the value stored in the given bucked and key.
func (db *DB) Get(collection, key []byte) (ret []byte, err error) {
	return db.get(context.Background(), collection, key)
}

// Set stores the given value on collection and key.
func (db *DB) Set(collection, key, value []byte) error {
	return db.set(context.Background(), collection, key, value)
}

// Del deletes the value stored in the given collection and key.
func (db *DB) Del(collection, key []byte) error {
	return db.del(context.Background(), collection, key)
}

func (db *DB) List(collection []byte) ([]*database.Entry, error) {
	return db.list(context.Background(), collection)
}

// CmpAndSwap modifies the value at the given collection and key (to newValue)
// only if the existing (current) value matches oldValue.
func (db *DB) CmpAndSwap(collection, key, oldValue, newValue []byte) ([]byte, bool, error) {
	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.db.Client().StartSession()
	if err != nil {
		return oldValue, false, fmt.Errorf("failed starting session: %w", err)
	}
	defer session.EndSession(context.Background())

	val, swapped := []byte{}, false
	err = mongo.WithSession(context.Background(), session, func(ctx mongo.SessionContext) error {
		if err = session.StartTransaction(txnOptions); err != nil {
			return fmt.Errorf("failed to pending transaction: %w", err)
		}

		val, swapped, err = db.cmpAndSwap(ctx, collection, key, oldValue, newValue)
		if err != nil {
			if err = session.AbortTransaction(ctx); err != nil {
				return fmt.Errorf("failed to execute CmpAndSwap transaction on %s/%s and failed to rollback transaction: %w", collection, key, err)
			}
			return fmt.Errorf("failed aborting transaction: %w", err)
		}

		if err = session.CommitTransaction(ctx); err != nil {
			return fmt.Errorf("failed committing transaction: %w", err)
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
		return fmt.Errorf("failed starting session: %w", err)
	}
	defer session.EndSession(context.Background())

	err = mongo.WithSession(context.Background(), session, func(ctx mongo.SessionContext) error {
		if err = session.StartTransaction(txnOptions); err != nil {
			return fmt.Errorf("failed to pending transaction: %w", err)
		}

		err = db.executeTransactions(ctx, tx, session)
		if err != nil {
			return err
		}

		if err = session.CommitTransaction(ctx); err != nil {
			return fmt.Errorf("failed committing transaction: %w", err)
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (db *DB) createTable(ctx context.Context, collection []byte) error {
	if err := db.db.CreateCollection(ctx, string(collection)); err != nil {
		return fmt.Errorf("failed creating collection %q: %w", collection, err)
	}

	// create an index on the Key field
	index := mongo.IndexModel{
		Keys:    createFilter("key", 1),
		Options: options.Index().SetUnique(true),
	}

	_, err := db.db.Collection(string(collection)).Indexes().CreateOne(ctx, index)
	if err != nil {
		return fmt.Errorf("failed creating collection %q: %w", collection, err)
	}

	return nil
}

func (db *DB) deleteTable(ctx context.Context, collection []byte) error {
	if !collectionExists(ctx, db.db, string(collection)) {
		return fmt.Errorf("failed deleting collection %q: %w ", collection, database.ErrNotFound)
	}

	if err := db.db.Collection(string(collection)).Drop(ctx); err != nil {
		return fmt.Errorf("failed dropping collection %q: %w", collection, err)
	}
	return nil
}

func (db *DB) get(ctx context.Context, collection, key []byte) (ret []byte, err error) {
	filter := createFilter("key", key)
	res := db.db.Collection(string(collection)).FindOne(ctx, filter)

	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("failed finding %s/%s: %w", collection, key, database.ErrNotFound)
	}

	result := tuple{}
	if err := res.Decode(&result); err != nil {
		return nil, fmt.Errorf("failed decoding value for %s/%s: %w", collection, key, err)
	}

	return result.Value, nil
}

func (db *DB) set(ctx context.Context, collection, key, value []byte) error {
	filter := createFilter("key", key)
	update := createUpdate(value)
	opts := options.Update().SetUpsert(true)

	_, err := db.db.Collection(string(collection)).UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed setting value %s/%s: %w", collection, key, err)
	}
	return nil
}

// List returns the full list of entries in a collection.
func (db *DB) list(ctx context.Context, collection []byte) ([]*database.Entry, error) {
	if !collectionExists(ctx, db.db, string(collection)) {
		return nil, fmt.Errorf("failed finding collection %q: %w", collection, database.ErrNotFound)
	}

	// match all
	filter := bson.D{{}}

	cursor, err := db.db.Collection(string(collection)).Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed listing values of collection %s to: %w", collection, err)
	}
	defer cursor.Close(ctx)

	if err = cursor.Err(); err != nil {
		return nil, fmt.Errorf("failed listing values of %q: %w", collection, err)
	}

	var entries []*database.Entry

	for cursor.Next(ctx) {
		t := tuple{}

		if err := cursor.Decode(&t); err != nil {
			return nil, fmt.Errorf("failed decoding value: %w", err)
		}

		entries = append(entries, &database.Entry{
			Bucket: collection,
			Key:    t.Key,
			Value:  t.Value,
		})
	}

	if err = cursor.Err(); err != nil {
		return nil, fmt.Errorf("failed listing values of collection %q: %w", collection, err)
	}

	return entries, nil
}

// Del deletes the value stored in the given collection and key.
func (db *DB) del(ctx context.Context, collection, key []byte) error {
	filter := createFilter("key", key)

	mongoRes, err := db.db.Collection(string(collection)).DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed deleting %s/%s: %w", collection, key, err)
	}

	if mongoRes.DeletedCount == 0 {
		return fmt.Errorf("failed to delete: %s/%s: %w", collection, key, database.ErrNotFound)
	}

	return nil
}

func (db *DB) cmpAndSwap(ctx context.Context, collection, key, target, newValue []byte) ([]byte, bool, error) {
	v, err := db.get(ctx, collection, key)
	if err != nil && !database.IsErrNotFound(err) {
		return nil, false, err
	}

	if !bytes.Equal(v, target) {
		return v, false, nil
	}

	err = db.set(ctx, collection, key, newValue)
	if err != nil {
		return nil, false, err
	}

	return newValue, true, nil
}

func (db *DB) executeTransactions(ctx mongo.SessionContext, tx *database.Tx, session mongo.Session) error {
	for _, op := range tx.Operations {
		var err error
		switch op.Cmd {
		case database.CreateTable:
			if err := db.CreateTable(op.Bucket); err != nil {
				return abort(ctx, session, err)
			}
		case database.DeleteTable:
			if err := db.DeleteTable(op.Bucket); err != nil {
				return abort(ctx, session, err)
			}
		case database.Get:
			if op.Result, err = db.get(ctx, op.Bucket, op.Key); err != nil {
				return abort(ctx, session, err)
			}
		case database.Set:
			if err := db.set(ctx, op.Bucket, op.Key, op.Value); err != nil {
				return abort(ctx, session, err)
			}
		case database.Delete:
			if err := db.del(ctx, op.Bucket, op.Key); err != nil {
				return abort(ctx, session, err)
			}
		case database.CmpAndSwap:
			op.Result, op.Swapped, err = db.cmpAndSwap(ctx, op.Bucket, op.Key, op.CmpValue, op.Value)
			if err != nil {
				return abort(ctx, session, err)
			}
		case database.CmpOrRollback:
			return abort(ctx, session, database.ErrOpNotSupported)
		default:
			return abort(ctx, session, database.ErrOpNotSupported)
		}
	}

	return nil
}

func collectionExists(ctx context.Context, db *mongo.Database, name string) bool {
	filter := createFilter("name", name)
	list, err := db.ListCollectionNames(ctx, filter)
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

func abort(ctx context.Context, session mongo.Session, err error) error {
	abortError := session.AbortTransaction(ctx)
	if abortError != nil {
		return fmt.Errorf("failed aborting transaction due to %q: %w", abortError, err)
	}
	return fmt.Errorf("failed executing transaction: %w", err)
}
