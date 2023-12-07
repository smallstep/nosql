//go:build !nomongodb
// +build !nomongodb

package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/smallstep/nosql/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DB struct {
	//	db     *mongo.Database
	Client *mongo.Client
	Ctx    context.Context
	Cancel context.CancelFunc
	Name   string
}

type Options struct {
	Database              string
	ValueDir              string
	BadgerFileLoadingMode string
}

type Entry struct {
	Bucket []byte
	Key    []byte
	Value  []byte
}

var (
	// ErrNotFound is the type returned on DB implementations if an item does not
	// exist.
	ErrNotFound = errors.New("not found")
	// ErrOpNotSupported is the type returned on DB implementations if an operation
	// is not supported.
	ErrOpNotSupported = errors.New("operation not supported")
)

// IsErrNotFound returns true if the cause of the given error is ErrNotFound.
func IsErrNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsErrOpNotSupported returns true if the cause of the given error is ErrOpNotSupported.
func IsErrOpNotSupported(err error) bool {
	return errors.Is(err, ErrOpNotSupported)
}

/*
type DB interface {
	Open(dbn, dataSourceName string) error
	Close() error
	Get(collection, key []byte) (ret []byte, err error)
	Set(collection, key, value []byte) error
	CmpAndSwap(collection, key, oldValue, newValue []byte) ([]byte, bool, error)
	Del(bucket, key []byte) error
	List(bucket []byte) ([]Entry, error)
	Update(tx *database.Tx) error
	CreateTable(collection []byte) error
	DeleteTable(collection []byte) error
}
*/

func connect(uri string) (*mongo.Client, context.Context, context.CancelFunc, error) {

	ctx, cancel := context.WithTimeout(context.Background(),
		30*time.Second)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	return client, ctx, cancel, err
}

func (db *DB) Open(datasource string, opt ...database.Option) error {
	var err error
	opts := &database.Options{}
	for _, o := range opt {
		if err := o(opts); err != nil {
			return err
		}
	}
	db.Name = opts.Database
	//open connects to the database and holds the connection
	db.Client, db.Ctx, db.Cancel, err = connect(datasource)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() error {
	defer db.Cancel()

	defer func() {
		if err := db.Client.Disconnect(db.Ctx); err != nil {
			panic(err)
		}
	}()
	return nil
}

func (db *DB) doesExist(collection string) bool {
	moncol, err := db.Client.Database(db.Name).ListCollectionNames(db.Ctx, bson.D{})
	if err != nil {
		return false
	}
	for _, c := range moncol {
		if strings.EqualFold(c, collection) {
			return true
		}
	}
	return false
}

func (db *DB) CreateTable(collection []byte) error {
	//check for existance of collection and then create if not exists
	//this should be safe to perform moving forward.
	if db.doesExist(string(collection)) {
		return nil
	}
	err := db.Client.Database(db.Name).CreateCollection(db.Ctx, string(collection))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) DeleteTable(collection []byte) error {
	//this will remove a collection. I think we might just have this return nil and not doing
	//anything initially.
	return nil
}

func (db *DB) Get(collection []byte, key []byte) ([]byte, error) {
	//fetch entry from collection
	var ret interface{}
	var err error
	moncol := db.Client.Database(db.Name).Collection(string(collection))
	val := moncol.FindOne(db.Ctx, key)
	err = val.Decode(&ret)
	if err != nil {
		return nil, err
	}
	return ret.([]byte), nil
}

func (db *DB) Set(collection, key, val []byte) error {
	//set value to collection and key
	record := map[string]interface{}{
		"key":   key,
		"value": val,
	}
	moncol := db.Client.Database(db.Name).Collection(string(collection))
	_, err := moncol.InsertOne(db.Ctx, record)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(collection, key []byte) error {
	filter := map[string]interface{}{
		"key": key,
	}
	moncol := db.Client.Database(db.Name).Collection(string(collection))
	_, err := moncol.DeleteOne(db.Ctx, filter)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) List(collection []byte) ([]*database.Entry, error) {
	//list is the equivalent of the find command run on a collection with no filter
	var docs []bson.D
	var ret []*database.Entry
	moncol := db.Client.Database(db.Name).Collection(string(collection))
	items, err := moncol.Find(db.Ctx, bson.D{})
	_ = items.All(db.Ctx, &docs)
	if err != nil {
		return nil, err
	}
	for _, item := range docs {
		tmp := database.Entry{
			Bucket: collection,
		}
		for _, v := range item {
			tmp.Key = []byte(v.Key)
			tmp.Value = v.Value.([]byte)
		}
		ret = append(ret, &tmp)
	}
	return ret, nil
}

func (db *DB) CmpAndSwap(collection, key, oldValue, newValue []byte) ([]byte, bool, error) {
	//compare and swap value at specified key
	cval, err := db.Get(collection, key)
	if err != nil {
		return nil, false, err
	}
	if bytes.Equal(cval, oldValue) {
		err = db.Set(collection, key, newValue)
		if err != nil {
			return nil, false, err
		}
		return newValue, true, nil
	}
	return nil, false, nil
}

func (db *DB) Update(txn *database.Tx) error {
	//update is a collection of instructions to run one after the next.
	var err error
	for _, i := range txn.Operations {
		// create or delete buckets
		switch i.Cmd {
		case database.CreateTable:
			err = db.CreateTable(i.Bucket)
			if err != nil {
				return error(fmt.Errorf("failed to create table %s", i.Bucket))
			}
		case database.DeleteTable:
			if err = db.DeleteTable(i.Bucket); err != nil {
				return error(fmt.Errorf("failed to delete table %s", i.Bucket))
			}
		case database.Get:
			var val []byte
			if val, err = db.Get(i.Bucket, i.Key); err != nil {
				return error(fmt.Errorf("failed to retrieve data from %s with key %s", i.Bucket, i.Key))
			}
			i.Result = val
		case database.Set:
			if err = db.Set(i.Bucket, i.Key, i.Value); err != nil {
				return error(fmt.Errorf("failed to set data to %s with key %s and value %s with err %s", i.Bucket, i.Key, i.Value, err))
			}
		case database.Delete:
			if err = db.Del(i.Bucket, i.Key); err != nil {
				return error(fmt.Errorf("failed to delete data in %s with key %s with err %s", i.Bucket, i.Key, err))
			}
		case database.CmpAndSwap:
			i.Result, i.Swapped, err = db.CmpAndSwap(i.Bucket, i.Key, i.CmpValue, i.Value)
			if err != nil {
				return error(fmt.Errorf("failed to swap data in %s with key %s with cmpvalue %s and value %s with err %s", i.Bucket, i.Key, i.CmpValue, i.Value, err))
			}
		case database.CmpOrRollback:
			return ErrOpNotSupported
		default:
			return ErrOpNotSupported
		}
	}
	return nil
}

func (db *DB) Compact() error {
	//not used outside of badger
	return nil
}
