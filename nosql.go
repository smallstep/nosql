package nosql

import (
	"github.com/pkg/errors"
	"github.com/smallstep/nosql/badger"
	"github.com/smallstep/nosql/bolt"
	"github.com/smallstep/nosql/database"
	"github.com/smallstep/nosql/mysql"
)

// Option is just a wrapper over database.Option.
type Option = database.Option

// DB is just a wrapper over database.DB.
type DB = database.DB

var (
	// WithValueDir is a wrapper over database.WithValueDir.
	WithValueDir = database.WithValueDir
	// WithDatabase is a wrapper over database.WithDatabase.
	WithDatabase = database.WithDatabase
)

// IsErrNotFound returns true if the cause of the given error is ErrNotFound.
func IsErrNotFound(err error) bool {
	return err == database.ErrNotFound || cause(err) == database.ErrNotFound
}

// IsErrOpNotSupported returns true if the cause of the given error is ErrOpNotSupported.
func IsErrOpNotSupported(err error) bool {
	return err == database.ErrOpNotSupported || cause(err) == database.ErrNotFound
}

// cause (from github.com/pkg/errors) returns the underlying cause of the
// error, if possible. An error value has a cause if it implements the
// following interface:
//
//     type causer interface {
//            Cause() error
//     }
//
// If the error does not implement Cause, the original error will
// be returned. If the error is nil, nil will be returned without further
// investigation.
func cause(err error) error {
	type causer interface {
		Cause() error
	}

	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return err
}

// New returns a database with the given driver.
func New(driver, dataSourceName string, opt ...Option) (db database.DB, err error) {
	switch driver {
	case "badger":
		db = &badger.DB{}
	case "bbolt":
		db = &bolt.DB{}
	case "mysql":
		db = &mysql.DB{}
	default:
		return nil, errors.Errorf("%s database not supported", driver)
	}
	err = db.Open(dataSourceName, opt...)
	return
}
