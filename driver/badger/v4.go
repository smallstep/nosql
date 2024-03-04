package badger

import (
	"context"
	"errors"

	badgerv4 "github.com/dgraph-io/badger/v4"

	"github.com/smallstep/nosql"
)

func init() {
	nosql.Register("badgerv4", OpenV4)
}

// OpenV4 implements a [nosql.Driver] for badger V4 databases.
func OpenV4(ctx context.Context, dir string) (nosql.DB, error) {
	opts := OptionsFromContext(ctx)

	db, err := badgerv4.Open(opts.toV4(dir))
	if err != nil {
		return nil, err
	}

	w := &wrapper[badgerv4.IteratorOptions, *badgerv4.Item, *badgerv4.Iterator, *badgerv4.Txn]{
		db:            db,
		isKeyNotFound: func(err error) bool { return errors.Is(err, badgerv4.ErrKeyNotFound) },
		isNoRewrite:   func(err error) bool { return errors.Is(err, badgerv4.ErrNoRewrite) },
		keysOnlyIteratorOptions: func(prefix []byte) badgerv4.IteratorOptions {
			return badgerv4.IteratorOptions{
				Prefix: prefix,
			}
		},
		keysAndValuesIteratorOptions: func(prefix []byte) badgerv4.IteratorOptions {
			return badgerv4.IteratorOptions{
				PrefetchValues: true,
				PrefetchSize:   100,
				Prefix:         prefix,
			}
		},
	}

	return nosql.Constrain(w), nil
}

func (o *Options) toV4(dir string) (opts badgerv4.Options) {
	opts = badgerv4.DefaultOptions(dir)

	if o.ValueDir != "" {
		opts.ValueDir = o.ValueDir
	}
	if o.Logger != nil {
		opts.Logger = o.Logger
	}

	return
}
