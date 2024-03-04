package badger

import (
	"context"
	"errors"

	badgerv1 "github.com/dgraph-io/badger"

	"github.com/smallstep/nosql"
)

func init() {
	nosql.Register("badgerv1", OpenV1)
}

// OpenV1 implements a [nosql.Driver] for badger V1 databases.
func OpenV1(ctx context.Context, dir string) (nosql.DB, error) {
	opts := OptionsFromContext(ctx)
	o, err := opts.toV1(dir)
	if err != nil {
		return nil, err
	}

	db, err := badgerv1.Open(o)
	if err != nil {
		return nil, err
	}

	w := &wrapper[badgerv1.IteratorOptions, *badgerv1.Item, *badgerv1.Iterator, *badgerv1.Txn]{
		db:            db,
		isKeyNotFound: func(err error) bool { return errors.Is(err, badgerv1.ErrKeyNotFound) },
		isNoRewrite:   func(err error) bool { return errors.Is(err, badgerv1.ErrNoRewrite) },
		keysOnlyIteratorOptions: func(prefix []byte) badgerv1.IteratorOptions {
			return badgerv1.IteratorOptions{
				Prefix: prefix,
			}
		},
		keysAndValuesIteratorOptions: func(prefix []byte) badgerv1.IteratorOptions {
			return badgerv1.IteratorOptions{
				PrefetchValues: true,
				PrefetchSize:   100,
				Prefix:         prefix,
			}
		},
	}

	return nosql.Constrain(w), nil
}

func (o *Options) toV1(dir string) (opts badgerv1.Options, err error) {
	opts = badgerv1.DefaultOptions(dir)

	if o.ValueDir != "" {
		opts.ValueDir = o.ValueDir
	}
	if o.Logger != nil {
		opts.Logger = o.Logger
	}

	return
}
