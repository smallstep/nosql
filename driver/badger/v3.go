package badger

import (
	"context"
	"errors"

	badgerv3 "github.com/dgraph-io/badger/v3"

	"github.com/smallstep/nosql"
)

func init() {
	nosql.Register("badgerv3", OpenV3)
}

// OpenV3 implements a [nosql.Driver] for badger V3 databases.
func OpenV3(ctx context.Context, dir string) (nosql.DB, error) {
	opts := OptionsFromContext(ctx)

	db, err := badgerv3.Open(opts.toV3(dir))
	if err != nil {
		return nil, err
	}

	w := &wrapper[badgerv3.IteratorOptions, *badgerv3.Item, *badgerv3.Iterator, *badgerv3.Txn]{
		DB:            db,
		IsKeyNotFound: func(err error) bool { return errors.Is(err, badgerv3.ErrKeyNotFound) },
		IsNoRewrite:   func(err error) bool { return errors.Is(err, badgerv3.ErrNoRewrite) },
		KeysOnlyIteratorOptions: func(prefix []byte) badgerv3.IteratorOptions {
			return badgerv3.IteratorOptions{
				Prefix: prefix,
			}
		},
		KeysAndValuesIteratorOptions: func(prefix []byte) badgerv3.IteratorOptions {
			return badgerv3.IteratorOptions{
				PrefetchValues: true,
				PrefetchSize:   100,
				Prefix:         prefix,
			}
		},
	}

	return nosql.Constrain(w), nil
}

func (o *Options) toV3(dir string) (opts badgerv3.Options) {
	opts = badgerv3.DefaultOptions(dir)

	if o.ValueDir != "" {
		opts.ValueDir = o.ValueDir
	}
	if o.Logger != nil {
		opts.Logger = o.Logger
	}

	return
}
