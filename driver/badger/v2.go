package badger

import (
	"context"
	"errors"

	badgerv2 "github.com/dgraph-io/badger/v2"

	"github.com/smallstep/nosql"
)

func init() {
	nosql.Register("badgerv2", OpenV2)
}

// OpenV2 implements a [nosql.Driver] for badger V2 databases.
func OpenV2(ctx context.Context, dir string) (nosql.DB, error) {
	opts := OptionsFromContext(ctx)

	db, err := badgerv2.Open(opts.toV2(dir))
	if err != nil {
		return nil, err
	}

	w := &wrapper[badgerv2.IteratorOptions, *badgerv2.Item, *badgerv2.Iterator, *badgerv2.Txn]{
		DB:            db,
		IsKeyNotFound: func(err error) bool { return errors.Is(err, badgerv2.ErrKeyNotFound) },
		IsNoRewrite:   func(err error) bool { return errors.Is(err, badgerv2.ErrNoRewrite) },
		KeysOnlyIteratorOptions: func(prefix []byte) badgerv2.IteratorOptions {
			return badgerv2.IteratorOptions{
				Prefix: prefix,
			}
		},
		KeysAndValuesIteratorOptions: func(prefix []byte) badgerv2.IteratorOptions {
			return badgerv2.IteratorOptions{
				PrefetchValues: true,
				PrefetchSize:   100,
				Prefix:         prefix,
			}
		},
	}

	return nosql.Constrain(w), nil
}

func (o *Options) toV2(dir string) (opts badgerv2.Options) {
	opts = badgerv2.DefaultOptions(dir)

	if o.ValueDir != "" {
		opts.ValueDir = o.ValueDir
	}
	if o.Logger != nil {
		opts.Logger = o.Logger
	}

	return
}
