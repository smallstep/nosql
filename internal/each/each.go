// Package each implements functionality relating to iterators.
package each

import (
	"bytes"
	"hash/maphash"

	"github.com/smallstep/nosql"
)

// Bucket calls fn with the pointers to the records belonging to each bucket in the provided
// [nosql.Record] slice.
//
// The provided function must not retain access (or mutate) its given arguments.
func Bucket(records []nosql.Record, fn func(bucket []byte, rex []*nosql.Record) error) (err error) {
	var (
		seed = maphash.MakeSeed()
		bm   = map[uint64]struct{}{} // buckets map, sum(bucket) -> presence
		rex  []*nosql.Record         // reusable records buffer
	)

	for i := range records {
		r := &records[i]

		id := maphash.Bytes(seed, r.Bucket)
		if _, ok := bm[id]; ok {
			continue // bucket already processed
		}
		bm[id] = struct{}{}

		rex = append(rex, r)

		for j := range records[i+1:] {
			if rr := &records[1+i+j]; bytes.Equal(r.Bucket, rr.Bucket) {
				rex = append(rex, rr)
			}
		}

		if err = fn(r.Bucket, rex); err != nil {
			break
		}

		clear(rex)
		rex = rex[:0]
	}

	return
}
