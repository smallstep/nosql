package each

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smallstep/nosql"
)

func TestBucket(t *testing.T) {
	var (
		b = func(v string) []byte {
			return []byte(v)
		}

		src = []nosql.Record{
			{Bucket: b("1"), Key: b("1"), Value: b("1")},
			{Bucket: b("2"), Key: b("1"), Value: b("1")},
			{Bucket: b("3"), Key: b("1"), Value: b("1")},
			{Bucket: b("1"), Key: b("2"), Value: b("1")},
			{Bucket: b("1"), Key: b("3"), Value: b("1")},
			{Bucket: b("3"), Key: b("2"), Value: b("2")},
			{Bucket: b("2"), Key: b("2"), Value: b("2")},
			{Bucket: b("1"), Key: b("1"), Value: b("2")},
		}

		exp = []nosql.Record{
			{Bucket: b("1"), Key: b("1"), Value: b("1")},
			{Bucket: b("1"), Key: b("2"), Value: b("1")},
			{Bucket: b("1"), Key: b("3"), Value: b("1")},
			{Bucket: b("1"), Key: b("1"), Value: b("2")},
			{Bucket: b("2"), Key: b("1"), Value: b("1")},
			{Bucket: b("2"), Key: b("2"), Value: b("2")},
			{Bucket: b("3"), Key: b("1"), Value: b("1")},
			{Bucket: b("3"), Key: b("2"), Value: b("2")},
		}
	)

	var got []nosql.Record
	err := Bucket(src, func(bucket []byte, rex []*nosql.Record) error {
		for _, r := range rex {
			got = append(got, *r)
		}

		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, exp, got)
}
