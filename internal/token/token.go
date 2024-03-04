package token

import (
	"crypto/rand"
	"io"
	mand "math/rand"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// New returns a random token with length in the [minSize, maxSize] interval.
//
// If bucket is set, then the returned value will be valid to be used as a bucket.
func New(t *testing.T, minSize, maxSize int, bucket bool) (tok []byte) {
	if minSize == maxSize {
		tok = make([]byte, maxSize)
	} else {
		tok = make([]byte, minSize+mand.Intn(1+maxSize-minSize)) //nolint:gosec // not a sensitive op
	}

	src := rand.Reader
	if bucket {
		src = allowedRuneReader{}
	}

	for {
		_, err := io.ReadFull(src, tok)
		require.NoError(t, err)

		if !bucket {
			break
		} else if r, _ := utf8.DecodeLastRune(tok); !unicode.IsSpace(r) {
			break
		}
	}

	return
}

// allowedRuneReader implements a reader that reads runes valid for buckets.
type allowedRuneReader struct{}

// Read implements io.Reader for [bucketRuneReader].
func (allowedRuneReader) Read(buf []byte) (n int, _ error) {
	for len(buf) > 0 {
		var r rune
		var s int
		for {
			const (
				runeStart = '\U00000001' // inclusive
				runeStop  = '\U00010000' // not inclusive
			)

			// determine a random rune up to the allowed remaining buffer size
			r = runeStart + mand.Int31n(runeStop-runeStart) //nolint:gosec // not a sensitive op
			if s = utf8.RuneLen(r); s != -1 && s <= len(buf) {
				break
			}
		}

		n += utf8.EncodeRune(buf, r)
		buf = buf[s:]
	}

	return
}
