package token

import (
	"bytes"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	const (
		minSize    = 5
		maxSize    = 42
		iterations = 1000
	)

	tokens := make([][]byte, 0, iterations)

	for i := 0; i < iterations; i++ {
		tok := New(t, minSize, maxSize, true)
		tokens = append(tokens, tok)

		assert.True(t, minSize <= len(tok) && len(tok) <= maxSize,
			"wrong size (%d) for token %q", len(tok), tok)

		assert.True(t, utf8.Valid(tok),
			"invalid utf8 in token %q", tok)

		assert.True(t, bytes.IndexByte(tok, 0) == -1,
			"zero byte in token %q", tok)

		assert.False(t, bytes.HasSuffix(tok, []byte{' '}),
			"token (%q) ends with a space", tok)
	}
	assert.Len(t, tokens, iterations)
}
