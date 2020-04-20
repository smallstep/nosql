package badger

import (
	"errors"
	"testing"

	"github.com/smallstep/assert"
)

func Test_badgerEncode(t *testing.T) {
	type args struct {
		val []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
		err  error
	}{
		{
			name: "fail/input-too-long",
			args: args{make([]byte, 65536)},
			err:  errors.New("length of input cannot be greater than 65535"),
		},
		{
			name: "fail/input-empty",
			args: args{nil},
			err:  errors.New("input cannot be empty"),
		},
		{
			name: "ok",
			args: args{[]byte("hello")},
			want: []byte{5, 0, 104, 101, 108, 108, 111},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := badgerEncode(tt.args.val)
			if err != nil {
				if assert.NotNil(t, tt.err) {
					assert.HasPrefix(t, err.Error(), tt.err.Error())
				}
			} else {
				if assert.Nil(t, tt.err) && assert.NotNil(t, got) && assert.NotNil(t, tt.want) {
					assert.Equals(t, got, tt.want)
				}
			}
		})
	}
}

func Test_toBadgerKey(t *testing.T) {
	type args struct {
		bucket []byte
		key    []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
		err  error
	}{
		{
			name: "fail/bucket-too-long",
			args: args{make([]byte, 65536), []byte("goodbye")},
			err:  errors.New("length of input cannot be greater than 65535"),
		},
		{
			name: "fail/key-empty",
			args: args{[]byte("hello"), nil},
			err:  errors.New("input cannot be empty"),
		},
		{
			name: "ok",
			args: args{[]byte("hello"), []byte("goodbye")},
			want: []byte{5, 0, 104, 101, 108, 108, 111, 7, 0, 103, 111, 111, 100, 98, 121, 101},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toBadgerKey(tt.args.bucket, tt.args.key)
			if err != nil {
				if assert.NotNil(t, tt.err) {
					assert.HasPrefix(t, err.Error(), tt.err.Error())
				}
			} else {
				if assert.Nil(t, tt.err) && assert.NotNil(t, got) && assert.NotNil(t, tt.want) {
					assert.Equals(t, got, tt.want)
				}
			}
		})
	}
}

func Test_fromBadgerKey(t *testing.T) {
	type args struct {
		bk []byte
	}
	type ret struct {
		bucket []byte
		key    []byte
	}
	tests := []struct {
		name string
		args args
		want ret
		err  error
	}{
		{
			name: "fail/input-too-short/no-bucket-length",
			args: args{[]byte{5}},
			err:  errors.New("invalid badger key: [5]"),
		},
		{
			name: "fail/input-too-short/no-key-length",
			args: args{[]byte{5, 0, 104, 101, 108, 108, 111}},
			err:  errors.New("invalid badger key: [5 0 104 101 108 108 111]"),
		},
		{
			name: "fail/input-too-short/invalid-key",
			args: args{[]byte{5, 0, 104, 101, 108, 108, 111, 7, 0, 103}},
			err:  errors.New("invalid badger key: [5 0 104 101 108 108 111 7 0 103]"),
		},
		{
			name: "ok",
			args: args{[]byte{5, 0, 104, 101, 108, 108, 111, 7, 0, 103, 111, 111, 100, 98, 121, 101}},
			want: ret{[]byte{104, 101, 108, 108, 111}, []byte{103, 111, 111, 100, 98, 121, 101}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := fromBadgerKey(tt.args.bk)
			if err != nil {
				if assert.NotNil(t, tt.err) {
					assert.HasPrefix(t, err.Error(), tt.err.Error())
				}
			} else {
				if assert.Nil(t, tt.err) && assert.NotNil(t, bucket) && assert.NotNil(t, key) {
					assert.Equals(t, bucket, tt.want.bucket)
					assert.Equals(t, key, tt.want.key)
				}
			}
		})
	}
}

func Test_parseBadgerEncode(t *testing.T) {
	type args struct {
		bk []byte
	}
	type ret struct {
		bucket []byte
		key    []byte
	}
	tests := []struct {
		name string
		args args
		want ret
	}{
		{
			name: "fail/keylen-too-short",
			args: args{[]byte{5}},
			want: ret{nil, []byte{5}},
		},
		{
			name: "fail/key-too-short",
			args: args{[]byte{5, 0, 111, 111}},
			want: ret{nil, []byte{5, 0, 111, 111}},
		},
		{
			name: "ok/exact-length",
			args: args{[]byte{5, 0, 104, 101, 108, 108, 111}},
			want: ret{[]byte{104, 101, 108, 108, 111}, nil},
		},
		{
			name: "ok/longer",
			args: args{[]byte{5, 0, 104, 101, 108, 108, 111, 7, 0, 103, 111, 111, 100, 98, 121, 101}},
			want: ret{[]byte{104, 101, 108, 108, 111}, []byte{7, 0, 103, 111, 111, 100, 98, 121, 101}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key := parseBadgerEncode(tt.args.bk)
			assert.Equals(t, bucket, tt.want.bucket)
			assert.Equals(t, key, tt.want.key)
		})
	}
}

func Test_isBadgerTable(t *testing.T) {
	type args struct {
		bk []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "false/keylen-too-short",
			args: args{[]byte{5}},
			want: false,
		},
		{
			name: "false/key-too-short",
			args: args{[]byte{5, 0, 111, 111}},
			want: false,
		},
		{
			name: "ok",
			args: args{[]byte{5, 0, 104, 101, 108, 108, 111}},
			want: true,
		},
		{
			name: "false/key-too-long",
			args: args{[]byte{5, 0, 104, 101, 108, 108, 111, 7, 0, 103, 111, 111, 100, 98, 121, 101}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equals(t, isBadgerTable(tt.args.bk), tt.want)
		})
	}
}
