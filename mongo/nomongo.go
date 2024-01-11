//go:build nomongo
// +build nomongo

package mongo

import "github.com/smallstep/nosql/database"

type DB = database.NotSupportedDB
