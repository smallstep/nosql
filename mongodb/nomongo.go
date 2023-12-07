//go:build nomysql
// +build nomysql

package mongo

import "github.com/smallstep/nosql/database"

type DB = database.NotSupportedDB
