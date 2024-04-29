package nosql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/smallstep/assert"
	"github.com/smallstep/nosql/database"
)

type testUser struct {
	Fname, lname string
	numPets      int
}

func run(t *testing.T, db database.DB) {
	var boogers = []byte("boogers")

	ub := []byte("testNoSQLUsers")
	assert.True(t, IsErrNotFound(db.DeleteTable(ub)))
	assert.Nil(t, db.CreateTable(ub))
	// Verify that re-creating the table does not cause a "table already exists" error
	assert.Nil(t, db.CreateTable(ub))

	// Test that we can create tables with illegal/special characters (e.g. `-`)
	illName := []byte("test-special-char")
	assert.Nil(t, db.CreateTable(illName))
	assert.Nil(t, db.DeleteTable(illName))
	_, err := db.List(illName)
	assert.True(t, IsErrNotFound(err))

	// List should be empty
	entries, err := db.List(ub)
	assert.Nil(t, err)
	assert.Equals(t, len(entries), 0)

	// check for mike - should not exist
	_, err = db.Get(ub, []byte("mike"))
	assert.True(t, IsErrNotFound(err))

	// add mike
	assert.Nil(t, db.Set(ub, []byte("mike"), boogers))

	// verify that mike is in db
	res, err := db.Get(ub, []byte("mike"))
	assert.FatalError(t, err)
	assert.Equals(t, boogers, res)

	// overwrite mike
	mike := testUser{"mike", "malone", 1}
	mikeb, err := json.Marshal(mike)
	assert.FatalError(t, err)

	assert.Nil(t, db.Set(ub, []byte("mike"), mikeb))
	// verify overwrite
	res, err = db.Get(ub, []byte("mike"))
	assert.FatalError(t, err)
	assert.Equals(t, mikeb, res)

	var swapped bool
	// CmpAndSwap should load since mike is not nil
	res, swapped, err = db.CmpAndSwap(ub, []byte("mike"), nil, boogers)
	assert.FatalError(t, err)
	assert.Equals(t, mikeb, res)
	assert.False(t, swapped)
	assert.Nil(t, err)

	// delete mike
	assert.FatalError(t, db.Del(ub, []byte("mike")))

	// CmpAndSwap should overwrite mike since mike is nil
	res, swapped, err = db.CmpAndSwap(ub, []byte("mike"), nil, boogers)
	assert.FatalError(t, err)
	assert.Equals(t, boogers, res)
	assert.True(t, swapped)
	assert.Nil(t, err)

	// delete mike
	assert.FatalError(t, db.Del(ub, []byte("mike")))

	// check for mike - should not exist
	_, err = db.Get(ub, []byte("mike"))
	assert.True(t, IsErrNotFound(err))

	// CmpAndSwap should store since mike does not exist
	res, swapped, err = db.CmpAndSwap(ub, []byte("mike"), nil, mikeb)
	assert.FatalError(t, err)
	assert.Equals(t, res, mikeb)
	assert.True(t, swapped)
	assert.Nil(t, err)

	// delete mike
	assert.FatalError(t, db.Del(ub, []byte("mike")))

	// Update //

	// create txns for update test
	mariano := testUser{"mariano", "Cano", 2}
	marianob, err := json.Marshal(mariano)
	assert.FatalError(t, err)
	seb := testUser{"sebastian", "tiedtke", 0}
	sebb, err := json.Marshal(seb)
	assert.FatalError(t, err)
	gates := testUser{"bill", "gates", 2}
	gatesb, err := json.Marshal(gates)
	assert.FatalError(t, err)

	casGates := &database.TxEntry{
		Bucket:   ub,
		Key:      []byte("bill"),
		Value:    gatesb,
		CmpValue: nil,
		Cmd:      database.CmpAndSwap,
	}
	setMike := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("mike"),
		Value:  mikeb,
		Cmd:    database.Set,
	}
	readMike := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("mike"),
		Cmd:    database.Get,
	}
	setMariano := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("mariano"),
		Value:  marianob,
		Cmd:    database.Set,
	}
	setSeb := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("sebastian"),
		Value:  sebb,
		Cmd:    database.Set,
	}
	readSeb := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("sebastian"),
		Cmd:    database.Get,
	}
	casGates2 := &database.TxEntry{
		Bucket:   ub,
		Key:      []byte("bill"),
		Value:    boogers,
		CmpValue: gatesb,
		Cmd:      database.CmpAndSwap,
	}
	casGates3 := &database.TxEntry{
		Bucket:   ub,
		Key:      []byte("bill"),
		Value:    []byte("belly-button-lint"),
		CmpValue: gatesb,
		Cmd:      database.CmpAndSwap,
	}

	// update: read write multiple entries.
	tx := &database.Tx{Operations: []*database.TxEntry{setMike, setMariano, readMike, setSeb, readSeb, casGates, casGates2, casGates3}}
	assert.Nil(t, db.Update(tx))

	// verify that mike is in db
	res, err = db.Get(ub, []byte("mike"))
	assert.FatalError(t, err)
	assert.Equals(t, mikeb, res)

	// verify that mariano is in db
	res, err = db.Get(ub, []byte("mariano"))
	assert.FatalError(t, err)
	assert.Equals(t, marianob, res)

	// verify that bill gates is in db
	res, err = db.Get(ub, []byte("bill"))
	assert.FatalError(t, err)
	assert.Equals(t, boogers, res)

	// verify that seb is in db
	res, err = db.Get(ub, []byte("sebastian"))
	assert.FatalError(t, err)
	assert.Equals(t, sebb, res)

	// check that the readMike update txn was successful
	assert.Equals(t, readMike.Result, mikeb)

	// check that the readSeb update txn was successful
	assert.Equals(t, readSeb.Result, sebb)

	// check that the casGates update txn was a successful write
	assert.True(t, casGates.Swapped)
	assert.Equals(t, casGates.Result, gatesb)

	// check that the casGates2 update txn was successful
	assert.True(t, casGates2.Swapped)
	assert.Equals(t, casGates2.Result, boogers)

	// check that the casGates3 update txn was did not update.
	assert.False(t, casGates3.Swapped)
	assert.Equals(t, casGates3.Result, boogers)

	// List //

	_, err = db.List([]byte("clever"))
	assert.True(t, IsErrNotFound(err))

	entries, err = db.List(ub)
	assert.FatalError(t, err)
	assert.Equals(t, len(entries), 4)

	// Update Again //

	// create txns for update test
	max := testUser{"max", "furman", 6}
	maxb, err := json.Marshal(max)
	assert.FatalError(t, err)
	maxey := testUser{"mike", "maxey", 3}
	maxeyb, err := json.Marshal(maxey)
	assert.FatalError(t, err)
	delMike := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("mike"),
		Cmd:    database.Delete,
	}
	setMax := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("max"),
		Value:  maxb,
		Cmd:    database.Set,
	}
	setMaxey := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("maxey"),
		Value:  maxeyb,
		Cmd:    database.Set,
	}
	delMaxey := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("maxey"),
		Cmd:    database.Delete,
	}
	delSeb := &database.TxEntry{
		Bucket: ub,
		Key:    []byte("sebastian"),
		Cmd:    database.Delete,
	}

	// update: read write multiple entries.
	tx = &database.Tx{Operations: []*database.TxEntry{
		delMike, setMax, setMaxey, delMaxey, delSeb,
	}}
	assert.Nil(t, db.Update(tx))

	entries, err = db.List(ub)
	assert.FatalError(t, err)
	assert.Equals(t, len(entries), 3)

	// verify that max and mariano are in the db
	res, err = db.Get(ub, []byte("max"))
	assert.FatalError(t, err)
	assert.Equals(t, maxb, res)
	res, err = db.Get(ub, []byte("mariano"))
	assert.FatalError(t, err)
	assert.Equals(t, marianob, res)

	assert.Nil(t, db.DeleteTable(ub))
	_, err = db.List(ub)
	assert.True(t, IsErrNotFound(err))
}

func TestMain(m *testing.M) {

	// setup
	path := "./tmp"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0755)
	}

	// run
	ret := m.Run()

	// teardown
	os.RemoveAll(path)

	os.Exit(ret)
}

func TestMySQL(t *testing.T) {
	var (
		uname = "user"
		pwd   = "password"
		proto = "tcp"
		addr  = "127.0.0.1:3306"
		//path   = "/tmp/mysql.sock"
		testDB = "test"
	)

	isCITest := os.Getenv("CI")
	if isCITest == "" {
		fmt.Printf("Not running MySql integration tests\n")
		return
	}

	db, err := New("mysql",
		fmt.Sprintf("%s:%s@%s(%s)/", uname, pwd, proto, addr),
		WithDatabase(testDB))
	assert.FatalError(t, err)
	defer db.Close()

	run(t, db)
}

func TestPostgreSQL(t *testing.T) {
	var (
		uname = "user"
		pwd   = "password"
		addr  = "127.0.0.1:5432"
		//path   = "/tmp/postgresql.sock"
		testDB = "test"
	)

	isCITest := os.Getenv("CI")
	if isCITest == "" {
		fmt.Printf("Not running PostgreSQL integration tests\n")
		return
	}

	db, err := New("postgresql",
		fmt.Sprintf("postgresql://%s:%s@%s/", uname, pwd, addr),
		WithDatabase(testDB))
	assert.FatalError(t, err)
	defer db.Close()

	run(t, db)
}

func TestBadger(t *testing.T) {
	path := "./tmp/badgerdb"

	if _, err := os.Stat(path); os.IsNotExist(err) {
		assert.FatalError(t, os.Mkdir(path, 0755))
	}

	db, err := New("badger", path, WithValueDir(path))
	assert.FatalError(t, err)
	defer db.Close()

	run(t, db)
}

func TestBolt(t *testing.T) {
	assert.FatalError(t, os.MkdirAll("./tmp", 0644))

	db, err := New("bbolt", "./tmp/boltdb")
	assert.FatalError(t, err)
	defer db.Close()

	run(t, db)
}

func TestIsErrNotFound(t *testing.T) {
	assert.True(t, IsErrNotFound(database.ErrNotFound))
	assert.True(t, IsErrNotFound(sql.ErrNoRows))
	assert.True(t, IsErrNotFound(fmt.Errorf("something failed: %w", database.ErrNotFound)))
	assert.True(t, IsErrNotFound(fmt.Errorf("something failed: %w", sql.ErrNoRows)))
	assert.False(t, IsErrNotFound(errors.New("not found")))
}
