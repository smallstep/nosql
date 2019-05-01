package nosql

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/smallstep/assert"
	"github.com/smallstep/nosql/database"
)

type testUser struct {
	fname, lname string
	numPets      int
}

func run(t *testing.T, db database.DB) {
	ub := []byte("testNoSQLUsers")
	assert.True(t, IsErrNotFound(db.DeleteTable(ub)))
	assert.Nil(t, db.CreateTable(ub))

	entries, err := db.List(ub)
	assert.Nil(t, err)
	assert.Equals(t, len(entries), 0)

	// check for mike - should not exist
	_, err = db.Get(ub, []byte("mike"))
	assert.True(t, IsErrNotFound(err))

	// add mike
	mike := testUser{"mike", "malone", 1}
	mikeb, err := json.Marshal(mike)
	assert.FatalError(t, err)
	assert.Nil(t, db.Set(ub, []byte("mike"), mikeb))

	// verify that mike is in db
	res, err := db.Get(ub, []byte("mike"))
	assert.FatalError(t, err)
	assert.Equals(t, mikeb, res)

	var found bool
	// loadOrStore should load since mike already exists
	res, found, err = db.LoadOrStore(ub, []byte("mike"), mikeb)
	assert.FatalError(t, err)
	assert.Equals(t, mikeb, res)
	assert.True(t, found)
	assert.Nil(t, err)

	// delete mike
	assert.FatalError(t, db.Del(ub, []byte("mike")))

	// check for mike - should not exist
	_, err = db.Get(ub, []byte("mike"))
	assert.True(t, IsErrNotFound(err))

	// loadOrStore should store since mike does not exist
	res, found, err = db.LoadOrStore(ub, []byte("mike"), mikeb)
	assert.FatalError(t, err)
	assert.Nil(t, res)
	assert.False(t, found)
	assert.Nil(t, err)

	// delete mike
	assert.FatalError(t, db.Del(ub, []byte("mike")))

	/* Update */

	// create txns for update test
	mariano := testUser{"mariano", "Cano", 2}
	marianob, err := json.Marshal(mariano)
	assert.FatalError(t, err)
	seb := testUser{"sebastian", "tiedtke", 0}
	sebb, err := json.Marshal(seb)
	assert.FatalError(t, err)
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

	// update: read write multiple entries.
	tx := &database.Tx{Operations: []*database.TxEntry{setMike, setMariano, readMike, setSeb, readSeb}}
	assert.Nil(t, db.Update(tx))

	// verify that mike is in db
	res, err = db.Get(ub, []byte("mike"))
	assert.FatalError(t, err)
	assert.Equals(t, mikeb, res)

	// verify that mariano is in db
	res, err = db.Get(ub, []byte("mariano"))
	assert.FatalError(t, err)
	assert.Equals(t, marianob, res)

	// verify that seb is in db
	res, err = db.Get(ub, []byte("sebastian"))
	assert.FatalError(t, err)
	assert.Equals(t, sebb, res)

	// check that the readMike update txn was successful
	assert.Equals(t, readMike.Value, mikeb)

	// check that the readSeb update txn was successful
	assert.Equals(t, readSeb.Value, sebb)

	/* List */

	_, err = db.List([]byte("clever"))
	assert.True(t, IsErrNotFound(err))

	entries, err = db.List(ub)
	assert.FatalError(t, err)
	assert.Equals(t, len(entries), 3)

	/* Update Again */

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
	assert.Equals(t, len(entries), 2)

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
		uname = "travis"
		pwd   = ""
		proto = "tcp"
		addr  = "127.0.0.1:3306"
		//path   = "/tmp/mysql.sock"
		testDB = "test"
	)

	isTravisTest := os.Getenv("TRAVIS")
	if len(isTravisTest) == 0 {
		fmt.Printf("Not running MySql integration tests\n")
		return
	}

	db, err := New("mysql",
		//fmt.Sprintf("%s:%s@%s(%s)/", uname, pwd, proto, addr),
		fmt.Sprintf("%s:%s@%s(%s)/", uname, pwd, proto, addr),
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
