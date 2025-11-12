package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"mess"
	"path/filepath"
	"time"
	"unsafe"

	"go.etcd.io/bbolt"
)

var messLogName = "messlog"

func (n *node) messlogf(format string, args ...any) {
	n.messlogErr(fmt.Errorf(format, args...))
}
func (n *node) messlogErr(err error) {
	s := err.Error()
	log.Println(s)
	if !n.dev {
		n.writeLogs(messLogName, messLogName, unsafe.Slice(unsafe.StringData(s), len(s)))
	}
}

func (n *node) openLog(realm, service string) (*bbolt.DB, error) {
	name := realm + "__" + service

	if v, ok := n.logs.Load(name); ok {
		return v.(*bbolt.DB), nil
	}

	opts := *bbolt.DefaultOptions
	opts.Timeout = 2 * time.Second
	dbPath := filepath.Join(n.logdir, name+".log")

	bolt, err := bbolt.Open(dbPath, 0600, &opts)
	if err != nil {
		return nil, err
	}

	n.logs.Store(name, bolt)

	return bolt, nil
}

var logBucket = []byte("log")

func (n *node) writeLogs(realm, service string, msgs ...[]byte) {
	db, err := n.openLog(realm, service)
	if err != nil {
		log.Printf("writing logs of %v@%v: db open: %v\n", service, realm, err)
		return
	}

	tx, err := db.Begin(true)
	if err != nil {
		log.Printf("writing logs of %v@%v: tx error: %v\n", service, realm, err)
		return
	}
	defer rollback(tx)

	b, err := tx.CreateBucketIfNotExists(logBucket)
	if err != nil {
		log.Printf("writing logs of %v@%v: creating bucket: %v\n", service, realm, err)
		return
	}

	for _, msg := range msgs {
		id, _ := b.NextSequence()
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, id)
		if err = b.Put(key, msg); err != nil {
			log.Printf("writing logs of %v@%v: %v\n", service, realm, err)
			return
		}
	}

	if b.Stats().KeyN-1_000_000 > 0 {
		c := b.Cursor()
		keys := make([][]byte, 0, 50_000)
		for k, _ := c.First(); k != nil && len(keys) < 50_000; k, _ = c.Next() {
			keys = append(keys, k)
		}
		for _, k := range keys {
			if err = b.Delete(k); err != nil {
				log.Printf("deleting older log records of %v@%v: %v\n", service, realm, err)
			}
		}
	}

	if err = tx.Commit(); err != nil {
		log.Printf("writing logs of %v@%v: commit error: %v\n", service, realm, err)
	}
}

func (n *node) readLogs(realm, service string, offset, limit uint64) ([]*mess.LogRecord, error) {

	db, err := n.openLog(realm, service)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}

	tx, err := db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	b := tx.Bucket(logBucket)
	if b == nil {
		return nil, nil
	}

	offsetKey := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetKey, offset+1)

	c := b.Cursor()
	k, v := c.Seek(offsetKey)
	if k == nil {
		return nil, nil
	}

	logs := make([]*mess.LogRecord, 0, limit)
	for ; k != nil && uint64(len(logs)) < limit; k, v = c.Next() {
		logs = append(logs, &mess.LogRecord{
			ID:   binary.BigEndian.Uint64(k),
			Data: bytes.Clone(v),
		})
	}
	return logs, nil
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }
