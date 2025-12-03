package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/monotime"
)

const logTTL = 7 * 24 * time.Hour

func (n *node) logf(format string, args ...any) {
	n.logErr(fmt.Errorf(format, args...))
}

func (n *node) logErr(err error) {
	s := err.Error()
	log.Println(s)
	if !n.dev {
		n.writeLogs(mess.ServiceName, mess.ServiceName, unsafe.Slice(unsafe.StringData(s), len(s)))
	}
}

func (n *node) getLogDB(realm, service string) (*dbInstance, error) {

	key := dbKey{
		realm: realm,
		name:  service,
	}
	if v, ok := n.logdb.Load(key); ok {
		return v.(*dbInstance), nil
	}

	n.logmu.Lock()
	defer n.logmu.Unlock()

	if v, ok := n.logdb.Load(key); ok {
		return v.(*dbInstance), nil
	}

	if realm == "" {
		realm = "default"
	}

	path := filepath.Join(n.logdir, realm, service)

	pdb, err := pebble.Open(path, pebbleLogOptions())
	if err != nil {
		return nil, err
	}

	iter, err := pdb.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("iterator: %w", err)
	}
	defer closeIter(iter)

	var seq *monotime.Gen

	if iter.Last() {
		if k := iter.Key(); len(k) == 8 {
			seq = monotime.NewGen(int64(binary.BigEndian.Uint64(k)))
		}
	}

	if seq == nil {
		seq = monotime.NewGen(0)
	}

	db := &dbInstance{DB: pdb, seq: seq}

	n.cleanupLogs(db)

	n.logdb.Store(key, db)

	return db, nil
}

func (n *node) deleteLog(realm, service string) error {

	key := dbKey{
		realm: realm,
		name:  service,
	}

	v, ok := n.logdb.Load(key)
	if !ok {
		return nil
	}

	if realm == "" {
		realm = "default"
	}

	if err := v.(*dbInstance).DB.Close(); err != nil {
		return fmt.Errorf("db close: %w", err)
	}

	n.logdb.Delete(key)

	if err := os.RemoveAll(filepath.Join(n.logdir, realm, service)); err != nil {
		return err
	}

	return nil
}

func (n *node) writeLogs(realm, service string, msgs ...[]byte) {
	if len(msgs) == 0 {
		return
	}

	db, err := n.getLogDB(realm, service)
	if err != nil {
		log.Printf("writing logs of %v: database: %v\n", internal.ServiceName(service, realm), err)
		return
	}

	batch := db.NewBatch()
	defer func(b *pebble.Batch) { _ = b.Close() }(batch)

	for _, msg := range msgs {

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(db.seq.Next()))

		if err = batch.Set(key, bytes.TrimSpace(msg), nil); err != nil {
			log.Printf("writing logs of %v: batch error: %v\n", internal.ServiceName(service, realm), err)
			return
		}
	}

	if err = batch.Commit(pebble.Sync); err != nil {
		log.Printf("writing logs of %v: commit error: %v\n", internal.ServiceName(service, realm), err)
	}

	if err == nil {
		if db.wcnt.Add(1)%1000 == 0 {
			n.cleanupLogs(db)
		}
	}
}

func (n *node) cleanupLogs(db *dbInstance) {
	var start, end [8]byte

	binary.BigEndian.PutUint64(start[:], uint64(time.Now().Add(-logTTL*100).UnixNano()))
	binary.BigEndian.PutUint64(end[:], uint64(time.Now().Add(-logTTL).UnixNano()))

	if err := db.DeleteRange(start[:], end[:], pebble.NoSync); err != nil {
		n.logf("error removing expired logs: %v", err)
	}
}

func (n *node) logsRequest(req *mess.LogsRequest) ([]mess.LogRecord, error) {

	if req.Service == mess.ServiceName {
		req.Realm = mess.ServiceName
	} else {
		if n.localServices.Load().getByRealmAndName(req.Realm, req.Service) == nil {
			return nil, fmt.Errorf("service %v not found", internal.ServiceName(req.Service, req.Realm))
		}
	}

	db, err := n.getLogDB(req.Realm, req.Service)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}

	var key []byte

	if req.Offset < 0 {
		key, err = findNegativeOffset(db, req.Offset)
		if err != nil {
			return nil, err
		}
	} else {
		key = make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(req.Offset))
	}

	logs := make([]mess.LogRecord, 0, req.Limit)

	_, logs, err = readLogs(db, key, logs, req, 0)

	return logs, err
}

func (n *node) logsStream(req *mess.LogsRequest) (Producer[mess.LogRecord], error) {

	if req.Service == mess.ServiceName {
		req.Realm = mess.ServiceName
	} else {
		if n.localServices.Load().getByRealmAndName(req.Realm, req.Service) == nil {
			return nil, fmt.Errorf("service %v not found", internal.ServiceName(req.Service, req.Realm))
		}
	}

	db, dberr := n.getLogDB(req.Realm, req.Service)
	if dberr != nil {
		return nil, fmt.Errorf("db open: %w", dberr)
	}

	var key []byte

	if req.Offset < 0 {
		key, dberr = findNegativeOffset(db, req.Offset)
		if dberr != nil {
			return nil, dberr
		}
	} else {
		key = make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(req.Offset))
	}

	producer := func(ctx context.Context, stream chan<- mess.LogRecord) {

		defer close(stream)

		sent := uint64(0)
		done := ctx.Done()

		var err error

		logs := make([]mess.LogRecord, 0, 64)

		for {
			key, logs, err = readLogs(db, key, logs, req, req.Limit-sent)

			for _, rec := range logs {
				select {
				case stream <- rec:
					sent++
					if req.Limit > 0 && sent >= req.Limit {
						return
					}
				case <-done:
					return
				}
			}

			if err != nil {
				n.logf("log stream error: %v", err)
				return
			}

			if len(logs) < cap(logs) {
				select {
				case <-time.After(time.Second):
				case <-done:
					return
				}
			}

			logs = logs[:0]
		}
	}

	return producer, nil
}

func readLogs(db *dbInstance, lastKnown []byte, destSlice []mess.LogRecord, req *mess.LogsRequest, atMost uint64) ([]byte, []mess.LogRecord, error) {

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lastKnown,
	})
	if err != nil {
		return lastKnown, destSlice, fmt.Errorf("iterator: %w", err)
	}
	defer closeIter(iter)

	added := uint64(0)
	mem := 0

	firstHandled := false
	for ok := iter.First(); ok; ok = iter.Next() {
		k := iter.Key()

		if len(k) != 8 {
			return lastKnown, destSlice, fmt.Errorf("invalid key len: %v", len(k))
		}

		if !firstHandled {
			firstHandled = true
			if bytes.Equal(lastKnown, k) {
				continue
			}
		}

		var v []byte
		v, err = iter.ValueAndErr()
		if err != nil {
			return lastKnown, destSlice, fmt.Errorf("value error: %w", err)
		}

		destSlice = append(destSlice, mess.LogRecord{
			ID:   int64(binary.BigEndian.Uint64(k)),
			Data: bytes.Clone(v),
		})
		copy(lastKnown, k)

		added++
		mem += len(v) // rough approx.

		if atMost > 0 && added >= atMost {
			break
		}
		if mem >= 10<<20 {
			break
		}

		if req.Limit > 0 && uint64(len(destSlice)) >= req.Limit {
			break
		}
	}
	return lastKnown, destSlice, nil
}

func findNegativeOffset(db *dbInstance, offset int64) (key []byte, err error) {

	iter, err := db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("iterator: %w", err)
	}
	defer closeIter(iter)

	for ok := iter.Last(); ok; ok = iter.Prev() {
		k := iter.Key()
		if len(k) != 8 {
			return nil, fmt.Errorf("invalid key len: %v", len(k))
		}
		if key == nil {
			key = make([]byte, len(k))
		}
		copy(key, k)

		offset++
		if offset > 0 { // offset + 1 because first element will be filtered out
			break
		}
	}
	return
}
