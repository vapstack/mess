package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/rosedblabs/rosedb/v2"
	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/monotime"
)

const (
	logTTL = 7 * 24 * time.Hour

	readLogsMax = 10_000
)

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

func (n *node) openLog(realm, service string) (*dbval, error) {

	key := dkey{
		realm: realm,
		name:  service,
	}
	if v, ok := n.logdb.Load(key); ok {
		return v.(*dbval), nil
	}

	n.logmu.Lock()
	defer n.logmu.Unlock()

	if v, ok := n.logdb.Load(key); ok {
		return v.(*dbval), nil
	}

	if realm == "" {
		realm = "default"
	}

	options := rosedb.DefaultOptions
	options.Sync = false
	options.SegmentSize = 256 * 1024 * 1024
	options.DirPath = filepath.Join(n.logdir, realm, service)
	options.AutoMergeCronExpr = toCronMinute(realm+service) + " 3 * * *"

	rose, err := rosedb.Open(options)
	if err != nil {
		return nil, err
	}

	var seq *monotime.Gen

	rose.Descend(func(k []byte, v []byte) (bool, error) {
		if len(k) == 8 {
			seq = monotime.NewGen(int64(binary.BigEndian.Uint64(k)))
		}
		return false, nil
	})

	if seq == nil {
		seq = monotime.NewGen(0)
	}

	db := &dbval{DB: rose, seq: seq}

	n.logdb.Store(key, db)

	return db, nil
}

func (n *node) deleteLog(realm, service string) error {

	key := dkey{
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

	if err := v.(*dbval).Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.RemoveAll(filepath.Join(n.logdir, realm, service)); err != nil {
		return err
	}

	return nil
}

func (n *node) writeLogs(realm, service string, msgs ...[]byte) {

	db, err := n.openLog(realm, service)
	if err != nil {
		log.Printf("writing logs of %v: db open: %v\n", internal.ServiceName(service, realm), err)
		return
	}

	batch := db.NewBatch(rosedb.BatchOptions{
		Sync:     false,
		ReadOnly: false,
	})
	defer func(b *rosedb.Batch) { _ = b.Rollback() }(batch)

	for _, msg := range msgs {

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(db.seq.Next()))

		if err = batch.PutWithTTL(key, bytes.TrimSpace(msg), logTTL); err != nil {
			log.Printf("writing logs of %v: batch error: %v\n", internal.ServiceName(service, realm), err)
			return
		}
	}

	if err = batch.Commit(); err != nil {
		log.Printf("writing logs of %v: commit error: %v\n", internal.ServiceName(service, realm), err)
	}
}

func (n *node) logsRequest(req *mess.LogsRequest) ([]mess.LogRecord, error) {

	// if req.Realm != mess.ServiceName || req.Service != mess.ServiceName {
	// 	if n.localServices.Load().getByRealmAndName(req.Realm, req.Service) == nil {
	// 		return nil, fmt.Errorf("service %v@%v not found", req.Service, req.Realm)
	// 	}
	// }

	if req.Service == mess.ServiceName {
		req.Realm = mess.ServiceName
	} else {
		if n.localServices.Load().getByRealmAndName(req.Realm, req.Service) == nil {
			return nil, fmt.Errorf("service %v not found", internal.ServiceName(req.Service, req.Realm))
		}
	}

	db, err := n.openLog(req.Realm, req.Service)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}

	if req.Limit == 0 {
		req.Limit = 100
	} else if req.Limit > readLogsMax {
		req.Limit = readLogsMax
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

	firstHandled := false
	db.AscendGreaterOrEqual(key, func(k []byte, v []byte) (bool, error) {
		if len(k) != 8 {
			err = errors.New("invalid key len")
			return false, err
		}
		if !firstHandled {
			firstHandled = true
			if bytes.Equal(key, k) {
				return true, nil
			}
		}
		logs = append(logs, mess.LogRecord{
			ID:   int64(binary.BigEndian.Uint64(k)),
			Data: v, // rosedb returns a copy
		})
		return uint64(len(logs)) < req.Limit, nil
	})

	return logs, err
}

func (n *node) logsStream(req *mess.LogsRequest) (Producer[mess.LogRecord], error) {

	// if /*req.Realm != mess.ServiceName || */ req.Service != mess.ServiceName {
	// 	if n.localServices.Load().getByRealmAndName(req.Realm, req.Service) == nil {
	// 		return nil, fmt.Errorf("service %v@%v not found", req.Service, req.Realm)
	// 	}
	// } else {
	// 	req.Realm = mess.ServiceName
	// }

	if req.Service == mess.ServiceName {
		req.Realm = mess.ServiceName
	} else {
		if n.localServices.Load().getByRealmAndName(req.Realm, req.Service) == nil {
			return nil, fmt.Errorf("service %v not found", internal.ServiceName(req.Service, req.Realm))
		}
	}

	db, dberr := n.openLog(req.Realm, req.Service)
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

		read := uint64(0)
		logs := make([]mess.LogRecord, 0, 64)
		done := ctx.Done()

		var err error
		for {
			firstHandled := false
			db.AscendGreaterOrEqual(key, func(k []byte, v []byte) (bool, error) {
				if len(k) != 8 {
					err = errors.New("invalid key len")
					return false, err
				}
				if !firstHandled {
					firstHandled = true
					if bytes.Equal(key, k) {
						return true, nil
					}
				}
				logs = append(logs, mess.LogRecord{
					ID:   int64(binary.BigEndian.Uint64(k)),
					Data: v, // rosedb returns a copy
				})
				key = k
				read++
				return len(logs) < cap(logs) && (req.Limit == 0 || read < req.Limit), nil
			})
			for _, rec := range logs {
				select {
				case stream <- rec:
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

func findNegativeOffset(db *dbval, offset int64) (key []byte, err error) {
	// DescendKeys returns only pattern-related errors
	_ = db.DescendKeys(nil, false, func(k []byte) (bool, error) {
		if len(k) != 8 {
			err = errors.New("invalid key len")
			return false, err
		}
		key = k
		offset++
		return offset <= 0, nil // offset + 1 because first element will be filtered out
	})
	return
}
