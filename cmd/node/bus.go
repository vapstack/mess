package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/monotime"
	"go.etcd.io/bbolt"

	"github.com/rosedblabs/rosedb/v2"
)

const eventTTL = 7 * 24 * time.Hour

const readEventsMax = 10_000

func (n *node) openBus(realm, topic string, autoCreate bool) (*dbval, error) {

	key := dkey{
		realm: realm,
		name:  topic,
	}
	if v, ok := n.busdb.Load(key); ok {
		return v.(*dbval), nil
	}

	n.busmu.Lock()
	defer n.busmu.Unlock()

	if v, ok := n.busdb.Load(key); ok {
		return v.(*dbval), nil
	}

	if realm == "" {
		realm = "default"
	}

	path := filepath.Join(n.busdir, realm, topic)

	if !autoCreate {
		if exists, _ := pathExists(path); !exists {
			return nil, nil
		}
	}

	options := rosedb.DefaultOptions
	options.Sync = true
	options.SegmentSize = 512 * 1024 * 1024
	options.DirPath = path
	options.AutoMergeCronExpr = toCronMinute(realm+topic) + " 4 * * *"

	rose, err := rosedb.Open(options)
	if err != nil {
		return nil, err
	}

	db := &dbval{DB: rose}

	n.busdb.Store(key, db)

	return db, nil
}

/*
func (n *node) deleteBus(realm, topic string) error {
	key := dkey{
		realm: realm,
		name:  topic,
	}
	v, ok := n.busdb.Load(key)
	if !ok {
		return nil
	}

	if realm == "" {
		realm = "default"
	}
	dbpath := filepath.Join(n.logdir, realm, topic)
	if err := v.(*dbval).Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := os.RemoveAll(dbpath); err != nil {
		return err
	}
	return nil
}
*/

var pubBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func getBusBuf() *bytes.Buffer { return pubBufPool.Get().(*bytes.Buffer) }
func putBusBuf(b *bytes.Buffer) {
	b.Reset()
	pubBufPool.Put(b)
}

func (n *node) publish(realm string, req *mess.PublishRequest) error {

	if len(req.Events) == 0 {
		return nil
	}

	for i, data := range req.Events {
		if len(data) > mess.MaxEventSize {
			return fmt.Errorf("max event size is %v, got %v (index: %v)", mess.MaxEventSize, len(data), i)
		}
	}

	db, err := n.openBus(realm, req.Topic, true)
	if err != nil {
		return err
	}

	metabuf := getBusBuf()
	defer putBusBuf(metabuf)

	if err = gob.NewEncoder(metabuf).Encode(req.Meta); err != nil {
		return fmt.Errorf("error encoding field data: %w", err)
	}
	metadata := metabuf.Bytes()

	bufs := make([]*bytes.Buffer, 0, len(req.Events))
	defer func() {
		for _, buf := range bufs {
			putBusBuf(buf)
		}
	}()

	batch := db.NewBatch(rosedb.DefaultBatchOptions)
	defer func(b *rosedb.Batch) { _ = b.Rollback() }(batch)

	var id monotime.UUID

	for _, data := range req.Events {

		if id, err = n.busids.Next(); err != nil {
			return fmt.Errorf("error reserving event id: %w", err)
		}

		buf := getBusBuf()
		bufs = append(bufs, buf)

		buf.Write(metadata)
		buf.Write(data)

		if err = batch.PutWithTTL(id[:], buf.Bytes(), eventTTL); err != nil {
			return fmt.Errorf("write error: %w", err)
		}
	}

	// todo: notify or somehow increase counters/metrics

	if err = batch.Commit(); err != nil {
		return fmt.Errorf("db commit error: %w", err)
	}

	n.busTopics.reg(realm, req.Topic, id) // ids[len(ids)-1])

	return nil
}

// func (n *node) emit(realm, sender string, topic string, payload []byte) error {
//
// }
//
// func (n *node) emitEvents(realm, sender string, events []*mess.Event) error {
//
// }

func (n *node) eventsRequest(req *mess.EventsRequest) ([]mess.Event, error) {

	db, err := n.openBus(req.Realm, req.Topic, false)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}
	if db == nil {
		return nil, nil
	}

	if req.Limit == 0 {
		req.Limit = 100
	} else if req.Limit > 1000 {
		req.Limit = 1000
	}

	var meta []mess.MetaField

	events := make([]mess.Event, 0, req.Limit)

	firstHandled := false
	db.AscendGreaterOrEqual(req.Offset[:], func(k []byte, v []byte) (bool, error) {
		if len(k) != 16 {
			err = errors.New("invalid key len")
			return false, err
		}
		if !firstHandled {
			firstHandled = true
			if bytes.Equal(req.Offset[:], k) {
				return true, nil
			}
		}

		r := bytes.NewReader(v)

		meta = meta[:0]

		if err = gob.NewDecoder(r).Decode(&meta); err != nil {
			err = fmt.Errorf("error decoding meta: %w", err)
			return false, err
		}

		if !metaMatch(meta, req.Filter) {
			return true, nil
		}

		events = append(events, mess.Event{
			ID:    monotime.UUID(bytes.Clone(k)),
			Topic: req.Topic,
			Data:  v[len(v)-r.Len():],
		})

		return uint64(len(events)) < req.Limit, nil
	})

	return events, err
}

func (n *node) eventsStream(req *mess.EventsRequest) (Producer[mess.Event], error) {

	db, dberr := n.openBus(req.Realm, req.Topic, false)
	if dberr != nil {
		return nil, fmt.Errorf("db open: %w", dberr)
	}
	if db == nil {
		return nil, errors.New("topic not found")
	}

	producer := func(ctx context.Context, stream chan<- mess.Event) {

		defer close(stream)

		read := uint64(0)
		done := ctx.Done()

		key := req.Offset

		events := make([]mess.Event, 0, 64)

		var meta []mess.MetaField
		var err error
		for {
			firstHandled := false
			db.AscendGreaterOrEqual(key[:], func(k []byte, v []byte) (bool, error) {
				if len(k) != 16 {
					err = errors.New("invalid key len")
					return false, err
				}
				if !firstHandled {
					firstHandled = true
					if bytes.Equal(key[:], k) {
						return true, nil
					}
				}

				r := bytes.NewReader(v)

				meta = meta[:0]

				if err = gob.NewDecoder(r).Decode(&meta); err != nil {
					err = fmt.Errorf("error decoding meta: %w", err)
					return false, err
				}

				key = monotime.UUID(bytes.Clone(k))

				if !metaMatch(meta, req.Filter) {
					return true, nil
				}

				events = append(events, mess.Event{
					ID:    key,
					Topic: req.Topic,
					Data:  v[len(v)-r.Len():], // rosedb returns a copy
				})

				read++

				return len(events) < cap(events) && (req.Limit == 0 || read < req.Limit), nil
			})

			for _, event := range events {
				select {
				case stream <- event:
				case <-done:
					return
				}
			}

			if err != nil {
				n.logf("log stream error: %v", err)
				return
			}

			if len(events) < cap(events) {
				select {
				case <-time.After(time.Second):
				case <-done:
					return
				}
			}

			events = events[:0]
		}
	}

	return producer, nil
}

func metaMatch(meta []mess.MetaField, filter []mess.MetaFilter) bool {
	for _, f := range filter {

		var value string

		for _, mv := range meta {
			if mv.Key == f.Key {
				value = mv.Value
				break
			}
		}

		if len(f.Any) > 0 {
			if value == "" {
				return false
			}
			match := false
			for _, wanted := range f.Any {
				if value == wanted {
					match = true
					break
				}
			}
			if !match {
				return false
			}
		}

		if len(f.Not) > 0 && value != "" {
			for _, excluded := range f.Not {
				if value == excluded {
					return false
				}
			}
		}
	}
	return true
}

func (n *node) receiveRequest(realm string, req *mess.ReceiveRequest) ([]mess.Event, error) {
	return nil, fmt.Errorf("not implemented")
}

func (n *node) eventProducerList(realm, topic string) []uint64 {
	list := make([]uint64, 0)

	if db, _ := n.openBus(realm, topic, false); db != nil {
		list = append(list, n.id)
	}

	for _, remote := range n.state.Load().Map {
		if remote.Publish.Has(realm, topic) {
			list = append(list, remote.ID)
		}
	}

	slices.Sort(list)

	return list
}

func (n *node) hasNewProducers(realm, topic string, active []uint64) bool {
	collected := n.eventProducerList(realm, topic)
	return hasNewIDs(active, collected)
}

func hasNewIDs(oldids, newids []uint64) bool {
	i, j := 0, 0
	for i < len(oldids) && j < len(newids) {
		av, bv := oldids[i], newids[j]
		switch {
		case bv == av:
			i++
			j++
		case bv > av:
			i++
		default:
			return true
		}
	}
	return j < len(newids)
}

func (n *node) collectEventProducers(realm string, req *mess.ReceiveRequest) ([]uint64, []Producer[mess.Event], error) {

	producers := make([]Producer[mess.Event], 0)
	list := make([]uint64, 0)

	var errlist []error

	if db, _ := n.openBus(realm, req.Topic, false); db != nil {
		p, err := n.eventsStream(&mess.EventsRequest{
			Realm:  realm,
			Topic:  req.Topic,
			Offset: req.Cursor.ExtractNode(n.id),
			Limit:  req.Limit,
			Stream: req.Stream,
			Filter: req.Filter,
		})
		if err != nil {
			errlist = append(errlist, fmt.Errorf("error creating event stream from local bus: %w", err))
		} else {
			producers = append(producers, p)
			list = append(list, n.id)
		}
	}

	for _, remote := range n.state.Load().Map {
		if remote.Publish.Has(realm, req.Topic) {
			p, err := n.remoteEventStream(remote, &mess.EventsRequest{
				Realm:  realm,
				Topic:  req.Topic,
				Offset: req.Cursor.ExtractNode(remote.ID),
				Limit:  req.Limit,
				Stream: req.Stream,
				Filter: req.Filter,
			})
			if err != nil {
				errlist = append(errlist, fmt.Errorf("error creating event stream from node %v: %w", remote.ID, err))
			} else {
				producers = append(producers, p)
				list = append(list, remote.ID)
			}
		}
	}

	slices.Sort(list)

	return list, producers, nil
}

func (n *node) remoteEventStream(tn *mess.Node, r *mess.EventsRequest) (Producer[mess.Event], error) {
	addr := tn.Address()
	if addr == "" {
		return nil, errors.New("no address known")
	}

	p := func(ctx context.Context, ch chan<- mess.Event) {

		defer close(ch)

		done := ctx.Done()
		offset := r.Offset
		backoff := time.Second

		for {
			if ctx.Err() != nil {
				return
			}
			r.Offset = offset

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(r); err != nil {
				n.logf("remote stream: error encoding events request: %v", err)
				return
			}

			dst := fmt.Sprintf("https://%v:%v/events", addr, mess.PublicPort)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, &buf)
			if err != nil {
				n.logf("remote stream: error building request to %v: %v", dst, err)
				return
			}
			req.Header.Set(mess.TargetNodeHeader, strconv.FormatUint(tn.ID, 10))

			res, err := n.client.Do(req)
			if err != nil {
				n.logf("remote stream: error dialing node %v: %v", tn.ID, err)

				select {
				case <-done:
					return
				case <-time.After(backoff):
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}

			backoff = time.Second

			func() {
				defer internal.DrainAndCloseBody(res)

				if res.StatusCode != http.StatusOK {
					n.logf("remote stream: node %v returned %v", tn.ID, res.Status)
					return
				}

				dec := gob.NewDecoder(res.Body)

				for {
					select {
					case <-done:
						return
					default:
					}

					var ev mess.Event
					if err := dec.Decode(&ev); err != nil {
						if !errors.Is(err, io.EOF) {
							n.logf("remote stream: decode error: %v", err)
						}
						return
					}

					select {
					case ch <- ev:
						offset = ev.ID
					case <-done:
						return
					}
				}
			}()
		}
	}

	return p, nil
}

func (n *node) receiveStream(realm string, req *mess.ReceiveRequest) (Producer[mess.Event], error) {

	list, producers, err := n.collectEventProducers(realm, req)
	if err != nil {
		return nil, err
	}

	p := func(ctx context.Context, out chan<- mess.Event) {

		defer close(out)

		done := ctx.Done()
		chans := make([]<-chan mess.Event, 0, len(producers))

		for _, p := range producers {
			pch := make(chan mess.Event, 32)
			chans = append(chans, pch)
			go p(ctx, pch)
		}

		stream := mergeChans(chans...)

		// producers receive the same context and should cancel themselves

		tick := time.NewTicker(time.Minute)
		sent := uint64(0)
		for {
			select {
			case v, ok := <-stream:
				if !ok {
					return
				}
				select {
				case out <- v:
					sent++
				case <-done:
					return
				}
				if req.Limit > 0 && sent >= req.Limit {
					return
				}
			case <-tick.C:
				if n.hasNewProducers(realm, req.Topic, list) {
					return
				}
			case <-done:
				return
			}
		}
	}
	return p, nil
}

func mergeChans[T any](chans ...<-chan T) <-chan T {
	out := make(chan T, len(chans))

	var wg sync.WaitGroup

	wg.Add(len(chans))

	for _, ch := range chans {
		go func(c <-chan T) {
			defer wg.Done()
			for v := range c {
				out <- v
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

/**/

type monobolt struct {
	mono *monotime.GenUUID
	bolt *bbolt.DB
	buck []byte
	key  []byte
}

func newMonoBolt(nodeID int, filename string) (*monobolt, error) {
	bolt, err := bbolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("initialization error: %w", err)
	}
	buck := []byte("mono")
	key := []byte("uuid")
	var uuid monotime.UUID

	err = bolt.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(buck)
		if err != nil {
			return err
		}
		if prev := b.Get(key); len(prev) > 0 {
			if len(prev) != 16 {
				return fmt.Errorf("stored value is invalid: wrong length")
			}
			uuid = monotime.UUID(prev)
			if !uuid.Valid() {
				return fmt.Errorf("stored value is invalid: wrong format")
			}
			if uuid.NodeID() != nodeID {
				return fmt.Errorf("node id mismatch: stored: %v, provided: %v", uuid.NodeID(), nodeID)
			}
		} else {
			uuid = monotime.ZeroUUID
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	g, err := monotime.NewGenUUID(nodeID, uuid)
	if err != nil {
		return nil, fmt.Errorf("monotime error: %w", err)
	}
	m := &monobolt{
		mono: g,
		bolt: bolt,
		buck: buck,
		key:  key,
	}
	return m, nil
}

func (m *monobolt) Next() (monotime.UUID, error) {
	var uuid monotime.UUID
	tx, err := m.bolt.Begin(true)
	if err != nil {
		return uuid, err
	}
	defer rollback(tx)

	uuid = m.mono.Next()
	if err = tx.Bucket(m.buck).Put(m.key, uuid[:]); err != nil {
		return uuid, err
	}
	return uuid, tx.Commit()
}

func (m *monobolt) close() error {
	return m.bolt.Close()
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }

/**/

type topicTracker struct {
	topics sync.Map
}

func (tt *topicTracker) each(fn func(realm, topic string, uuid monotime.UUID)) {
	tt.topics.Range(func(key, value any) bool {
		k := key.(dkey)
		t := value.(*atomic.Value).Load().(monotime.UUID)
		fn(k.realm, k.name, t)
		return true
	})
}

func (tt *topicTracker) reg(realm, topic string, uuid monotime.UUID) {
	key := dkey{
		realm: realm,
		name:  topic,
	}
	// the most recent uuid is not required, so last write wins
	v, ok := tt.topics.Load(key)
	if ok {
		v.(*atomic.Value).Store(uuid)
	} else {
		t := new(atomic.Value)
		t.Store(uuid)
		tt.topics.Store(key, t)
	}
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
