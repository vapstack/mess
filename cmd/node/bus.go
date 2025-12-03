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

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/bbolt"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/monotime"
)

const eventTTL = 7 * 24 * time.Hour

func (n *node) getBusDB(realm, topic string, autoCreate bool) (*dbInstance, error) {

	key := dbKey{
		realm: realm,
		name:  topic,
	}
	if v, ok := n.busdb.Load(key); ok {
		return v.(*dbInstance), nil
	}

	n.busmu.Lock()
	defer n.busmu.Unlock()

	if v, ok := n.busdb.Load(key); ok {
		return v.(*dbInstance), nil
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

	pdb, err := pebble.Open(path, pebbleBusOptions())
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}

	db := &dbInstance{
		DB: pdb,
	}

	n.cleanupEvents(db)

	n.busdb.Store(key, db)

	return db, nil
}

/*
func (n *node) deleteBus(realm, topic string) error {
	key := dbKey{
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
	if err := v.(*pebble.DB).Close(); err != nil {
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

	db, err := n.getBusDB(realm, req.Topic, true)
	if err != nil {
		return err
	}

	metaBuf := getBusBuf()
	defer putBusBuf(metaBuf)

	if err = gob.NewEncoder(metaBuf).Encode(req.Meta); err != nil {
		return fmt.Errorf("error encoding field data: %w", err)
	}
	metadata := metaBuf.Bytes()

	bufs := make([]*bytes.Buffer, 0, len(req.Events))
	defer func() {
		for _, buf := range bufs {
			putBusBuf(buf)
		}
	}()

	batch := db.NewBatch()
	defer closeBatch(batch)

	var id monotime.UUID

	for _, data := range req.Events {

		if id, err = n.busids.Next(); err != nil {
			return fmt.Errorf("error reserving event id: %w", err)
		}

		buf := getBusBuf()
		bufs = append(bufs, buf)

		buf.Write(metadata)
		buf.Write(data)

		if err = batch.Set(id[:], buf.Bytes(), nil); err != nil {
			return fmt.Errorf("write error: %w", err)
		}
	}

	// todo: notify or somehow increase counters/metrics

	if err = batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("db commit error: %w", err)
	}

	n.busTopics.reg(realm, req.Topic, id)

	if db.wcnt.Add(1)%5000 == 0 {
		n.cleanupEvents(db)
	}

	return nil
}

func (n *node) cleanupEvents(db *dbInstance) {
	start := monotime.MinUUID(int(n.id), time.Now().Add(-eventTTL*10))
	end := monotime.MinUUID(int(n.id), time.Now().Add(-eventTTL))
	if err := db.DeleteRange(start[:], end[:], pebble.NoSync); err != nil {
		n.logf("error removing expired events: %v", err)
	}
}

func (n *node) eventsRequest(req *mess.EventsRequest) ([]mess.Event, error) {

	db, err := n.getBusDB(req.Realm, req.Topic, false)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}
	if db == nil {
		return nil, nil
	}

	if req.Offset == monotime.ZeroUUID {
		req.Offset, err = getLastBusID(db)
		if err != nil {
			return nil, err
		}
	}

	events := make([]mess.Event, 0, req.Limit)

	_, events, err = readEvents(db, req.Offset, events, req, 0)

	return events, err
}

func (n *node) eventsStream(req *mess.EventsRequest) (Producer[mess.Event], error) {

	db, err := n.getBusDB(req.Realm, req.Topic, false)
	if err != nil {
		return nil, fmt.Errorf("db open: %w", err)
	}
	if db == nil {
		return nil, errors.New("topic not found")
	}

	if req.Offset == monotime.ZeroUUID {
		req.Offset, err = getLastBusID(db)
		if err != nil {
			return nil, err
		}
	}

	producer := func(ctx context.Context, stream chan<- mess.Event) {

		defer close(stream)

		sent := uint64(0)
		done := ctx.Done()

		key := req.Offset

		var err error

		events := make([]mess.Event, 0, 64)

		for {
			key, events, err = readEvents(db, key, events, req, req.Limit-sent)

			for _, event := range events {
				select {
				case stream <- event:
					sent++
					if req.Limit > 0 && sent >= req.Limit {
						return
					}
				case <-done:
					return
				}
			}

			if err != nil {
				n.logf("event stream error: %v", err)
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

func getLastBusID(db *dbInstance) (monotime.UUID, error) {
	zero := monotime.ZeroUUID

	iter, err := db.NewIter(nil)
	if err != nil {
		return zero, fmt.Errorf("iterator: %w", err)
	}
	defer closeIter(iter)

	if iter.Last() {
		if k := iter.Key(); len(k) != 16 {
			return zero, fmt.Errorf("invalid key len: %v", len(k))
		} else {
			return monotime.UUID(k), nil
		}
	}
	return zero, nil
}

var metaPool = sync.Pool{
	New: func() any {
		s := make([]mess.MetaField, 0, 8)
		return &s
	},
}

func readEvents(db *dbInstance, lastKnown monotime.UUID, destSlice []mess.Event, req *mess.EventsRequest, atMost uint64) (monotime.UUID, []mess.Event, error) {

	firstHandled := false

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lastKnown[:],
	})
	if err != nil {
		return lastKnown, destSlice, fmt.Errorf("iterator: %w", err)
	}
	defer closeIter(iter)

	meta := metaPool.Get().(*[]mess.MetaField)
	defer metaPool.Put(meta)

	added := uint64(0)
	mem := 0

	for ok := iter.First(); ok; ok = iter.Next() {

		k := iter.Key()

		if len(k) != 16 {
			return lastKnown, destSlice, fmt.Errorf("invalid key len: %v", len(k))
		}

		if !firstHandled {
			firstHandled = true
			if bytes.Equal(lastKnown[:], k) {
				continue
			}
		}

		var v []byte
		v, err = iter.ValueAndErr()
		if err != nil {
			return lastKnown, destSlice, fmt.Errorf("value error: %w", err)
		}

		r := bytes.NewReader(v)
		*meta = (*meta)[:0]

		if err = gob.NewDecoder(r).Decode(meta); err != nil {
			return lastKnown, destSlice, fmt.Errorf("error decoding meta: %w", err)
		}

		lastKnown = monotime.UUID(k)

		if !metaMatch(*meta, req.Filter) {
			continue
		}

		destSlice = append(destSlice, mess.Event{
			ID:    lastKnown,
			Topic: req.Topic,
			Meta:  slices.Clone(*meta),
			Data:  bytes.Clone(v[len(v)-r.Len():]),
		})

		added++
		mem += len(v) // rough approx.

		if atMost > 0 && added >= atMost {
			break
		}
		if mem >= 32<<20 {
			break
		}

		if len(destSlice) == cap(destSlice) {
			break
		}
	}

	return lastKnown, destSlice, nil
}

// func (n *node) emit(realm, sender string, topic string, payload []byte) error {
//
// }
//
// func (n *node) emitEvents(realm, sender string, events []*mess.Event) error {
//
// }
//
// func (n *node) receiveRequest(realm string, req *mess.SubscribeRequest) ([]mess.Event, error) {
//
// }

func (n *node) eventProducerList(realm, topic string) []uint64 {
	list := make([]uint64, 0)

	if db, _ := n.getBusDB(realm, topic, false); db != nil {
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

func (n *node) hasNewProducers(realm, topic string, active []uint64) bool {
	collected := n.eventProducerList(realm, topic)
	return hasNewIDs(active, collected)
}

func hasNewIDs(oldIDs, newIDs []uint64) bool {
	i, j := 0, 0
	for i < len(oldIDs) && j < len(newIDs) {
		av, bv := oldIDs[i], newIDs[j]
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
	return j < len(newIDs)
}

func (n *node) collectEventProducers(realm string, req *mess.SubscribeRequest) ([]uint64, []Producer[mess.Event], error) {

	producers := make([]Producer[mess.Event], 0)
	list := make([]uint64, 0)

	var errs []error

	if db, _ := n.getBusDB(realm, req.Topic, false); db != nil {
		p, err := n.eventsStream(&mess.EventsRequest{
			Realm:  realm,
			Topic:  req.Topic,
			Offset: req.Cursor.ExtractNode(n.id),
			Limit:  0,
			Stream: true,
			Filter: req.Filter,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("error creating event stream from local bus: %w", err))
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
				Limit:  0,
				Stream: true,
				Filter: req.Filter,
			})
			if err != nil {
				errs = append(errs, fmt.Errorf("error creating event stream from node %v: %w", remote.ID, err))
			} else {
				producers = append(producers, p)
				list = append(list, remote.ID)
			}
		}
	}

	if len(producers) == 0 {
		if len(errs) > 0 {
			return nil, nil, errs[0]
		}
		return nil, nil, nil
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

		buf := new(bytes.Buffer)
		done := ctx.Done()
		offset := r.Offset
		backoff := time.Second
		maxBackoff := 30 * time.Second

		for {
			if ctx.Err() != nil {
				return
			}

			r.Offset = offset

			buf.Reset()

			if err := gob.NewEncoder(buf).Encode(r); err != nil {
				n.logf("remote stream: error encoding events request: %v", err)
				return
			}

			dst := "https://" + addr + publicPortStr + "/events"

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, buf)
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
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
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

func (n *node) subscriptionStream(realm string, req *mess.SubscribeRequest) (Producer[mess.Event], error) {

	list, producers, err := n.collectEventProducers(realm, req)
	if err != nil {
		return nil, err
	}
	if len(producers) == 0 {
		return nil, fmt.Errorf("no event producers found for topic %v", req.Topic)
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
		for {
			select {
			case v, ok := <-stream:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-done:
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

type monoBolt struct {
	mono *monotime.GenUUID
	bolt *bbolt.DB
	buck []byte
	key  []byte
}

func newMonoBolt(nodeID int, filename string) (*monoBolt, error) {
	opts := *bbolt.DefaultOptions
	opts.Timeout = time.Second
	bolt, err := bbolt.Open(filename, 0o600, &opts)
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
	m := &monoBolt{
		mono: g,
		bolt: bolt,
		buck: buck,
		key:  key,
	}
	return m, nil
}

func (m *monoBolt) Next() (monotime.UUID, error) {
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

func (m *monoBolt) close() error {
	return m.bolt.Close()
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }

/**/

type topicTracker struct {
	topics sync.Map
}

func (tt *topicTracker) each(fn func(realm, topic string, uuid monotime.UUID)) {
	tt.topics.Range(func(key, value any) bool {
		k := key.(dbKey)
		t := value.(*atomic.Value).Load().(monotime.UUID)
		fn(k.realm, k.name, t)
		return true
	})
}

func (tt *topicTracker) reg(realm, topic string, uuid monotime.UUID) {
	key := dbKey{
		realm: realm,
		name:  topic,
	}
	// the most recent uuid is not required, so the last write wins
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
