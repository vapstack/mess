package mess

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/vapstack/monotime"
	"go.etcd.io/bbolt"
)

type (
	Event struct {

		// ID is the unique event identifier in UUID v7 format. Set by the node.
		// It contains the event time and the ID of the node that published the event.
		// This ID is guaranteed to be unique among IDs published by the node whose ID is stored in the UUID.
		ID monotime.UUID `json:"id"`

		// Topic on which the event was published. Required.
		Topic string `json:"topic"`

		// Meta holds user-provided metadata.
		Meta []MetaField `json:"meta,omitempty"`

		// Data is the event payload.
		Data json.RawMessage `json:"data"`
	}

	MetaField struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	PublishRequest struct {
		Topic  string            `json:"topic"`
		Meta   []MetaField       `json:"meta"`
		Events []json.RawMessage `json:"events"`
	}

	SubscribeRequest struct {
		Topic  string       `json:"topic"`
		Cursor EventCursor  `json:"cursor"`
		Filter []MetaFilter `json:"filter"`

		// Limit  uint64 `json:"limit"`
		// Stream bool   `json:"stream"`
	}

	MetaFilter struct {
		Key string   `json:"key"`
		Any []string `json:"any"`
		Not []string `json:"not"`
	}

	EmitRequest struct {
		Topic  string            `json:"topic"`
		Meta   []MetaField       `json:"meta"`
		Events []json.RawMessage `json:"events"`
	}

	ListenRequest struct {
		Topic string
	}

	EventsRequest struct {
		Realm  string        `json:"realm"`
		Topic  string        `json:"topic"`
		Offset monotime.UUID `json:"offset"`
		Limit  uint64        `json:"limit"`
		Filter []MetaFilter  `json:"filter"`
		Stream bool          `json:"stream"`
	}
)

type EventCursor []monotime.UUID

const MaxEventSize = 16 << 20

func (ec EventCursor) Update(uuid monotime.UUID) EventCursor {
	nodeID := uuid.NodeID()
	for i, u := range ec {
		if u.NodeID() == nodeID {
			ec[i] = uuid
			return ec
		}
	}
	return append(ec, uuid)
}

func (ec EventCursor) ExtractNode(id uint64) monotime.UUID {
	for _, u := range ec {
		if uint64(u.NodeID()) == id {
			return u
		}
	}
	return monotime.ZeroUUID
}

/**/

// Subscription represents a persistent event subscription on a single topic.
type Subscription struct {
	api    API
	topic  string
	stored *boltCursor
	mu     sync.Mutex
	closed bool
}

// Receive dispatches new events to the provided handler.
// The optional filter is used for server-side filtering of incoming events.
//
// Receive blocks until one of the following happens:
//   - the handler returns false, indicating that processing should stop;
//   - the handler returns a non-nil error, which is then returned by Receive;
//   - the context is canceled or reaches its deadline.
//
// Any panic in the handler is recovered and returned as an error.
//
// Subsequent calls to Receive on the same Subscription block until the
// previous call has returned.
func (s *Subscription) Receive(ctx context.Context, filter []MetaFilter, handler func(*Event) (bool, error)) (err error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("subscription is closed")
	}

	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()

	req := SubscribeRequest{
		Topic:  s.topic,
		Cursor: s.stored.get(),
		Filter: filter,
	}

	return s.api.Subscribe(ctx, req, func(event *Event) (bool, error) {
		ok, herr := handler(event)
		if ok && herr == nil {
			if serr := s.stored.update(event.ID); serr != nil {
				return false, fmt.Errorf("error persisting cursor: %w", serr)
			}
		}
		return ok, herr
	})
}

// Cursor returns a snapshot of the current global position of the subscription.
// The returned cursor can be used on any node to resume event processing
// from the current position of this subscription.
func (s *Subscription) Cursor() EventCursor {
	return s.stored.get()
}

// Topic returns the subscription topic.
func (s *Subscription) Topic() string {
	return s.topic
}

// Close closes the underlying cursor store.
// Close blocks until any active Receive calls return.
func (s *Subscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.stored.close()
}

/**/

type boltCursor struct {
	cursor atomic.Value
	bolt   *bbolt.DB
	buck   []byte
	key    []byte
}

func newBoltCursor(topic string, start EventCursor, filename string) (*boltCursor, error) {
	bolt, err := bbolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("initialization error: %w", err)
	}

	buck := []byte("mono")
	key := []byte("cur")

	var cur EventCursor

	err = bolt.Update(func(tx *bbolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists(buck)
		if err != nil {
			return err
		}

		tkey := []byte("topic")
		tbytes := b.Get(tkey)

		if len(tbytes) > 0 {
			t := string(tbytes)
			if t != topic {
				return fmt.Errorf("topic mismatch: stored: %v, provided: %v", t, topic)
			}

		} else {
			if err := b.Put(tkey, []byte(topic)); err != nil {
				return err
			}
		}

		if prev := b.Get(key); len(prev) > 0 {
			if len(prev)%16 != 0 {
				return fmt.Errorf("stored value is invalid: wrong length")
			}
			for i := 0; i < len(prev); i += 16 {
				cur = append(cur, monotime.UUID(prev[i:i+16]))
			}

		} else {
			v := make([]byte, 0, len(start)*16)
			for _, id := range start {
				v = append(v, id[:]...)
			}
			if err := b.Put(key, v); err != nil {
				return err
			}
			cur = start
		}

		return nil
	})
	if err != nil {
		_ = bolt.Close()
		return nil, err
	}

	m := &boltCursor{
		bolt: bolt,
		buck: buck,
		key:  key,
	}

	m.cursor.Store(cur)

	return m, nil
}

func (c *boltCursor) get() EventCursor {
	return slices.Clone(c.cursor.Load().(EventCursor))
}

func (c *boltCursor) close() error {
	return c.bolt.Close()
}

func (c *boltCursor) update(uuid monotime.UUID) error {
	if !uuid.Valid() {
		return fmt.Errorf("UUID is not valid")
	}

	cur := c.cursor.Load().(EventCursor).Update(uuid)

	c.cursor.Store(cur)

	tx, err := c.bolt.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	v := make([]byte, 0, len(cur)*16)
	for _, id := range cur {
		v = append(v, id[:]...)
	}

	if err = tx.Bucket(c.buck).Put(c.key, v); err != nil {
		return err
	}

	return tx.Commit()
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }
