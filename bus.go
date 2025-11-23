package mess

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

	EmitRequest struct {
		Topic  string            `json:"topic"`
		Meta   []MetaField       `json:"meta"`
		Events []json.RawMessage `json:"events"`
	}

	ReceiveRequest struct {
		Topic  string      `json:"topic"`
		Cursor EventCursor `json:"cursor"`
		Limit  uint64      `json:"limit"`
		Stream bool        `json:"stream"`

		Filter []MetaFilter `json:"filter"`
	}

	EventsRequest struct {
		Realm  string        `json:"realm"`
		Topic  string        `json:"topic"`
		Offset monotime.UUID `json:"offset"`
		Limit  uint64        `json:"limit"`
		Stream bool          `json:"stream"`

		Filter []MetaFilter `json:"filter"`
	}

	MetaFilter struct {
		Key string   `json:"key"`
		Any []string `json:"any"`
		Not []string `json:"not"`
	}
)

type EventCursor []monotime.UUID

const MaxEventSize = 1 << 24 // 16 Mb

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

// Subscription listens for published events and passes them to handler.
// It automatically handles event cursor persistence.
// For emitted events use Receiver.
type Subscription struct {
	topic   string
	stored  *boltcursor
	handler func()
	running atomic.Bool
}

/*
	s.bus.Subscribe(vap.EventName[amk.UserCreated](), cursor, s.userCreated)
*/

// Receive dispatches new events to the provided handler.
// If handler returns false, processing stops.
// Otherwise, the event is considered handled and the subscription cursor is updated.
// Receive does not block. Calling Receive multiple times on the same subscription results in error.
// Only one active handler is allowed. Calling Receive multiple time with an already active handler is a no-op.
func (s *Subscription) Receive(handler func(*Event) bool) (err error) {
	if !s.running.CompareAndSwap(false, true) {
		return errors.New("handler is already registered")
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
		}
		s.running.Store(false)
	}()

	ok, herr := fn(event)
	if herr != nil {
		//
	}
	if !ok {
		//
	}

}

// Cursor returns the current global position of the subscription.
// This cursor can be used from any node to resume event processing
// from the current position of this subscription.
func (s *Subscription) Cursor() EventCursor {
	return slices.Clone(s.stored.cursor)
}

// Topic returns the subscription topic.
func (s *Subscription) Topic() string {
	return s.topic
}

// Close closes the subscription file.
func (s *Subscription) Close() error {
	return s.stored.close()
}

/**/

// Watcher listens for emitted events and passes them to handler.
// It does not persist any state and receive only newly created events.
// For published events, use Subscription.
type Watcher struct {
	topic   string
	recv    chan *Event
	running atomic.Bool
}

// Receive dispatches new events to the provided fn. If fn returns false, processing stops.
// Receive does not block. Calling Receive multiple times on the same subscription results in error.
func (r *Watcher) Receive(fn func(*Event) bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
		}
		r.running.Store(false)
	}()

}

func (r *Watcher) Close() {
	//
}

/**/

type boltcursor struct {
	cursor EventCursor
	bolt   *bbolt.DB
	buck   []byte
	key    []byte
}

func newBoltCursor(topic string, start EventCursor, filename string) (*boltcursor, error) {
	bolt, err := bbolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("initialization error: %w", err)
	}

	buck := []byte("mono")
	key := []byte("cur")

	var cur EventCursor

	err = bolt.View(func(tx *bbolt.Tx) error {

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
			if err := tx.Bucket(buck).Put(key, v); err != nil {
				return err
			}
			cur = start
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	m := &boltcursor{
		cursor: cur,
		bolt:   bolt,
		buck:   buck,
		key:    key,
	}
	return m, nil
}

func (c *boltcursor) close() error {
	return c.bolt.Close()
}

func (c *boltcursor) update(uuid monotime.UUID) error {
	if uuid.Valid() {
		return fmt.Errorf("UUID is not valid")
	}

	c.cursor = c.cursor.Update(uuid)

	tx, err := c.bolt.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	v := make([]byte, 0, len(c.cursor)*16)
	for _, id := range c.cursor {
		v = append(v, id[:]...)
	}

	if err = tx.Bucket(c.buck).Put(c.key, v); err != nil {
		return err
	}

	return tx.Commit()
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }
