package mess

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/vapstack/monotime"
	"go.etcd.io/bbolt"
)

type (
	Event struct {

		// ID is the unique event identifier in UUID v7 format. Set by the node.
		// It contains the event time and the ID of the node that published the event.
		// This ID is guaranteed to be unique within the cluster.
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

const MaxEventSize = 16 << 20

// EventCursor is a vector clock that represents a per-node event position vector
// based on monotime UUIDs.
//
// Each entry in the cursor stores the last observed event UUID for a specific node.
// The node identity is encoded inside monotime.UUID.
//
// At most one UUID is stored per node. For each node, the UUID represents
// the latest known event for that node.
type EventCursor []monotime.UUID

// Update updates the cursor entry for the node encoded in the provided uuid
// if it is newer than the existing value associated with that node.
// If the node is not yet present in the cursor, uuid is appended.
//
// Update updates the cursor in-place.
// The returned cursor shares the same underlying array.
func (ec EventCursor) Update(uuid monotime.UUID) EventCursor {
	nodeID := uuid.NodeID()
	for i, u := range ec {
		if u.NodeID() == nodeID {
			if bytes.Compare(ec[i][:], uuid[:]) < 0 {
				ec[i] = uuid
			}
			return ec
		}
	}
	return append(ec, uuid)
}

// Clone returns a copy of the cursor.
func (ec EventCursor) Clone() EventCursor {
	return slices.Clone(ec)
}

// Set overwrites the cursor entry for the node encoded in the uuid.
// If the node is not present, the provided uuid is appended.
//
// Set updates the cursor in-place.
// The returned cursor shares the same underlying array.
func (ec EventCursor) Set(uuid monotime.UUID) EventCursor {
	nodeID := uuid.NodeID()
	for i, v := range ec {
		if v.NodeID() == nodeID {
			ec[i] = uuid
			return ec
		}
	}
	return append(ec, uuid)
}

// ExtractNode returns the event ID associated with the given node ID.
// If the node is not present in the cursor, ExtractNode returns monotime.ZeroUUID.
func (ec EventCursor) ExtractNode(id uint64) monotime.UUID {
	for _, v := range ec {
		if uint64(v.NodeID()) == id {
			return v
		}
	}
	return monotime.ZeroUUID
}

// Merge merges another cursor into the current.
//
// For each node, the resulting cursor contains the newest event ID.
// If a node exists in both cursors, Update semantics are applied.
// Nodes present only in the provided cursor are appended.
//
// Merge updates the cursor in-place.
// The returned cursor shares the same underlying array.
func (ec EventCursor) Merge(cursor EventCursor) EventCursor {
	result := ec
	for _, v := range cursor {
		result = result.Update(v)
	}
	return result
}

// Nodes returns the list of node IDs present in the cursor.
func (ec EventCursor) Nodes() []uint64 {
	result := make([]uint64, len(ec))
	for i, v := range ec {
		result[i] = uint64(v.NodeID())
	}
	return result
}

/**/

// Subscription represents a persistent event subscription on a single topic.
type Subscription struct {
	api    API
	topic  string
	cursor *boltCursor
	mu     sync.Mutex
	closed bool

	cancel   func()
	cancelMu sync.Mutex
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

	subCtx, cancel := context.WithCancel(ctx)

	s.cancelMu.Lock()
	s.cancel = cancel
	s.cancelMu.Unlock()

	defer cancel()

	req := SubscribeRequest{
		Topic:  s.topic,
		Cursor: s.cursor.get(),
		Filter: filter,
	}

	return s.api.Subscribe(subCtx, req, func(event *Event) (bool, error) {
		ok, herr := handler(event)
		if ok && herr == nil {
			if serr := s.cursor.update(event.ID); serr != nil {
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
	return s.cursor.get()
}

// Update updates the cursor entry for the node encoded in the provided
// uuid if it is newer than the existing value associated with that node.
// If the node is not yet present in the cursor, uuid is appended.
//
// This operation cannot be rolled back and must be used with caution.
//
// It does not interrupt any active handlers and does not shift their positions.
func (s *Subscription) Update(uuid monotime.UUID) error {
	return s.cursor.update(uuid)
}

// Topic returns the subscription topic.
func (s *Subscription) Topic() string {
	return s.topic
}

// Close closes the underlying cursor store.
// Close blocks until any active Receive calls return.
// Close must not be called inside a handler provided to Receive.
func (s *Subscription) Close() error {

	s.cancelMu.Lock()
	cancel := s.cancel
	s.cancelMu.Unlock()

	if cancel != nil {
		cancel()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.cancelMu.Lock()
	s.cancel = nil
	s.cancelMu.Unlock()

	s.closed = true

	return s.cursor.close()
}

/**/

type boltCursor struct {
	cursor EventCursor
	bolt   *bbolt.DB
	buck   []byte
	key    []byte
	mu     sync.RWMutex
}

func newBoltCursor(topic string, start EventCursor, filename string) (*boltCursor, error) {
	bolt, dbErr := bbolt.Open(filename, 0o600, nil)
	if dbErr != nil {
		return nil, fmt.Errorf("initialization error: %w", dbErr)
	}

	buck := []byte("mono")
	key := []byte("cur")

	var cur EventCursor

	err := bolt.Update(func(tx *bbolt.Tx) error {

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
			if err = b.Put(tkey, []byte(topic)); err != nil {
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
			if err = b.Put(key, v); err != nil {
				return err
			}
			cur = start.Clone()
		}

		return nil
	})
	if err != nil {
		_ = bolt.Close()
		return nil, err
	}

	m := &boltCursor{
		cursor: cur,
		bolt:   bolt,
		buck:   buck,
		key:    key,
	}

	return m, nil
}

func (c *boltCursor) get() EventCursor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Clone(c.cursor)
}

func (c *boltCursor) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bolt.Close()
}

func (c *boltCursor) update(uuid monotime.UUID) error {
	if !uuid.Valid() {
		return fmt.Errorf("UUID is not valid")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cur := c.cursor.Clone()
	cur = cur.Update(uuid)

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

	if err = tx.Commit(); err != nil {
		return err
	}
	c.cursor = cur
	return nil
}

func rollback(tx *bbolt.Tx) { _ = tx.Rollback() }
