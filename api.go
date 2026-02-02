package mess

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/vapstack/mess/internal"
	"github.com/vapstack/monotime"
)

// State is a helper for API.State
// func State(ctx context.Context) (*NodeState, error) { return DefaultAPI.State(ctx) }
//
// NextSequence is a helper for API.NextSequence
// func NextSequence(ctx context.Context) (uint64, error) { return DefaultAPI.NextSequence(ctx) }
//
// NextNamedSequence is a helper for API.NextNamedSequence
// func NextNamedSequence(ctx context.Context, name string) (uint64, error) {
// 	return DefaultAPI.NextNamedSequence(ctx, name)
// }
//
// Publish is a helper for API.Publish
// func Publish(ctx context.Context, req PublishRequest) error { return DefaultAPI.Publish(ctx, req) }
//
// Emit is a helper for API.Emit
// func Emit(ctx context.Context, req EmitRequest) error { return DefaultAPI.Emit(ctx, req) }
//
// CreateSubscription is an alias for API.CreateSubscription
// func CreateSubscription(filename string, topic string, cursor EventCursor) (*Subscription, error) {
// 	return DefaultAPI.CreateSubscription(filename, topic, cursor)
// }

/**/

// API provides methods to interact with the mess.
type API struct {
	Client *http.Client
}

var DefaultAPI = API{Client: NewClient()}

// State returns a state of the current node along with a map of the cluster.
func (a API) State(ctx context.Context) (*NodeState, error) {
	nd := new(NodeState)
	return nd, a.send(ctx, "/state", nil, nd)
}

// Peers returns a list of nodes with services matching the specified service name.
// The current service (the one making the call) is excluded.
// Only services in the current realm are considered.
// Services are returned regardless of their current availability or runtime state.
func (a API) Peers(ctx context.Context, service string) (NodeList, error) {
	ns, err := a.State(ctx)
	if err != nil {
		return nil, err
	}
	return ns.Peers(service), nil
}

// Resolve returns a list of node with services matching the specified service name
// and available at the time of the call.
// Only services in the current realm are considered.
func (a API) Resolve(ctx context.Context, service string) (NodeList, error) {
	ns, err := a.State(ctx)
	if err != nil {
		return nil, err
	}
	return ns.Resolve(service), nil
}

// NextSequence returns the next sequential ID,
// which is globally unique across all nodes in the cluster.
// It consists of a 16-bit node ID and a 48-bit counter.
func (a API) NextSequence(ctx context.Context) (uint64, error) {
	var seq uint64
	return seq, a.send(ctx, "/seq", nil, &seq)
}

// NextNamedSequence returns the next sequential identifier associated with the specified name,
// which is globally unique across all cluster nodes.
// It consists of a 16-bit node ID and a 48-bit counter.
func (a API) NextNamedSequence(ctx context.Context, name string) (uint64, error) {
	var seq uint64
	return seq, a.send(ctx, "/seq?name="+name, nil, &seq)
}

// Publish publishes the provided events on the specified topic.
// Meta fields, if specified, will be stored with each event.
// Publish guarantees that either all the provided events are stored or none.
// If a nil error is returned, all events are successfully published and
// the returned slice contains the IDs of the stored events.
func (a API) Publish(ctx context.Context, req PublishRequest) ([]monotime.UUID, error) {
	if req.Topic == "" {
		return nil, errors.New("topic is empty")
	}
	if len(req.Events) == 0 {
		return nil, nil
	}
	for i, event := range req.Events {
		if len(event) > MaxEventSize {
			return nil, fmt.Errorf("max event size is %v, got %v (index: %v)", MaxEventSize, len(event), i)
		}
	}
	ids := make([]monotime.UUID, 0)
	err := a.send(ctx, "/publish", req, &ids)
	return ids, err
}

// Subscribe receives events from the specified topic using the provided cursor as an offset.
// If the cursor is nil or empty, the subscription starts from the most recent events.
// An optional filter is used for server-side filtering of incoming events.
//
// The handler is called for each event. If the handler returns false or a non-nil error,
// processing stops and the error is returned.
// The event is not considered handled and will be retried later.
//
// Subscribe transparently reconnects with exponential backoff if needed.
// Connection errors do not stop processing; they are logged using the standard log package.
//
// If the context is canceled or expires, Subscribe waits until the currently running
// handler returns, then stops processing and returns the context error.
//
// Subscribe blocks until the context is canceled or expires,
// or the handler returns false or a non-nil error.
//
// Subscribe does not provide cursor persistence; use CreateSubscription for that.
func (a API) Subscribe(ctx context.Context, req SubscribeRequest, handler func(*Event) (bool, error)) error {

	if req.Topic == "" {
		return errors.New("topic is empty")
	}

	buf := getbuf()
	defer putbuf(buf)

	const (
		minBackoff = time.Second
		maxBackoff = 30 * time.Second
	)

	backoff := minBackoff

	dst := "http://mess/subscribe"

	done := ctx.Done()

	for {

		buf.Reset()

		if err := gob.NewEncoder(buf).Encode(req); err != nil {
			return fmt.Errorf("request encode error: %w", err)
		}

		r, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, buf)
		if err != nil {
			return fmt.Errorf("error building request: %w", err)
		}
		r.Header["Content-Type"] = gobContentType

		res, err := a.Client.Do(r)
		if err != nil {

			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			log.Printf("mess: Subscribe: request error: %v\n", err)

			select {
			case <-done:
				return ctx.Err()
			case <-time.After(backoff):
				if backoff *= 2; backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		if res.StatusCode >= 300 {
			b, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()

			if len(b) == 0 {
				log.Printf("mess: Subscribe: status error: %v - %v\n", res.StatusCode, http.StatusText(res.StatusCode))
			} else {
				log.Printf("mess: Subscribe: status error: %v, body: %v\n", res.StatusCode, string(b))
			}

			select {
			case <-done:
				return ctx.Err()
			case <-time.After(backoff):
				if backoff *= 2; backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		backoff = minBackoff

		next, err := func() (bool, error) {
			defer internal.DrainAndCloseBody(res)

			dec := gob.NewDecoder(res.Body)

			for {
				var ev Event
				if err := dec.Decode(&ev); err != nil {
					if !errors.Is(err, io.EOF) {
						log.Printf("mess: Subscribe: decode error: %v", err)
					}
					return true, nil
				}

				next, err := handler(&ev)
				if err != nil {
					return next, err
				}
				if !next {
					return next, nil
				}

				req.Cursor = req.Cursor.Update(ev.ID)
			}
		}()
		if err != nil {
			return err
		}
		if !next {
			return nil
		}
		if err = ctx.Err(); err != nil {
			return err
		}

		select {
		case <-done:
			return ctx.Err()
		case <-time.After(backoff):
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// OpenSubscription opens a previously created persistent Subscription.
// Subscription automatically manages cursor persistence and resumes processing from where it left off.
//
// If the file does not exist, an error is returned; CreateSubscription should be used to create a new one.
func (a API) OpenSubscription(topic string, filename string) (*Subscription, error) {

	if _, err := os.Stat(filename); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrSubscriptionNotExist
		}
		return nil, fmt.Errorf("file stat error: %w", err)
	}

	c, err := newBoltCursor(topic, nil, filename)
	if err != nil {
		return nil, err
	}

	s := &Subscription{
		api:    a,
		topic:  topic,
		cursor: c,
	}
	return s, nil
}

var (
	ErrSubscriptionExists   = errors.New("subscription already exists")
	ErrSubscriptionNotExist = errors.New("subscription does not exist")
)

// CreateSubscription creates a persistent Subscription to the provided topic.
// Subscription automatically manages cursor persistence and resumes processing from where it left off.
//
// The provided cursor is used as a starting position (offset).
// If cursor is nil or empty, the subscription starts from the most recent events.
//
// If the file exists, an error is returned; OpenSubscription should be used to open an existing one.
func (a API) CreateSubscription(filename string, topic string, cursor EventCursor) (*Subscription, error) {

	if _, err := os.Stat(filename); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("file stat error: %w", err)
		}

	} else {
		return nil, ErrSubscriptionExists
	}

	c, err := newBoltCursor(topic, cursor, filename)
	if err != nil {
		return nil, err
	}

	s := &Subscription{
		api:    a,
		topic:  topic,
		cursor: c,
	}

	return s, nil
}

func (a API) Emit(ctx context.Context, req EmitRequest) error {
	return fmt.Errorf("not implemented")
}

func (a API) Listen(ctx context.Context, topic string) (<-chan *Event, error) {
	return nil, fmt.Errorf("not implemented")
}

func (a API) send(ctx context.Context, endpoint string, request, response any) error {

	buf := getbuf()
	defer putbuf(buf)

	if request != nil {
		if err := gob.NewEncoder(buf).Encode(request); err != nil {
			return fmt.Errorf("encode error: %w", err)
		}
	}

	dst := "http://mess" + endpoint

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, buf)
	if err != nil {
		return fmt.Errorf("error building request: %w", err)
	}

	req.Header["Content-Type"] = gobContentType

	res, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer internal.DrainAndCloseBody(res)

	if res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 1<<10))
		if len(b) == 0 {
			return fmt.Errorf("status error: %v - %v", res.StatusCode, http.StatusText(res.StatusCode))
		}
		return fmt.Errorf("status error: %v, body: %v", res.StatusCode, string(b))
	}

	if response != nil {
		if err = gob.NewDecoder(res.Body).Decode(response); err != nil {
			return fmt.Errorf("decode error: %w", err)
		}
	}
	return nil
}

var gobContentType = []string{"application/gob"}
