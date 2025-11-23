package mess

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/vapstack/mess/internal"
)

func State(ctx context.Context) (*NodeState, error) {
	return api.State(ctx)
}
func Publish(ctx context.Context, req PublishRequest) error {
	return api.Publish(ctx, req)
}
func Emit(ctx context.Context, req EmitRequest) error {
	return api.Emit(ctx, req)
}

type API struct {
	Client     *http.Client
	BatchDelay int
}

var api = &API{
	Client: NewClient(),
}

func (a API) State(ctx context.Context) (*NodeState, error) {
	nd := new(NodeState)
	return nd, a.send(ctx, "/state", nil, nd)
}

// func (a API) MustPublish(topic string, payload []byte) {
// 	err := a.Publish(context.Background(), topic, payload)
// 	if err != nil {
// 		panic(err)
// 	}
// }

func (a API) Publish(ctx context.Context, req PublishRequest) error {
	if req.Topic == "" {
		return fmt.Errorf("topic is empty")
	}
	if len(req.Events) == 0 {
		return nil
	}
	for i, event := range req.Events {
		if len(event) > MaxEventSize {
			return fmt.Errorf("max event size is %v, got %v (index: %v)", MaxEventSize, len(event), i)
		}
	}
	return a.send(ctx, "/publish", req, nil)
}

func (a API) Emit(ctx context.Context, req EmitRequest) error {
	return fmt.Errorf("not implemented")
}

func (a API) Subscribe(ctx context.Context, topic string, cursor EventCursor, fn func(*Event) (bool, error)) error {

	return nil
}

type SubscriptionOptions struct {
	Topic    string
	Cursor   EventCursor
	FileName string
	Handler  func(*Event) bool
	Filter   []MetaFilter
}

// OpenSubscription opens or creates a persistent Subscription to the provided topic.
//
// Subscription automatically manages cursor persistence and resumes processing from where it left off.
//
// If the file does not exist, the provided cursor is used as a starting position,
// if cursor is nil, subscription starts from the current position (most recent events).
// If the file exists, the cursor is not used.
// If the topic stored in the file does not match the provided topic, an error is returned.
//
// If a nil error is returned, Subscription immediately starts processing events
// dispatching them to the provided handler. Processing is strictly sequential.
// If the handler returns false, processing stops.
// Otherwise, the event is considered handled and the subscription cursor is updated.
func (a API) OpenSubscription(topic string, cursor EventCursor, filename string, handler func(*Event) bool) (*Subscription, error) {
	c, err := newBoltCursor(topic, cursor, filename)
	if err != nil {
		return nil, err
	}
	s := &Subscription{
		topic:  topic,
		stored: c,
	}
	return s, nil
}

/*func (a API) publish(ctx context.Context, topic string, data []json.RawMessage) error {
	req := defaultRequest.WithContext(ctx)

	req.URL = &url.URL{
		Scheme: "http",
		Host:   "mess",
		Path:   "/publish/" + topic,
	}
	req.Host = "mess"
	req.Header = make(http.Header, 4)

	buf := getbuf()
	defer putbuf(buf)

	if err := gob.NewEncoder(buf).Encode(data); err != nil {
		return fmt.Errorf("encode error: %w", err)
	}
	a.send()

	req.Body = io.NopCloser(bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	req.GetBody = func() (io.ReadCloser, error) {
		r := bytes.NewReader(data)
		return io.NopCloser(r), nil
	}

	res, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer func(rsp *http.Response) {
		_, _ = io.Copy(io.Discard, rsp.Body)
		_ = rsp.Body.Close()
	}(res)

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status error: %v, cannot read body: %w", res.StatusCode, e)
		}
		return fmt.Errorf("status error: %v, body: %v", res.StatusCode, string(b))
	}
	return nil
}*/

func (a API) send(ctx context.Context, endpoint string, request, response any) error {

	buf := getbuf()
	defer putbuf(buf)

	if request != nil {
		if err := gob.NewEncoder(buf).Encode(request); err != nil {
			return fmt.Errorf("encode error: %w", err)
		}
	}

	req := newRequest(ctx, endpoint, buf)

	res, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer internal.DrainAndCloseBody(res)

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status error: %v, cannot read body: %w", res.StatusCode, e)
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

func newRequest(ctx context.Context, endpoint string, body *bytes.Buffer) *http.Request {
	req := defaultRequest.WithContext(ctx)

	req.URL = &url.URL{
		Scheme: "http",
		Host:   "mess",
		Path:   endpoint,
	}
	req.Host = "mess"

	req.Header = http.Header{
		"Content-Type": gobContentType,
	}

	if nodeID, ok := targetNodeFromContext(ctx); ok {
		req.Header.Set(TargetNodeHeader, strconv.FormatUint(nodeID, 10))
	}
	if realm, ok := targetRealmFromContext(ctx); ok {
		req.Header.Set(TargetRealmHeader, realm)
	}

	if body != nil {
		req.Body = io.NopCloser(body)
		req.ContentLength = int64(body.Len())
		buf := body.Bytes()
		req.GetBody = func() (io.ReadCloser, error) {
			r := bytes.NewReader(buf)
			return io.NopCloser(r), nil
		}
	}

	return req
}

var defaultRequest = &http.Request{
	Method: http.MethodPost,
}
