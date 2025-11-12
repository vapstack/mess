package mess

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"
)

type API struct{ *http.Client }

func NewClient() *http.Client {
	return &http.Client{
		Transport: NewTransport(),
	}
}

// NewCustomClient creates a client targeting specific realm, node and service.
// Empty realm is treated as the current realm.
// Zero node and empty service are ignored.
func NewCustomClient(realm string, node uint64, service string) *http.Client {
	return &http.Client{
		Transport: NewCustomTransport(realm, node, service),
	}
}

var DefaultClient = NewClient()

var api = API{NewClient()}

func Info(ctx context.Context) (*NodeData, error) {
	return api.Info(ctx)
}

func (a API) Info(ctx context.Context) (*NodeData, error) {
	nd := new(NodeData)
	return nd, a.send(ctx, "info", nil, nd)
}

func Find(ctx context.Context, search string) ([]*Node, error) {
	return api.Find(ctx, search)
}

func FindIn(ctx context.Context, search, realm string) ([]*Node, error) {
	return api.FindIn(ctx, search, realm)
}

func (a API) Find(ctx context.Context, search string) ([]*Node, error) {
	return a.FindIn(ctx, search, env.Realm)
}

func (a API) FindIn(ctx context.Context, search, realm string) ([]*Node, error) {
	var nodes []*Node
	nd := new(NodeData)
	if err := a.send(ctx, "info", nil, nd); err != nil {
		return nil, err
	}
	if nd.Node.Services.HasIn(search, realm) {
		nodes = append(nodes, nd.Node)
	}
	for _, n := range nd.Map {
		if n.Services.HasIn(search, realm) {
			nodes = append(nodes, n)
		}
	}
	slices.SortFunc(nodes, nd.Node.ProximitySort)
	return nodes, nil
}

func Closest(ctx context.Context, service string) (*Service, error) {
	return api.Closest(ctx, service)
}

func (a API) Closest(ctx context.Context, service string) (*Service, error) {
	nd := new(NodeData)
	if err := a.send(ctx, "info", nil, nd); err != nil {
		return nil, err
	}
	if svc := nd.Node.Services.Get(service); svc != nil {
		return svc, nil
	}
	nodes := make([]*Node, 0)
	for _, n := range nd.Map {
		if n.Has(service) {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	slices.SortFunc(nodes, nd.Node.ProximitySort)
	return nodes[0].Get(service), nil
}

// func (c *Client) WriteStream(ctx context.Context, streamName string, messageData []byte) error {
// 	panic("not implemented")
// }
//
// func (c *Client) ReadStream(ctx context.Context, streamName string, offset, limit uint64) ([]StreamRecord, error) {
// 	panic("not implemented")
// }
//
// func (c *Client) TruncateStream(ctx context.Context, streamName string, offset, limit uint64) ([]StreamRecord, error) {
// 	panic("not implemented")
// }
//
// func (c *Client) ExtractStream(ctx context.Context, streamName string, offset, limit uint64) ([]StreamRecord, error) {
// 	panic("not implemented")
// }

func (a API) send(ctx context.Context, endpoint string, request, response any) error {

	dst := "http://mess/" + endpoint

	buf := getbuf()
	defer putbuf(buf)

	if request != nil {
		if err := gob.NewEncoder(buf).Encode(request); err != nil {
			return fmt.Errorf("encode error: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, buf)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/gob")

	addContextHeaders(ctx, req.Header)

	res, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	defer func() { _, _ = io.Copy(io.Discard, res.Body) }()

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status error: %v, cannot read body: %w", res.StatusCode, e)
		}
		return fmt.Errorf("status error: %v, body: %v", res.StatusCode, string(b))
	}

	if response != nil {
		if err = gob.NewDecoder(res.Body).Decode(response); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
	}
	return nil
}

/**/

var bufpool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func getbuf() *bytes.Buffer { return bufpool.Get().(*bytes.Buffer) }
func putbuf(b *bytes.Buffer) {
	b.Reset()
	bufpool.Put(b)
}

/**/

type RoundTripper struct {
	Realm   string // specific realm
	Node    uint64 // single target node
	Service string // single target service
	*http.Transport
}

func NewTransport() RoundTripper {
	return NewCustomTransport("", 0, "")
}

// NewCustomTransport creates a custom transport that can target specific realm, node and services.
// Empty realm is treated as the current realm.
func NewCustomTransport(realm string, node uint64, service string) RoundTripper {
	return RoundTripper{
		Realm:   realm,
		Node:    node,
		Service: service,
		Transport: &http.Transport{
			ForceAttemptHTTP2:      true,
			DialContext:            DialContext,
			DisableCompression:     true,
			MaxIdleConns:           0,
			MaxIdleConnsPerHost:    1024,
			MaxConnsPerHost:        0,
			IdleConnTimeout:        time.Minute,
			MaxResponseHeaderBytes: 0,
		},
	}
}

func (r RoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if env.Service != "" {
		request.Header.Set(CallerServiceHeader, env.Service)
	}
	if r.Realm != "" {
		request.Header.Set(TargetRealmHeader, r.Realm)
	}
	if r.Node != 0 {
		request.Header.Set(TargetNodeHeader, strconv.FormatUint(r.Node, 10))
	}
	if r.Service != "" {
		request.Header.Set(TargetServiceHeader, r.Service)
	}
	return r.Transport.RoundTrip(request)
}

/**/

func DialContext(ctx context.Context, _ string, _ string) (net.Conn, error) {
	return new(net.Dialer).DialContext(ctx, env.ProxyNetwork, env.ProxyAddr)
}
