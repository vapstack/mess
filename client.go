package mess

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

func NewClient() *http.Client {
	return &http.Client{
		Transport: NewTransport(),
	}
}

// NewCustomClient creates a client targeting specific realm, node and/or service.
// Empty realm is treated as the current realm.
// Zero node and empty service are ignored.
func NewCustomClient(realm string, node uint64, service string) *http.Client {
	return &http.Client{
		Transport: NewCustomTransport(realm, node, service),
	}
}

var DefaultClient = NewClient()

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
