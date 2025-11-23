package mess

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/vapstack/mess/internal"
	"golang.org/x/net/http2"
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
	http.RoundTripper
}

func NewTransport() RoundTripper {
	return NewCustomTransport("", 0, "")
}

// NewCustomTransport creates a custom transport that can target specific realm, node and services.
// Empty realm is treated as the current realm.
// Since mess proxy supports h2c, NewCustomTransport creates an http2.Transport.
func NewCustomTransport(realm string, node uint64, service string) RoundTripper {
	return RoundTripper{
		Realm:   realm,
		Node:    node,
		Service: service,
		RoundTripper: &http2.Transport{
			DialTLSContext:     DialH2CContext,
			DisableCompression: true,
			AllowHTTP:          true,
		},
		// Transport: &http.Transport{
		// 	ForceAttemptHTTP2:      true,
		// 	DialContext:            DialContext,
		// 	DisableCompression:     true,
		// 	MaxIdleConns:           0,
		// 	MaxIdleConnsPerHost:    1024,
		// 	MaxConnsPerHost:        0,
		// 	IdleConnTimeout:        time.Minute,
		// 	MaxResponseHeaderBytes: 0,
		// },
	}
}

func (r RoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	// if env.Service != "" {
	// 	request.Header.Set(CallerServiceHeader, env.Service)
	// }

	request.Header.Set(CallerHeader, internal.ConstructCaller(env.NodeID, env.Realm, env.Service))

	if r.Realm != "" {
		request.Header.Set(TargetRealmHeader, r.Realm)
	}
	if r.Node != 0 {
		request.Header.Set(TargetNodeHeader, strconv.FormatUint(r.Node, 10))
	}
	if r.Service != "" {
		request.Header.Set(TargetServiceHeader, r.Service)
	}
	return r.RoundTripper.RoundTrip(request)
}

/**/

func DialContext(ctx context.Context, _ string, _ string) (net.Conn, error) {
	return new(net.Dialer).DialContext(ctx, env.ProxyNetwork, env.ProxyAddr)
}

func DialH2CContext(ctx context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
	return new(net.Dialer).DialContext(ctx, env.ProxyNetwork, env.ProxyAddr)
}
