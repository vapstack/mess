package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Proxy struct {
	network  string
	addr     string
	listener net.Listener
	server   *http.Server
	reverse  *httputil.ReverseProxy
	closed   atomic.Bool
}

func (p *Proxy) Addr() string {
	return p.addr
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.reverse.ServeHTTP(w, r)
}

func (p *Proxy) Close() error {
	if p == nil {
		return nil
	}
	if p.closed.CompareAndSwap(false, true) {
		if p.network == "unix" {
			defer func(p *Proxy) {
				if e := os.Remove(p.addr); e != nil && !errors.Is(e, fs.ErrNotExist) {
					log.Println("error removing socket file:", e)
				}
			}(p)
		}
		if p.server != nil {
			go func(p *Proxy) { _ = p.server.Shutdown(context.Background()) }(p)
		}
		if p.listener != nil {
			return p.listener.Close()
		}
	}
	return nil
}

func ServiceToNode(svc *mess.Service, nodeID uint64, dir string, handler http.HandlerFunc) (*Proxy, error) {

	var network, addr, port string
	var err error

	switch p := strings.ToLower(svc.Proxy); p {

	case "", "tcp":
		network = "tcp"
		addr = "127.0.0.1:0"

	case "unix":
		network = p
		addr, err = filepath.Abs(filepath.Join(dir, "mess.proxy.sock"))
		if err != nil {
			return nil, fmt.Errorf("error getting absolute path for proxy unix-socket: %v", err)
		}

	default:
		network, addr, err = internal.ParseNetworkAddr(svc.Proxy)
		if err != nil {
			return nil, fmt.Errorf("error parsing proxy string %v: %w", svc.Proxy, err)
		}
		if network == "unix" {
			if !filepath.IsAbs(addr) {
				addr, err = filepath.Abs(filepath.Join(dir, addr))
				if err != nil {
					return nil, fmt.Errorf("error getting absolute path for proxy unix-socket (%v): %v", svc.Proxy, err)
				}
			}
		}
	}

	if network == "unix" {
		if err = os.Remove(addr); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Println("error removing socket file:", err)
		}
	}

	l, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		network:  network,
		listener: l,
	}

	if network == "unix" {
		p.addr = addr

	} else {
		_, port, err = net.SplitHostPort(l.Addr().String())
		if err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("error getting listener port: %w", err)
		}
		p.addr = "127.0.0.1:" + port
	}

	p.server = &http.Server{
		Handler: h2c.NewHandler(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Header.Set(mess.CallerHeader, internal.ConstructCaller(nodeID, svc.Realm, svc.Name))
				handler(w, r)
			}),
			&http2.Server{
				MaxConcurrentStreams: 16,
			},
		),
		ErrorLog: log.New(io.Discard, "", 0),
	}

	if err = http2.ConfigureServer(p.server, &http2.Server{
		MaxConcurrentStreams: 32,
	}); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("http/2 server configuration error: %w", err)
	}

	go func() {
		if e := p.server.Serve(p.listener); e != nil && !errors.Is(e, http.ErrServerClosed) {
			log.Println("http server error:", e)
		}
	}()

	return p, nil
}

func NodeToService(svc *mess.Service, dir string) (*Proxy, error) {

	network, addr, err := internal.ParseNetworkAddr(svc.Listen)
	if err != nil {
		return nil, fmt.Errorf("invalid Listen: %w", err)
	}

	transport := &http.Transport{
		ForceAttemptHTTP2:   true,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     2 * time.Minute,
	}

	switch network {
	case "unix":
		if !filepath.IsAbs(addr) {
			addr, err = filepath.Abs(filepath.Join(dir, addr))
			if err != nil {
				return nil, fmt.Errorf("error getting absolute path for proxy unix-socket (%v): %v", svc.Listen, err)
			}
		}
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, "unix", addr)
		}

	case "tcp":
		host, port, e := net.SplitHostPort(addr)
		if e != nil {
			return nil, fmt.Errorf("invalid Listen: %w", e)
		}
		if host == "" || host == "localhost" {
			host = "127.0.0.1"
		}

		dst := net.JoinHostPort(host, port)

		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, "tcp", dst)
		}
	}

	r := &httputil.ReverseProxy{
		Rewrite:       Rewrite,
		Transport:     transport,
		FlushInterval: -1,
		BufferPool:    BufferPool,
	}

	p := &Proxy{
		reverse: r,
	}
	return p, nil
}
