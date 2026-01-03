package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/gzhttp"
	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal/manager"
	"github.com/vapstack/mess/internal/proxy"
	"github.com/vapstack/mess/internal/tlsutil"
	"golang.org/x/net/http2"
)

func (n *node) serviceHandler(hw http.ResponseWriter, hr *http.Request) {

	w, r := proxy.Wrap(hw, hr)
	defer w.Release()
	defer n.collectProxyMetrics(w)

	if err := w.FromLocal(hr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var (
		tn *mess.Node
		ok bool
	)

	d := n.state.Load()

	if w.Target.Service == mess.ServiceName {

		if w.Target.NodeID == 0 || w.Target.NodeID == n.id {
			w.ToLocal()
			n.nodeHandler(w, r, strings.Split(strings.Trim(r.URL.Path, "/"), "/"), true)
			return
		}

		if tn, ok = d.Map[w.Target.NodeID]; !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

	} else if w.Target.NodeID == 0 || w.Target.NodeID == n.id {

		if n.tryRouteToLocal(w, r) {
			return
		}

		if w.Target.NodeID == n.id {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		nodes := (*n.remoteServices.Load()).getByRealmAndName(w.Target.Realm, w.Target.Service)
		if len(nodes) == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		tn = nodes[0]

	} else if tn, ok = d.Map[w.Target.NodeID]; !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if n.dev {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if err := w.ToRemote(tn); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	n.proxy.ServeHTTP(w, r)
}

func (n *node) tryRouteToLocal(w *proxy.Wrapper, r *http.Request) bool {

	if rr := (*n.localAliases.Load()).getByRealmAndName(w.Target.Realm, w.Target.Service); rr != nil {
		if len(rr.pms) == 1 {
			if n.tryRouteToProcess(rr.pms[0], w, r) {
				return true
			}
		} else if len(rr.pms) > 1 {
			for range rr.pms {
				if pm := rr.pms[int(rr.next.Add(1)%uint64(len(rr.pms)))]; pm != nil {
					if n.tryRouteToProcess(pm, w, r) {
						return true
					}
				}
			}
		}
	}
	return false
}

func (n *node) tryRouteToProcess(pm *manager.Manager, w *proxy.Wrapper, r *http.Request) bool {
	s := pm.Service()
	if n.id == w.Caller.NodeID {
		if w.Caller.Service == s.Name {
			if w.Caller.Realm == s.Realm {
				return false
			}
		}
	}
	if !s.Private {
		if p := pm.Proxy(); p != nil {
			w.ToLocal()
			p.ServeHTTP(w, r)
			return true
		}
	}
	return false
}

func (n *node) publicHandler(hw http.ResponseWriter, hr *http.Request) {

	w, r := proxy.Wrap(hw, hr)
	defer w.Release()
	defer n.collectProxyMetrics(w)

	if err := w.FromRemote(r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if w.Target.Service == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if w.Target.NodeID != 0 && w.Target.NodeID != n.id {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if w.Target.Service == mess.ServiceName {
		w.ToLocal()
		n.nodeHandler(w, r, strings.Split(strings.Trim(r.URL.Path, "/"), "/"), false)
		return
	}

	if n.tryRouteToLocal(w, r) {
		return
	}

	w.WriteHeader(http.StatusServiceUnavailable)
}

func (n *node) nodeHandler(w *proxy.Wrapper, r *http.Request, path []string, fromLocal bool) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if len(path) != 1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	h, ok := nodeHandlers[path[0]]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	h.call(n, w, r, fromLocal)
}

func (n *node) setupProxyClient() {

	/*
		transport := gzhttp.Transport(&http.Transport{

			ForceAttemptHTTP2:     true,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 2 * time.Second,
			MaxIdleConnsPerHost:   8,

			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
					return n.cert.Load(), nil
				},
				VerifyPeerCertificate: tlsutil.PeerCertVerifier(n.id, n.pool),
				RootCAs:               n.pool,
				MinVersion:            tls.VersionTLS13,
			},
		})
	*/
	//
	// experiment
	//
	transport := gzhttp.Transport(&http2.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return n.cert.Load(), nil
			},
			VerifyPeerCertificate: tlsutil.PeerCertVerifier(n.id, n.pool),
			RootCAs:               n.pool,
			MinVersion:            tls.VersionTLS13,
		},
		ReadIdleTimeout: 3 * time.Second,
		// DialTLSContext:             nil,
		// ConnPool:                   nil,
		// DisableCompression:         false,
		// AllowHTTP:                  false,
		// MaxHeaderListSize:          0,
		// MaxReadFrameSize:           0,
		// MaxDecoderHeaderTableSize:  0,
		// MaxEncoderHeaderTableSize:  0,
		// StrictMaxConcurrentStreams: false,
		// IdleConnTimeout:            0,
		// PingTimeout:                0,
		// WriteByteTimeout:           0,
		// CountError:                 nil,
	})

	n.proxy = &httputil.ReverseProxy{
		Rewrite:       proxy.Rewrite,
		Transport:     transport,
		BufferPool:    proxy.BufferPool,
		FlushInterval: -1,
	}

	n.client = &http.Client{
		Transport: &roundTripper{
			node:   n,
			base:   transport,
			caller: fmt.Sprintf("%v;;%v", n.id, mess.ServiceName),
		},
	}
}

type roundTripper struct {
	caller string
	base   http.RoundTripper
	node   *node
}

type clientMeter struct {
	Start  time.Time
	Target string
	Status int
	Bytes  uint64
	Body   io.ReadCloser
	done   atomic.Bool // todo: reset when pool is implemented
	node   *node
}

func (rt *roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	// todo: get from pool
	m := &clientMeter{
		Start:  time.Now(),
		Target: r.Header.Get(mess.TargetNodeHeader),
		Status: http.StatusOK,
		node:   rt.node,
	}
	r.Header.Set(mess.CallerHeader, rt.caller)
	r.Header.Set(mess.TargetServiceHeader, mess.ServiceName)
	r.Header.Set("Content-Type", "application/gob") // ~?

	response, err := rt.base.RoundTrip(r)
	if err != nil {
		return response, err
	}
	m.Status = response.StatusCode
	m.Body = response.Body
	response.Body = m

	return response, nil
}

func (m *clientMeter) Read(p []byte) (int, error) {
	n, err := m.Body.Read(p)
	m.Bytes += uint64(n)
	if err != nil {
		m.finish()
	}
	return n, err
}

func (m *clientMeter) finish() {
	if m.done.CompareAndSwap(false, true) {
		m.node.collectClientMetrics(m)
		m.release()
	}
}

func (m *clientMeter) Close() error {
	err := m.Body.Close()
	m.finish()
	return err
}

func (m *clientMeter) release() {
	// todo: return to pool
}

func (n *node) setupPublicServer() (stopfn func(), errch chan error, err error) {

	addr := fmt.Sprintf("%v:%v", n.state.Load().Node.Bind, mess.PublicPort)

	publicListener, lerr := net.Listen("tcp", addr)
	if lerr != nil {
		return nil, nil, fmt.Errorf("listen tcp %v: %w", addr, lerr)
	}

	var tlsConfig *tls.Config

	if !n.dev {
		tlsConfig = &tls.Config{
			ClientCAs: n.pool,
			GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return n.cert.Load(), nil
			},
			VerifyPeerCertificate: tlsutil.PeerCertVerifier(n.id, n.pool),
			ClientAuth:            tls.RequireAndVerifyClientCert,
			MinVersion:            tls.VersionTLS13,
		}
	}

	publicServer := &http.Server{
		Handler:           gzhttp.GzipHandler(http.HandlerFunc(n.publicHandler)),
		TLSConfig:         tlsConfig,
		ErrorLog:          log.New(io.Discard, "", 0),
		ReadHeaderTimeout: 4 * time.Second,
	}
	if err = http2.ConfigureServer(publicServer, &http2.Server{
		MaxConcurrentStreams: 32,
	}); err != nil {
		return nil, nil, fmt.Errorf("http/2 server configuration error: %w", err)
	}

	stopfn = func() {
		log.Println("stopping public server...")
		if e := publicServer.Shutdown(context.Background()); e != nil {
			n.logf("public server shutdown error: %v", e)
		}
	}

	errch = make(chan error, 1)
	go func(dev bool) {
		defer close(errch)
		if dev {
			if e := publicServer.Serve(publicListener); e != nil && !errors.Is(e, http.ErrServerClosed) {
				errch <- e
			}
		} else {
			if e := publicServer.ServeTLS(publicListener, "", ""); e != nil && !errors.Is(e, http.ErrServerClosed) {
				errch <- e
			}
		}
	}(n.dev)
	return
}
