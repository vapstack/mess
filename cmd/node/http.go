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
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal/proc"
	"github.com/vapstack/mess/internal/proxy"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func (n *node) localHandler(hw http.ResponseWriter, hr *http.Request) {

	w, r := proxy.Wrap(hw, hr) // n.getProxyBase(hw)
	defer w.Release()
	defer n.collectProxyMetrics(w)

	if err := w.FromLocal(hr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// r := hr.WithContext(proxy.NewContext(hr.Context(), b)) // hr.WithContext(context.WithValue(hr.Context(), ctxProxyBaseKey, b))

	/*if n.dev {

		if b.target.service == mess.ServiceName {
			if r.Method == http.MethodPost {
				if strings.Trim(r.URL.Path, "/") == "info" {
					b.toLocal()
					n.devInfoHandler(w, r)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if p, ok := n.devmap[b.target.realm+"."+b.target.service]; ok {
			b.toLocal()
			p.ServeHTTP(w, r)
			return
		}

		w.WriteHeader(http.StatusNotFound)
		return
	}*/

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
	return
}

func (n *node) tryRouteToLocal(w *proxy.Wrapper, r *http.Request) bool {

	// if pm := (*n.localServices.Load()).getByRealmAndName(w.Target.Realm, w.Target.Service); pm != nil {
	// 	if tryLocal(pm, w, r) {
	// 		return true
	// 	}
	// }

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

func (n *node) tryRouteToProcess(pm *proc.Manager, w *proxy.Wrapper, r *http.Request) bool {
	if n.id == w.Caller.NodeID {
		svc := pm.Service()
		if w.Caller.Realm == svc.Realm && w.Caller.Service == svc.Name {
			return false
		}
	}
	if !pm.Passive() {
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

	w.WriteHeader(http.StatusNotFound)
}

func (n *node) nodeHandler(w *proxy.Wrapper, r *http.Request, path []string, fromLocal bool) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if len(path) != 1 {
		// if path[0] == "publish" {
		// 	r.Pattern = path[1]
		// } else {
		// 	w.WriteHeader(http.StatusNotFound)
		// 	return
		// }
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

	transport := &http.Transport{

		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,

		TLSClientConfig: &tls.Config{
			InsecureSkipVerify:    true,
			GetCertificate:        n.getCert,
			VerifyPeerCertificate: n.verifyPeerCert,
			RootCAs:               n.pool,
			MinVersion:            tls.VersionTLS13,
		},
	}

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
	base   *http.Transport
	node   *node
}

type clientMeter struct {
	Start  time.Time
	Target string
	Status int
	Bytes  uint64
	Body   io.ReadCloser

	node *node
}

func (rt *roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	// todo: get from pool?
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
		return response, err // no request was made
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
		m.node.collectClientMetrics(m)
		m.release()
	}

	return n, err
}

func (m *clientMeter) Close() error {
	err := m.Body.Close()
	m.node.collectClientMetrics(m)
	m.release()
	return err
}

func (m *clientMeter) release() {
	// todo: return to pool?
}

/**/
/*
func (n *node) setupLocalServer() (stopfn func(), errch chan error, err error) {

	var localListener net.Listener

	if n.dev || runtime.GOOS == "windows" {
		addr := fmt.Sprintf("127.0.0.1:%v", mess.DevPort)
		localListener, err = net.Listen("tcp", addr)
		if err != nil {
			return nil, nil, fmt.Errorf("listen tcp %v: %w", addr, err)
		}

		// } else {
		// 	localListener, err = net.Listen("unix", n.sock)
		// 	if err != nil {
		// 		return nil, nil, fmt.Errorf("listen unix %v: %w", n.sock, err)
		// 	}
	}

	localServer := &http.Server{
		Handler:  http.HandlerFunc(n.localHandler),
		ErrorLog: log.New(io.Discard, "", 0),
	}

	stopfn = func() {
		// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		// defer cancel()
		log.Println("stopping local server...")
		if e := localServer.Shutdown(context.Background()); e != nil {
			n.messlogf("local server shutdown error: %v\n", e)
			// log.Printf("local server shutdown error: %v\n", e)
		}
	}

	errch = make(chan error, 1)
	go func() {
		defer close(errch)
		if e := localServer.Serve(localListener); e != nil && !errors.Is(e, http.ErrServerClosed) {
			errch <- e
		}
	}()

	return
}
*/

func (n *node) setupPublicServer() (stopfn func(), errch chan error, err error) {

	addr := fmt.Sprintf("%v:%v", n.state.Load().Node.Bind, mess.PublicPort)

	publicListener, lerr := net.Listen("tcp", addr)
	if lerr != nil {
		return nil, nil, fmt.Errorf("listen tcp %v: %w", addr, err)
	}

	var tlsConfig *tls.Config

	if !n.dev {
		tlsConfig = &tls.Config{
			ClientCAs:             n.pool,
			GetCertificate:        n.getCert,
			VerifyPeerCertificate: n.verifyPeerCert,
			ClientAuth:            tls.RequireAndVerifyClientCert,
			MinVersion:            tls.VersionTLS13,
		}
	}

	publicServer := &http.Server{
		Handler:           h2c.NewHandler(http.HandlerFunc(n.publicHandler), nil),
		TLSConfig:         tlsConfig,
		ErrorLog:          log.New(io.Discard, "", 0),
		ReadHeaderTimeout: 4 * time.Second,
	}
	if err = http2.ConfigureServer(publicServer, nil); err != nil {
		return nil, nil, fmt.Errorf("http/2 server configuration error: %w", err)
	}

	stopfn = func() {
		// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		// defer cancel()
		log.Println("stopping public server...")
		if e := publicServer.Shutdown(context.Background()); e != nil {
			n.logf("public server shutdown error: %v", e)
			// log.Printf("public server shutdown error: %v\n", e)
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

//
// func (pm *procManager) createServiceOutgoingProxy(svc *mess.Service) (string, error) {
//
// 	var network, addr string
// 	var err error
//
// 	switch p := strings.ToLower(svc.Proxy); p {
//
// 	case "", "tcp":
// 		network = p
// 		addr = "127.0.0.1:0"
//
// 	case "unix":
// 		network = p
// 		addr, err = filepath.Abs(filepath.Join(pm.path, "proxy.sock"))
// 		if err != nil {
// 			return "", fmt.Errorf("error getting absolute path for socket: %v", err)
// 		}
//
// 	default:
// 		network, addr, err = mess.ParseNetworkAddr(svc.Proxy)
// 		if err != nil {
// 			return "", fmt.Errorf("error parsing proxy string %v: %w", svc.Proxy, err)
// 		}
// 	}
// 	if network == "unix" {
// 		if err = os.Remove(addr); err != nil {
// 			return "", fmt.Errorf("error deleting sock file %v: %w", addr, err)
// 		}
// 	}
//
// 	l, err := net.Listen(network, addr)
// 	if err != nil {
// 		return "", err
// 	}
// 	pm.listener = l
// 	if network == "unix" {
// 		pm.unixsock = addr
// 	}
//
// 	pm.server = &http.Server{
// 		Handler: h2c.NewHandler(
// 			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 				r.Header.Set(mess.CallerNodeHeader, strconv.FormatUint(pm.node.id, 10))
// 				r.Header.Set(mess.CallerRealmHeader, svc.Realm)
// 				r.Header.Set(mess.CallerServiceHeader, svc.Name)
// 				pm.node.localHandler(w, r)
// 			}),
// 			new(http2.Server),
// 		),
// 		ErrorLog: log.New(io.Discard, "", 0),
// 	}
//
// 	go func() {
// 		if e := pm.server.Serve(pm.listener); e != nil && !errors.Is(e, http.ErrServerClosed) {
// 			if svc.Realm != "" {
// 				pm.node.messlogf("%v@%v: http server error: %v\n", svc.Name, svc.Realm, e)
// 			} else {
// 				pm.node.messlogf("%v: http server error: %v\n", svc.Name, e)
// 			}
// 		}
// 	}()
//
// 	if network == "unix" {
// 		return addr, nil
// 	} else {
// 		return "127.0.0.1:" + strings.Split(addr, ":")[1], nil
// 	}
//
// 	/*
// 		if pm.node.dev {
// 			network, addr, err := mess.ParseNetworkAddr(svc.Proxy)
// 			if err != nil {
// 				return fmt.Errorf("failed to parse proxy addr: %w", err)
// 			}
// 			l, err := net.Listen(network, addr)
// 			if err != nil {
// 				return fmt.Errorf("listen %v on %v: %w", network, addr, err)
// 			}
// 			if network == "unix" {
// 				pm.unixListener = l
// 			} else {
// 				pm.tcpListener = l
// 			}
// 		}
//
// 		proxyUnix, err := filepath.Abs(filepath.Join(pm.path, "proxy.sock"))
// 		if err != nil {
// 			pm.node.messlogf("error getting absolute path for socket: %v", err)
// 		} else {
// 			_ = os.Remove(proxyUnix)
// 			unixListener, uerr := net.Listen("unix", proxyUnix)
// 			if uerr != nil {
// 				return fmt.Errorf("listen unix %v: %w", proxyUnix, uerr)
// 			}
// 			pm.unixListener = unixListener
// 			pm.proxyUnix = proxyUnix
// 		}
//
// 		tcpListener, terr := net.Listen("tcp", "127.0.0.1:0")
// 		if terr != nil {
// 			if pm.unixListener != nil {
// 				_ = pm.unixListener.Close()
// 				_ = os.Remove(proxyUnix)
// 			}
// 			return fmt.Errorf("listen tcp: %w", terr)
// 		}
//
// 		pm.tcpListener = tcpListener
// 		if addr, ok := tcpListener.Addr().(*net.TCPAddr); ok {
// 			// proxyPort = addr.Port
// 			pm.proxyPort = addr.Port // proxyPort
// 		}
//
// 		pm.server = &http.Server{
// 			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 				r.Header.Set(mess.CallerNodeHeader, strconv.FormatUint(pm.node.id, 10))
// 				r.Header.Set(mess.CallerRealmHeader, svc.Realm)
// 				r.Header.Set(mess.CallerServiceHeader, svc.Name)
// 				pm.node.localHandler(w, r)
// 			}),
// 			ErrorLog: log.New(io.Discard, "", 0),
// 		}
//
// 		if pm.unixListener != nil {
// 			go func() {
// 				if e := pm.server.Serve(pm.unixListener); e != nil && !errors.Is(e, http.ErrServerClosed) {
// 					if svc.Realm != "" {
// 						pm.node.messlogf("%v@%v: unix-socket server error: %v\n", svc.Name, svc.Realm, e)
// 					} else {
// 						pm.node.messlogf("%v: unix-socket server error: %v\n", svc.Name, e)
// 					}
// 				}
// 			}()
// 		}
// 		go func() {
// 			if e := pm.server.Serve(pm.tcpListener); e != nil && !errors.Is(e, http.ErrServerClosed) {
// 				if svc.Realm != "" {
// 					pm.node.messlogf("%v@%v: tcp-socket server error: %v\n", svc.Name, svc.Realm, e)
// 				} else {
// 					pm.node.messlogf("%v: tcp-socket server error: %v\n", svc.Name, e)
// 				}
// 			}
// 		}()
// 		return nil*/
// }

// func createServiceIncomingProxy(svc *mess.Service, binpath string) (*httputil.ReverseProxy, error) {
// 	network, addr, err := mess.ParseNetworkAddr(svc.Listen)
// 	if err != nil {
// 		return nil, fmt.Errorf("invalid Listen: %w", err)
// 	}
//
// 	transport := &http.Transport{
// 		ForceAttemptHTTP2:   true,
// 		MaxIdleConnsPerHost: 1024,
// 		IdleConnTimeout:     2 * time.Minute,
// 	}
//
// 	switch network {
// 	case "unix":
// 		if !filepath.IsAbs(addr) {
// 			if binpath == "" {
// 				return nil, fmt.Errorf("dev mode requires absolute path for unix sockets")
// 			}
// 			addr = filepath.Join(binpath, addr)
// 		}
// 		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
// 			return new(net.Dialer).DialContext(ctx, "unix", addr)
// 		}
//
// 	case "tcp":
// 		host, port, e := net.SplitHostPort(addr)
// 		if e != nil {
// 			return nil, fmt.Errorf("invalid Listen: %w", e)
// 		}
// 		if host == "" || host == "localhost" {
// 			host = "127.0.0.1"
// 		}
// 		dst := net.JoinHostPort(host, port)
// 		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
// 			return new(net.Dialer).DialContext(ctx, "tcp", dst)
// 		}
// 	}
//
// 	proxy := &httputil.ReverseProxy{
// 		Rewrite:       proxyRewrite,
// 		Transport:     transport,
// 		FlushInterval: -1,
// 		BufferPool:    bufpool,
// 	}
//
// 	return proxy, nil
// }
