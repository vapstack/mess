package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"mess"
	"mess/internal/proxy"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"
)

//
// type ctxKey string
//
// const ctxProxyBaseKey ctxKey = "proxyBase"
//
// func proxyRewrite(pr *httputil.ProxyRequest) {
// 	b := pr.In.Context().Value(ctxProxyBaseKey).(*proxyBase)
//
// 	pr.Out.URL.Scheme = b.scheme
// 	pr.Out.URL.Host = b.host
// 	pr.Out.Host = b.host
//
// 	if b.caller.nodeID > 0 {
// 		pr.Out.Header.Set(mess.CallerNodeHeader, strconv.FormatUint(b.caller.nodeID, 10))
// 	}
// 	if b.target.nodeID > 0 {
// 		pr.Out.Header.Set(mess.TargetNodeHeader, strconv.FormatUint(b.target.nodeID, 10))
// 	}
//
// 	if b.caller.realm != "" {
// 		pr.Out.Header.Set(mess.CallerRealmHeader, b.caller.realm)
// 	}
// 	if b.target.realm != "" {
// 		pr.Out.Header.Set(mess.TargetRealmHeader, b.target.realm)
// 	}
//
// 	if b.caller.service != "" {
// 		pr.Out.Header.Set(mess.CallerServiceHeader, b.caller.service)
// 	}
// 	if b.target.service != "" {
// 		pr.Out.Header.Set(mess.TargetServiceHeader, b.target.service)
// 	}
// }

/**/

func (n *node) localHandler(hw http.ResponseWriter, hr *http.Request) {

	b, w := proxy.GetBase(hw) // n.getProxyBase(hw)
	defer b.Release()
	defer n.collectMetrics(b)

	if err := b.FromLocal(n.id, hr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	r := hr.WithContext(proxy.WithBase(hr.Context(), b)) // hr.WithContext(context.WithValue(hr.Context(), ctxProxyBaseKey, b))

	/*if n.dev {

		if b.target.service == mess.MessService {
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

	d := n.data.Load()

	if b.Target.Service == mess.MessService {

		if b.Target.NodeID == 0 || b.Target.NodeID == n.id {
			b.ToLocal()
			n.nodeHandler(w, r, strings.Split(strings.Trim(r.URL.Path, "/"), "/"))
			return
		}

		if tn, ok = d.Map[b.Target.NodeID]; !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

	} else if b.Target.NodeID == 0 || b.Target.NodeID == n.id {

		if n.routeToLocal(w, r, b) {
			return
		}

		if b.Target.NodeID == n.id {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		nodes := (*n.remoteServices.Load()).getByRealmAndName(b.Target.Realm, b.Target.Service)
		if len(nodes) == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		tn = nodes[0]

	} else if tn, ok = d.Map[b.Target.NodeID]; !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if n.dev {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	b.ToRemote(tn)
	n.proxy.ServeHTTP(w, r)
	return
}

func (n *node) routeToLocal(w http.ResponseWriter, r *http.Request, b *proxy.Base) bool {

	if pm := (*n.localServices.Load()).getByRealmAndName(b.Target.Realm, b.Target.Service); pm != nil {
		if !pm.Passive() {
			if p := pm.Proxy(); p != nil {
				b.ToLocal()
				p.ServeHTTP(w, r)
				return true
			}
		}
	}
	if rr := (*n.localAliases.Load()).getByRealmAndName(b.Target.Realm, b.Target.Service); rr != nil {
		if len(rr.pms) == 1 {
			if p := rr.pms[0].Proxy(); p != nil {
				b.ToLocal()
				p.ServeHTTP(w, r)
				return true
			}
		} else if len(rr.pms) > 1 {
			for range rr.pms {
				if pm := rr.pms[int(rr.rr.Add(1)%uint64(len(rr.pms)))]; pm != nil {
					if p := pm.Proxy(); p != nil {
						b.ToLocal()
						p.ServeHTTP(w, r)
						return true
					}
				}
			}
		}
	}
	return false
}

func (n *node) publicHandler(hw http.ResponseWriter, hr *http.Request) {

	b, w := proxy.GetBase(hw) // n.getProxyBase(hw)
	defer b.Release()
	defer n.collectMetrics(b)

	// r := hr.WithContext(context.WithValue(hr.Context(), ctxProxyBaseKey, b))
	r := hr.WithContext(proxy.WithBase(hr.Context(), b))

	if err := b.FromRemote(r); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if b.Target.Service == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if b.Target.Service == mess.MessService {
		b.ToLocal()
		n.nodeHandler(w, r, strings.Split(strings.Trim(r.URL.Path, "/"), "/"))
		return
	}

	if n.routeToLocal(w, r, b) {
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func (n *node) nodeHandler(w http.ResponseWriter, r *http.Request, path []string) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if len(path) != 1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fn, ok := nodeHandlers[path[0]]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fn(n, w, r)
}

func (n *node) setupProxyClient() {

	n.client = &http.Client{
		Transport: &http.Transport{
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
		},
	}

	n.proxy = &httputil.ReverseProxy{
		Rewrite:       proxy.Rewrite,
		Transport:     n.client.Transport,
		BufferPool:    proxy.BufferPool,
		FlushInterval: -1,
	}
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
	addr := fmt.Sprintf("%v:%v", n.data.Load().Bind, mess.PublicPort)
	publicListener, lerr := net.Listen("tcp", addr)
	if lerr != nil {
		return nil, nil, fmt.Errorf("listen tcp %v: %w", addr, err)
	}

	publicServer := &http.Server{
		Handler: http.HandlerFunc(n.publicHandler),
		TLSConfig: &tls.Config{
			ClientCAs:             n.pool,
			GetCertificate:        n.getCert,
			VerifyPeerCertificate: n.verifyPeerCert,
			ClientAuth:            tls.RequireAndVerifyClientCert,
			MinVersion:            tls.VersionTLS13,
		},
		ErrorLog: log.New(io.Discard, "", 0),
	}

	stopfn = func() {
		// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		// defer cancel()
		log.Println("stopping public server...")
		if e := publicServer.Shutdown(context.Background()); e != nil {
			n.messlogf("public server shutdown error: %v", e)
			// log.Printf("public server shutdown error: %v\n", e)
		}
	}

	errch = make(chan error, 1)
	go func() {
		defer close(errch)
		if e := publicServer.ServeTLS(publicListener, "", ""); e != nil && !errors.Is(e, http.ErrServerClosed) {
			errch <- e
		}
	}()
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
