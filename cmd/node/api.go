package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/mess/internal/proxy"
	"github.com/vapstack/mess/internal/sign"
	"github.com/vapstack/mess/internal/storage"
	"github.com/vapstack/mess/internal/tlsutil"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	OPS  = 1
	ROOT = 2
)

type apiHandler struct {
	dev    bool // allowed to handle requests in dev mode
	lock   bool // lock api global mutex
	local  bool // allowed to handle local requests
	public bool // allowed to handle external requests
	system bool // system endpoint
	access int  // access control (0 - everyone, ROOT - root-only, OPS - ops-only)

	fn func(n *node, w *proxy.Wrapper, r *http.Request)
}

func (ah *apiHandler) call(n *node, w *proxy.Wrapper, r *http.Request, fromLocal bool) {

	if ah.system {
		if r.Header.Get("Content-Type") != "application/gob" {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}
	}

	if n.dev && !ah.dev {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	if fromLocal {
		if !ah.local {
			w.WriteHeader(http.StatusForbidden)
			return
		}
	} else {
		if !ah.public {
			w.WriteHeader(http.StatusForbidden)
			return
		}
	}

	if ah.access == ROOT {
		if err := sign.Verify(n.pubk, r.Header.Get(sign.Header)); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	if ah.access == OPS || ah.access == ROOT {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if id, err := strconv.ParseUint(r.TLS.PeerCertificates[0].Subject.CommonName, 10, 64); err != nil || id != 0 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	if ah.lock {
		defer cover(n)(w)
	} else {
		defer readcover(w)
	}

	ah.fn(n, w, r)
}

var nodeHandlers map[string]*apiHandler

func init() {
	nodeHandlers = map[string]*apiHandler{

		"rotate": {
			lock:   true,
			public: true,
			access: ROOT,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[internal.RotateRequest](w, r, 0))
				rsCheck(n.rotateCert(req))
			},
		},
		"upgrade": {
			lock:   true,
			public: true,
			access: ROOT,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				b := sMust(io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20)))
				rsCheck(n.upgradeNode(b))
			},
		},
		"shutdown": {
			lock:   true,
			public: true,
			access: OPS,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				select {
				case <-n.ctx.Done():
					return
				default:
					n.stop()
				}
			},
		},

		/**/

		"state": {
			dev:    true,
			local:  true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(send(w, r, n.getState()))
			},
		},

		"logs": {
			dev:    true,
			local:  true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[mess.LogsRequest](w, r, 0))
				rFail(req.Service == "", "service is empty")
				if req.Stream {
					outStream(n, w, r, sMust(n.logsStream(req)))
				} else {
					sCheck(send(w, r, sMust(n.logsRequest(req))))
				}
			},
		},

		/**/

		"seq": {
			local:  true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(send(w, r, sMust(n.getSeq(w.Caller.Realm, r.URL.Query().Get("name")))))
			},
		},

		/**/

		"put": {
			lock:   true,
			public: true,
			access: OPS,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				s := rMust(BodyTo[mess.Service](w, r, 0))
				rFail(s.Name == "", "service name is empty")
				sCheck(n.apiPutService(s))
			},
		},

		"start": {
			local:  true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				q := r.URL.Query()
				service, realm := internal.ParseServiceRealm(rMust(getService(q)))
				rsCheck(n.serviceCommand("start", getRealm(realm, w, q), service))
			},
		},
		"stop": {
			local:  true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				q := r.URL.Query()
				service, realm := internal.ParseServiceRealm(rMust(getService(q)))
				rsCheck(n.serviceCommand("stop", getRealm(realm, w, q), service))
			},
		},
		"restart": {
			local:  true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				q := r.URL.Query()
				service, realm := internal.ParseServiceRealm(rMust(getService(q)))
				rsCheck(n.serviceCommand("restart", getRealm(realm, w, q), service))
			},
		},
		"delete": {
			lock:   true,
			public: true,
			access: OPS,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				q := r.URL.Query()
				service, realm := internal.ParseServiceRealm(rMust(getService(q)))
				rsCheck(n.serviceCommand("delete", getRealm(realm, w, q), service))
			},
		},

		"store": {
			public: true,
			access: OPS,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				q := r.URL.Query()
				service, realm := internal.ParseServiceRealm(rMust(getService(q)))
				rsCheck(n.storeServicePackage(getRealm(realm, w, q), service, http.MaxBytesReader(w, r.Body, 1<<30)))
			},
		},

		/**/

		"publish": {
			dev:   true,
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[mess.PublishRequest](w, r, 64<<20))
				rFail(req.Topic == "", "topic is empty")
				sCheck(send(w, r, sMust(n.publish(w.Caller.Realm, req))))
			},
		},
		"subscribe": {
			dev:   true,
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[mess.SubscribeRequest](w, r, 0))
				rFail(req.Topic == "", "topic is empty")
				// if req.Stream {
				outStream(n, w, r, sMust(n.subscriptionStream(w.Caller.Realm, req)))
				// } else {
				// 	sCheck(send(w, r, sMust(n.receiveRequest(w.Caller.Realm, req))))
				// }
			},
		},
		"events": {
			dev:    true,
			local:  true, // ~
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[mess.EventsRequest](w, r, 0))
				rFail(req.Topic == "", "topic is empty")
				if req.Realm == "" {
					req.Realm = w.Caller.Realm
				}
				if req.Stream {
					outStream(n, w, r, sMust(n.eventsStream(req)))
				} else {
					sCheck(send(w, r, sMust(n.eventsRequest(req))))
				}
			},
		},

		/**/

		"emit": {
			dev:   true,
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				// req := rMust(BodyTo[mess.EmitRequest](w, r, 16<<20))
				sCheck(errors.New("not implemented"))
			},
		},
		"listen": {
			dev:   true,
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(errors.New("not implemented"))
			},
		},

		/**/

		"post": {
			system: true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(errors.New("not implemented"))
			},
		},

		/**/

		"pulse": {
			system: true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				s := rMust(n.getPulseState(w, r))
				sCheck(gob.NewEncoder(w).Encode(sMust(n.applyPeerMap(s, r.RemoteAddr))))
			},
		},
	}
}

func BodyTo[T any](w *proxy.Wrapper, r *http.Request, limit int) (*T, error) {
	body := r.Body
	if limit > 0 {
		body = http.MaxBytesReader(w, r.Body, int64(limit))
	}
	v := new(T)
	return v, getContentTypeDecoder(body, getContentType(r)).Decode(v)
}

func getRealm(realm string, w *proxy.Wrapper, q url.Values) string {
	if realm == "" {
		if realm = q.Get("realm"); realm == "" {
			realm = w.Caller.Realm
		}
	}
	return realm
}

var (
	ErrServiceEmpty    = errors.New("service is not specified")
	ErrServiceNotFound = errors.New("service not found")
)

func getService(q url.Values) (string, error) {
	service := q.Get("service")
	if service == "" {
		return service, ErrServiceEmpty
	}
	return service, nil
}

/**/

func (n *node) getPulseState(w *proxy.Wrapper, r *http.Request) (*mess.NodeState, error) {

	if w.Caller.NodeID == n.id {
		n.logf("malformed pulse request: caller node ID equals to current node: %v", w.Caller.NodeID)
		return nil, internal.ErrInvalidCaller
	}

	if w.Caller.Service != mess.ServiceName {
		return nil, mess.ErrInternalEndpoint
	}

	if w.Target.NodeID > 0 && w.Target.NodeID != n.id {
		n.logf("malformed pulse request: wrong target node ID: want %v, got %v", n.id, w.Target.NodeID)
		return nil, internal.ErrInvalidCaller
	}

	s, err := BodyTo[mess.NodeState](w, r, 0)
	if err != nil {
		return nil, errors.New("invalid data")
	}

	if s.Node != nil && s.Node.ID != w.Caller.NodeID {
		n.logf("malformed pulse request: body/header node id mismatch: header: %v, body: %v", w.Caller.NodeID, s.Node.ID)
		return nil, internal.ErrInvalidCaller
	}

	return s, nil
}

func (n *node) serviceCommand(command string, realm, service string) (rerr error, serr error) {
	if pm := (*n.localServices.Load()).getByRealmAndName(realm, service); pm != nil {

		defer n.rebuildAliasMap()
		defer func() {
			if serr != nil {
				n.logf("error executing %v command: %v", command, serr)
			}
		}()

		s := pm.Service()

		switch command {
		case "start":
			log.Printf("starting service %v...\n", internal.ServiceName(s.Name, s.Realm))
			return nil, pm.Start()

		case "stop":
			log.Printf("stopping service %v...\n", internal.ServiceName(s.Name, s.Realm))
			pm.Stop()
			return nil, nil

		case "restart":
			log.Printf("stopping service %v...\n", internal.ServiceName(s.Name, s.Realm))
			pm.Stop()
			log.Printf("starting service %v...\n", internal.ServiceName(s.Name, s.Realm))
			return nil, pm.Start()

		case "delete":
			log.Printf("deleting service %v...\n", internal.ServiceName(s.Name, s.Realm))
			svc := pm.Service()
			err := pm.Delete()
			if err != nil {
				return nil, err
			}
			return nil, n.apiDeleteService(svc)

		default:
			return fmt.Errorf("unknown command: %v", command), nil
		}
	}
	return ErrServiceNotFound, nil
}

func (n *node) apiPutService(s *mess.Service) error {
	state := n.stateClone()
	ls := *n.localServices.Load()

	for _, svc := range state.Node.Services {
		if svc.Name == s.Name && svc.Realm == s.Realm {
			if pm := ls.get(s); pm != nil {

				pm.UpdateDefinition(s)

				if err := n.apiUpdateService(s); err != nil {
					return fmt.Errorf("error updating service: %w", err)
				}
				return nil
			}
			return errors.New("bug: failed to find service manager")
		}
	}
	state.Node.Services = append(state.Node.Services, s)

	if err := n.initManager(s); err != nil {
		return fmt.Errorf("error initializing process manager: %w", err)
	}

	if err := n.saveState(state); err != nil {
		return fmt.Errorf("error persisting node state: %w", err)
	}

	return nil
}

func (n *node) rotateCert(req *internal.RotateRequest) (error, error) {
	key, crt := []byte(req.Key), []byte(req.Crt)
	if len(key) == 0 || len(crt) == 0 {
		return errors.New("invalid request"), nil
	}
	tc, err := tls.X509KeyPair(crt, key)
	if err != nil {
		return nil, fmt.Errorf("error constructing X509 key pair: %w", err)
	}
	if err = tlsutil.VerifyKeyCert(n.pool, key, crt); err != nil {
		return fmt.Errorf("certificate validation failed: %w", err), nil
	}
	if err = storage.WriteFile("node.key", key); err != nil {
		return nil, fmt.Errorf("error persisting key: %w", err)
	}
	if err = storage.WriteFile("node.crt", crt); err != nil {
		return nil, fmt.Errorf("error persisting certificate: %w", err)
	}
	n.cert.Store(&tc)
	return nil, nil
}

func (n *node) storeServicePackage(realm, service string, r io.Reader) (error, error) {
	pm := (*n.localServices.Load()).getByRealmAndName(realm, service)
	if pm == nil {
		return ErrServiceNotFound, nil
	}

	t := filepath.Join(n.tmpdir, rand.Text())
	f, err := os.OpenFile(t, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("error creating temp file: %w", err)
	}
	defer removeFile(t)
	defer closeFile(f)

	if _, err = io.Copy(f, r); err != nil {
		return nil, fmt.Errorf("error writing payload: %w", err)
	}
	if err = f.Close(); err != nil {
		return nil, fmt.Errorf("error closing temporary file: %w", err)
	}

	if err = pm.StorePackage(t); err != nil {
		return nil, fmt.Errorf("error storing data package: %w", err)
	}

	return nil, nil

}

func (n *node) upgradeNode(bindata []byte) (error, error) {
	if len(bindata) < 1<<20 {
		return fmt.Errorf("file too small: %v", len(bindata)), nil
	}

	binName, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to get executable name: %w", err)
	}

	tmpName := binName + ".temp"
	f, err := os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o700)
	if err != nil {
		return nil, err
	}
	defer closeFile(f)

	l, err := f.Write(bindata)
	if err != nil {
		return nil, fmt.Errorf("write error: %w", err)
	} else if l != len(bindata) {
		return nil, fmt.Errorf("bytes written (%v) != bytes sent (%v)", l, len(bindata))
	}

	if err = f.Close(); err != nil {
		return nil, fmt.Errorf("close: %w", err)
	}

	if err = os.Rename(tmpName, binName); err != nil {
		return nil, fmt.Errorf("rename: %w", err)
	}

	return nil, nil
}

/**/

type cType byte

const (
	ctypeJSON = 1 << iota
	ctypeGOB
	ctypeMsgpack
)

type (
	Encoder interface{ Encode(v any) error }
	Decoder interface{ Decode(v any) error }
)

func getContentType(r *http.Request) cType {
	ctype := strings.TrimPrefix(r.Header.Get("Content-Type"), "application/")
	if ctype == "" {
		ctype = strings.TrimPrefix(r.Header.Get("Accept"), "application/")
	}
	if ctype == "gob" || ctype == "x-gob" {
		return ctypeGOB
	}
	if ctype == "" || strings.HasPrefix(ctype, "json") {
		return ctypeJSON
	}
	if ctype == "msgpack" || ctype == "x-msgpack" {
		return ctypeMsgpack
	}
	return ctypeJSON
}

func setContentType(w *proxy.Wrapper, ctype cType, streaming bool) {
	switch ctype {
	case ctypeJSON:
		if streaming {
			w.Header().Set("Content-Type", "application/jsonl")
		} else {
			w.Header().Set("Content-Type", "application/json")
		}
	case ctypeGOB:
		w.Header().Set("Content-Type", "application/gob")
	case ctypeMsgpack:
		w.Header().Set("Content-Type", "application/msgpack")
	default:
		if streaming {
			w.Header().Set("Content-Type", "application/jsonl")
		} else {
			w.Header().Set("Content-Type", "application/json")
		}
	}
}

func getContentTypeEncoder(w *proxy.Wrapper, ctype cType) Encoder {
	switch ctype {
	case ctypeJSON:
		return json.NewEncoder(w)
	case ctypeGOB:
		return gob.NewEncoder(w)
	case ctypeMsgpack:
		return msgpack.NewEncoder(w)
	}
	return json.NewEncoder(w)
}

func getContentTypeDecoder(r io.Reader, ctype cType) Decoder {
	switch ctype {
	case ctypeJSON:
		return json.NewDecoder(r)
	case ctypeGOB:
		return gob.NewDecoder(r)
	case ctypeMsgpack:
		return msgpack.NewDecoder(r)
	}
	return json.NewDecoder(r)
}

func getEncoder(w *proxy.Wrapper, r *http.Request, streaming bool) Encoder {
	ctype := getContentType(r)
	enc := getContentTypeEncoder(w, ctype)
	setContentType(w, ctype, streaming)
	return enc
}

func getDecoder(r *http.Request) Decoder {
	return getContentTypeDecoder(r.Body, getContentType(r))
}

/**/

func send(w *proxy.Wrapper, r *http.Request, v any) error {
	return getEncoder(w, r, false).Encode(v)
}

func outStream[T any](n *node, w *proxy.Wrapper, r *http.Request, producer Producer[T]) {

	rc := http.NewResponseController(w)

	enc := getEncoder(w, r, true)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	data := make(chan T, 32)
	go producer(ctx, data)

	defer func(rc *http.ResponseController) { _ = rc.Flush() }(rc)

	nDone := n.ctx.Done()
	cDone := ctx.Done()

	for {
		select {
		case <-nDone:
			return
		case <-cDone:
			return
		case v, open := <-data:
			if !open {
				return
			}

			if err := enc.Encode(v); err != nil {
				n.logf("stream error: encoding error: %v", err)
				return
			}
			if err := rc.Flush(); err != nil {
				n.logf("stream error: flush: %v", err)
				return
			}
		}
	}
}

/**/

func cover(n *node) func(w *proxy.Wrapper) {
	n.mu.Lock()
	return n.recoverUnlock
}

func (n *node) recoverUnlock(w *proxy.Wrapper) {
	defer n.mu.Unlock()
	if p := recover(); p != nil {
		if he, ok := p.(handlerError); ok {
			if he.req != nil {
				http.Error(w, he.req.Error(), http.StatusBadRequest)
			} else if he.srv != nil {
				http.Error(w, he.srv.Error(), http.StatusInternalServerError)
			}
			return
		}
		http.Error(w, fmt.Sprint(p), http.StatusInternalServerError)
	}
}

func readcover(w *proxy.Wrapper) {
	if p := recover(); p != nil {
		if he, ok := p.(handlerError); ok {
			if he.req != nil {
				http.Error(w, he.req.Error(), http.StatusBadRequest)
			} else if he.srv != nil {
				http.Error(w, he.srv.Error(), http.StatusInternalServerError)
			}
			return
		}
		http.Error(w, fmt.Sprint(p), http.StatusInternalServerError)
	}
}

/**/

type handlerError struct{ req, srv error }

func rMust[T any](v T, err error) T {
	if err != nil {
		panic(handlerError{err, nil})
	}
	return v
}
func rCheck(err error) {
	if err != nil {
		panic(handlerError{err, nil})
	}
}
func rFail(cond bool, text string) {
	if cond {
		panic(handlerError{errors.New(text), nil})
	}
}
func sFail(cond bool, text string) {
	if cond {
		panic(handlerError{nil, errors.New(text)})
	}
}
func sMust[T any](v T, err error) T {
	if err != nil {
		panic(handlerError{nil, err})
	}
	return v
}
func sCheck(err error) {
	if err != nil {
		panic(handlerError{nil, err})
	}
}
func rsCheck(rerr, serr error) {
	if rerr != nil || serr != nil {
		panic(handlerError{rerr, serr})
	}
}

var publicPortStr = fmt.Sprintf(":%v", mess.PublicPort)
