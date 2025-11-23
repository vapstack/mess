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
	"strings"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/mess/internal/proxy"
	"github.com/vmihailenco/msgpack/v5"
)

type apiHandler struct {
	lock   bool
	local  bool
	public bool
	gob    bool
	method string
	verify bool

	fn func(n *node, w *proxy.Wrapper, r *http.Request)
}

func (ah *apiHandler) call(n *node, w *proxy.Wrapper, r *http.Request, fromLocal bool) {
	if ah.gob {
		if r.Header.Get("Content-Type") != "application/gob" {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}
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

	if ah.method == "" {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	} else if r.Method != ah.method {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if ah.verify {
		if err := verifySignatureHeader(n.pubk, r.Header.Get(internal.SigHeader)); err != nil {
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
			verify: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[internal.RotateRequest](w, r, 0))
				rsCheck(n.rotateCert(req))
			},
		},
		"upgrade": {
			lock:   true,
			public: true,
			verify: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				b := sMust(io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<26)))
				rsCheck(n.upgradeNode(b))
			},
		},
		"shutdown": {
			lock:   true,
			public: true,
			verify: true,
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
			local:  true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(send(w, r, n.getState()))
			},
		},

		"logs": {
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

		"put": {
			lock:   true,
			public: true,
			verify: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				s := rMust(BodyTo[mess.Service](w, r, 0))
				rFail(s.Name == "", "service name is empty")
				sCheck(n.putService(s))
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
				service, realm := internal.ParseServiceRealm(rMust(getService(q))) // mess/restart/?service=UserService@amk
				rsCheck(n.serviceCommand("restart", getRealm(realm, w, q), service))
			},
		},
		"delete": {
			lock:   true,
			public: true,
			verify: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				q := r.URL.Query()
				service, realm := internal.ParseServiceRealm(rMust(getService(q)))
				rsCheck(n.serviceCommand("delete", getRealm(realm, w, q), service))
			},
		},

		"store": {
			public: true,
			verify: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				q := r.URL.Query()
				service, realm := internal.ParseServiceRealm(rMust(getService(q)))
				rsCheck(n.storeServicePackage(getRealm(realm, w, q), service, http.MaxBytesReader(w, r.Body, 1<<30)))
			},
		},

		/**/

		"publish": {
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[mess.PublishRequest](w, r, 1<<26)) // max 64 Mb in total
				rFail(req.Topic == "", "topic is empty")
				sCheck(n.publish(w.Caller.Realm, req))
			},
		},
		"receive": {
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[mess.ReceiveRequest](w, r, 0))
				rFail(req.Topic == "", "topic is empty")
				if req.Stream {
					outStream(n, w, r, sMust(n.receiveStream(w.Caller.Realm, req)))
				} else {
					sCheck(send(w, r, sMust(n.receiveRequest(w.Caller.Realm, req))))
				}
			},
		},
		"events": {
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				req := rMust(BodyTo[mess.EventsRequest](w, r, 0))
				rFail(req.Topic == "", "topic is empty")
				if req.Stream {
					outStream(n, w, r, sMust(n.eventsStream(req)))
				} else {
					sCheck(send(w, r, sMust(n.eventsRequest(req))))
				}
			},
		},

		/**/

		"emit": {
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(errors.New("not implemented"))
			},
		},
		"watch": {
			local: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(errors.New("not implemented"))
			},
		},
		"post": {
			gob:    true,
			public: true,
			fn: func(n *node, w *proxy.Wrapper, r *http.Request) {
				sCheck(errors.New("not implemented"))
			},
		},

		/**/

		"pulse": {
			gob:    true,
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

// func getTopic(topic string, q url.Values) (string, error) {
// 	if topic == "" {
// 		if topic = q.Get("topic"); topic == "" {
// 			return topic, fmt.Errorf("no topic specified")
// 		}
// 	}
// 	return topic, nil
// }

// func getFields(q url.Values) []mess.Field {
// 	var fields []mess.Field
// 	for k, v := range q {
// 		if strings.HasPrefix(k, "meta.") && len(v) > 0 && v[0] != "" {
// 			fields = append(fields, mess.Field{strings.TrimPrefix(k, "meta."), v[0]})
// 		}
// 	}
// 	return fields
// }

// func isStream(q url.Values) bool {
// 	v, _ := strconv.ParseBool(q.Get("stream"))
// 	return v
// }
//
// func getOffset(q url.Values) int64 {
// 	v, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
// 	return v
// }
//
// func getLimit(q url.Values) uint64 {
// 	v, _ := strconv.ParseUint(q.Get("limit"), 10, 64)
// 	return v
// }

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
		n.logf("malformed pulse request: body/header node ID mismatch: header: %v, body: %v", w.Caller.NodeID, s.Node.ID)
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
			log.Printf("starting service %v@%v...\n", s.Name, s.Realm)
			return nil, pm.Start()

		case "stop":
			log.Printf("stopping service %v@%v...\n", s.Name, s.Realm)
			return nil, pm.Stop()

		case "restart":
			log.Printf("stopping service %v@%v...\n", s.Name, s.Realm)
			if err := pm.Stop(); err != nil {
				return nil, err
			}
			log.Printf("starting service %v@%v...\n", s.Name, s.Realm)
			return nil, pm.Start()

		case "delete":
			log.Printf("deleting service %v@%v...\n", s.Name, s.Realm)
			svc := pm.Service()
			err := pm.Delete()
			if err != nil {
				return nil, err
			}
			return nil, n.deleteService(svc)

		default:
			return fmt.Errorf("unknown command: %v", command), nil
		}
	}
	return ErrServiceNotFound, nil
}

func (n *node) putService(s *mess.Service) error {
	// lock acquired in handler
	d := n.stateClone()
	ls := *n.localServices.Load()
	for _, svc := range d.Node.Services {
		if svc.Name == s.Name && svc.Realm == s.Realm {
			if pm := ls.get(s); pm != nil {
				if err := pm.Update(s); err != nil {
					return err
				}
				if err := n.updateService(s); err != nil {
					return fmt.Errorf("error updating service: %w", err)
				}
				return nil
			}
			return errors.New("bug: failed to find service manager")
		}
	}
	d.Node.Services = append(d.Node.Services, s)
	if err := n.runService(s); err != nil {
		return fmt.Errorf("error initializing process manager: %w", err)
	}
	if err := n.saveState(d); err != nil {
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
	if err = VerifyKeyCert(n.pool, key, crt); err != nil {
		return fmt.Errorf("certificate validation failed: %w", err), nil
	}
	if err = internal.WriteFile("node.key", key); err != nil {
		return nil, fmt.Errorf("error persisting key: %w", err)
	}
	if err = internal.WriteFile("node.crt", crt); err != nil {
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
	f, err := os.OpenFile(t, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("error creating temp file: %w", err)
	}
	defer loggedRemove(t)
	defer silentClose(f)

	_, err = io.Copy(f, r)
	if err = f.Close(); err != nil {
		return nil, fmt.Errorf("error closing temporary file: %w", err)
	}

	if err = pm.Store(t); err != nil {
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
	f, err := os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0700)
	if err != nil {
		return nil, err
	}
	defer silentClose(f)

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

// var sendBufPool = sync.Pool{
// 	New: func() any { return new(bytes.Buffer) },
// }

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
func rMust2[T1, T2 any](v1 T1, v2 T2, err error) (T1, T2) {
	if err != nil {
		panic(handlerError{err, nil})
	}
	return v1, v2
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
