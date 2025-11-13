package main

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mess"
	"mess/internal"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

var nodeHandlers map[string]func(n *node, w http.ResponseWriter, r *http.Request)

func init() {
	nodeHandlers = map[string]func(n *node, w http.ResponseWriter, r *http.Request){

		"rotate": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer cover(n, w)()
			req := requestValue(BodyTo[internal.RotateRequest](r))
			key, crt := []byte(req.Key), []byte(req.Crt)
			tc := requestValue(tls.X509KeyPair(crt, key))
			requestCheck(VerifyKeyCert(n.pool, key, crt))
			serverCheck(internal.WriteFile("node.key", key))
			serverCheck(internal.WriteFile("node.crt", crt))
			n.cert.Store(&tc)
			return
		},

		"pulse": func(n *node, w http.ResponseWriter, r *http.Request) {
			if gobOnly(w, r) {
				d := requestValue(BodyTo[mess.NodeState](r))
				serverCheck(send(w, r, serverValue(n.applyPeerMap(d, r.RemoteAddr))))
			}
		},

		"state": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer readcover(w)()
			serverCheck(send(w, r, n.getStatefullData()))
		},

		"upgrade": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer cover(n, w)()
			b := serverValue(io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<26)))
			serverCheck(n.upgrade(b))
		},

		"shutdown": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer cover(n, w)()
			select {
			case <-n.ctx.Done():
				return
			default:
				n.stop()
			}
		},

		"put": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer cover(n, w)()
			s := requestValue(BodyTo[mess.Service](r))
			d := n.stateClone()
			ls := *n.localServices.Load()
			for _, svc := range d.Node.Services {
				if svc.Name == s.Name && svc.Realm == s.Realm {
					if pm := ls.get(s); pm != nil {
						serverCheck(pm.Update(s))
						serverCheck(n.updateService(s))
						return
					}
					serverCheck(fmt.Errorf("bug: failed to find service manager"))
				}
			}
			d.Node.Services = append(d.Node.Services, s)
			serverCheck(n.runService(s))
			serverCheck(n.storeState(d))
		},

		"start": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer readcover(w)()
			multiCheck(n.serviceCommand("start", requestValue(BodyTo[internal.ServiceCommand](r))))
		},
		"stop": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer readcover(w)()
			multiCheck(n.serviceCommand("stop", requestValue(BodyTo[internal.ServiceCommand](r))))
		},
		"restart": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer readcover(w)()
			multiCheck(n.serviceCommand("restart", requestValue(BodyTo[internal.ServiceCommand](r))))
		},
		"delete": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer cover(n, w)()
			multiCheck(n.serviceCommand("delete", requestValue(BodyTo[internal.ServiceCommand](r))))
		},

		"store": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer readcover(w)()
			q := r.URL.Query()
			realm, service := q.Get("realm"), q.Get("service")
			if pm := (*n.localServices.Load()).getByRealmAndName(realm, service); pm != nil {
				b := http.MaxBytesReader(w, r.Body, 1<<30)
				t := filepath.Join(n.tmpdir, rand.Text())
				f := serverValue(os.OpenFile(t, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600))
				defer func() { _ = os.Remove(t) }()
				defer func() { _ = f.Close() }()
				_ = serverValue(io.Copy(f, b))
				serverCheck(f.Close())
				serverCheck(pm.Store(t))
				if q.Get("restart") == "1" {
					timeout, _ := strconv.Atoi(q.Get("timeout"))
					multiCheck(n.serviceCommand("restart", &internal.ServiceCommand{
						Realm:   realm,
						Service: service,
						Timeout: timeout,
					}))
				}
				return
			}
			requestCheck(fmt.Errorf("service not found"))
		},

		"logs": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer readcover(w)()
			req := requestValue(BodyTo[internal.LogsRequest](r))
			// res := []mess.LogRecord // (*internal.LogsResponse)(nil)
			var logs []mess.LogRecord
			if req.Service == mess.NodeService {
				// res = &internal.LogsResponse{Logs: serverValue(n.readLogs(messLogName, messLogName, req.Offset, req.Limit))}
				logs = serverValue(n.readLogs(messLogName, messLogName, req.Offset, req.Limit))
			} else {
				// res = &internal.LogsResponse{Logs: serverValue(n.readLogs(req.Realm, req.Service, req.Offset, req.Limit))}
				logs = serverValue(n.readLogs(req.Realm, req.Service, req.Offset, req.Limit))
			}
			serverCheck(send(w, r, logs))
		},

		"publish": func(n *node, w http.ResponseWriter, r *http.Request) {
			defer readcover(w)()
			// req := requestValue(BodyTo[internal.LogsRequest](r))
		},
		"subscribe": func(n *node, w http.ResponseWriter, r *http.Request) {

		},
		"cursor": func(n *node, w http.ResponseWriter, r *http.Request) {

		},
	}
}

func BodyTo[T any](r *http.Request) (*T, error) {
	v := new(T)
	if r.Header.Get("Content-Type") == "application/gob" {
		return v, gob.NewDecoder(r.Body).Decode(v)
	} else {
		return v, json.NewDecoder(r.Body).Decode(v)
	}
}

/**/

func (n *node) serviceCommand(command string, cmd *internal.ServiceCommand) (rerr error, serr error) {
	if pm := (*n.localServices.Load()).getByRealmAndName(cmd.Realm, cmd.Service); pm != nil {

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
			return nil, pm.Stop(cmd.Timeout)

		case "restart":
			log.Printf("stopping service %v@%v...\n", s.Name, s.Realm)
			if err := pm.Stop(cmd.Timeout); err != nil {
				return nil, err
			}
			log.Printf("starting service %v@%v...\n", s.Name, s.Realm)
			return nil, pm.Start()

		case "delete":
			log.Printf("deleting service %v@%v...\n", s.Name, s.Realm)
			err := pm.Delete()
			if err != nil {
				return nil, err
			}
			return nil, n.deleteService(pm.Service())

		default:
			return fmt.Errorf("unknown command: %v", command), nil
		}
	}
	return fmt.Errorf("service not found"), nil
}

/**/

var sendBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func send(w http.ResponseWriter, r *http.Request, v any) error {

	if r.Header.Get("Content-Type") == "application/gob" {
		w.Header().Set("Content-Type", "application/gob")

		buf := sendBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer sendBufPool.Put(buf)

		if err := gob.NewEncoder(buf).Encode(v); err != nil {
			return err
		}

		_, err := buf.WriteTo(w)
		return err
	}

	w.Header().Set("Content-Type", "application/json")

	buf := sendBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer sendBufPool.Put(buf)

	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")

	if err := enc.Encode(v); err != nil {
		return err
	}

	_, err := buf.WriteTo(w)
	return err
}

/**/

func cover(n *node, w http.ResponseWriter) func() {
	n.mu.Lock()
	return func() {
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
}

func readcover(w http.ResponseWriter) func() {
	return func() {
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
}

/**/

type handlerError struct{ req, srv error }

func requestValue[T any](v T, err error) T {
	if err != nil {
		panic(handlerError{err, nil})
	}
	return v
}
func requestCheck(err error) {
	if err != nil {
		panic(handlerError{err, nil})
	}
}
func serverValue[T any](v T, err error) T {
	if err != nil {
		panic(handlerError{nil, err})
	}
	return v
}
func serverCheck(err error) {
	if err != nil {
		panic(handlerError{nil, err})
	}
}
func multiCheck(rerr, serr error) {
	if rerr != nil || serr != nil {
		panic(handlerError{rerr, serr})
	}
}
func gobOnly(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Content-Type") != "application/gob" {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return false
	}
	return true
}
