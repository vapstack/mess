package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
)

/**/

type (
	Wrapper struct {
		writer http.ResponseWriter

		Start time.Time

		Caller proxyArgs
		Target proxyArgs

		InProto string

		Scheme string
		Host   string

		Status int
		Bytes  uint64
	}
	proxyArgs struct {
		NodeID  uint64
		Realm   string
		Service string
	}
)

// func Client(hr *http.Request) (*Wrapper, *http.Request) {
//
// }

func Wrap(hw http.ResponseWriter, hr *http.Request) (*Wrapper, *http.Request) {
	w := wPool.Get().(*Wrapper)

	*w = Wrapper{
		writer: hw,

		Start:  time.Now(),
		Status: http.StatusOK,

		InProto: hr.Proto,
	}

	r := hr.WithContext(context.WithValue(hr.Context(), ctxProxyBaseKey, w))

	return w, r
}

func (w *Wrapper) Release() {
	w.writer = nil
	w.Scheme = ""
	w.Host = ""
	wPool.Put(w)
}

var wPool = sync.Pool{
	New: func() any { return new(Wrapper) },
}

func (w *Wrapper) WriteHeader(code int) {
	w.Status = code
	w.writer.WriteHeader(code)
}

func (w *Wrapper) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.Bytes += uint64(n)
	return n, err
}

func (w *Wrapper) Header() http.Header {
	return w.writer.Header()
}

func (w *Wrapper) Unwrap() http.ResponseWriter {
	return w.writer
}

var publicPortStr = fmt.Sprintf(":%v", mess.PublicPort)

func (w *Wrapper) ToLocal() {
	w.Scheme = "http"
	w.Host = "localhost"
}

func (w *Wrapper) ToRemote(tn *mess.Node) error {
	addr := tn.Address()
	if addr == "" {
		return errors.New("no host address")
	}
	w.Scheme = "https"
	w.Host = tn.Address() + publicPortStr
	w.Target.NodeID = tn.ID
	return nil
}

func (w *Wrapper) FromLocal(r *http.Request) (err error) {
	callerHeader := r.Header.Get(mess.CallerHeader)
	w.Caller.NodeID, w.Caller.Realm, w.Caller.Service, err = internal.ParseCaller(callerHeader)
	if err != nil {
		return err
	}
	// w.Caller.NodeID = nodeID
	// w.Caller.Realm = r.Header.Get(mess.CallerRealmHeader)
	// w.Caller.Service = r.Header.Get(mess.CallerServiceHeader)
	//
	// if h := r.Header.Get(mess.TargetNodeHeader); h != "" {
	// 	id, err := strconv.ParseUint(h, 10, 64)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	w.Target.NodeID = id
	// }

	w.Target.Service = r.URL.Hostname()

	if h := r.Header.Get(mess.TargetServiceHeader); h != "" {
		w.Target.Service = h
	}

	w.Target.Realm = r.Header.Get(mess.TargetRealmHeader)
	w.Target.NodeID, err = parseTargetNode(mess.TargetNodeHeader)

	return err
}

func (w *Wrapper) FromRemote(r *http.Request) (err error) {
	callerHeader := r.Header.Get(mess.CallerHeader)
	w.Caller.NodeID, w.Caller.Realm, w.Caller.Service, err = internal.ParseCaller(callerHeader)
	if err != nil {
		return err
	}

	// if h := r.Header.Get(mess.CallerNodeHeader); h != "" {
	// 	id, err := strconv.ParseUint(h, 10, 64)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	w.Caller.NodeID = id
	// }
	// w.Caller.Realm = r.Header.Get(mess.CallerRealmHeader)
	// w.Caller.Service = r.Header.Get(mess.CallerServiceHeader)
	//
	// if h := r.Header.Get(mess.TargetNodeHeader); h != "" {
	// 	id, err := strconv.ParseUint(h, 10, 64)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	w.Target.NodeID = id
	// }

	w.Target.Service = r.Header.Get(mess.TargetServiceHeader)
	w.Target.Realm = r.Header.Get(mess.TargetRealmHeader)
	w.Target.NodeID, err = parseTargetNode(mess.TargetNodeHeader)

	return err
}

func parseTargetNode(header string) (uint64, error) {
	if header == "" {
		return 0, nil
	}
	return strconv.ParseUint(header, 10, 64)
}
