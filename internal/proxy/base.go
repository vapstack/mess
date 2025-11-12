package proxy

import (
	"bufio"
	"fmt"
	"io"
	"mess"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

/**/

type (
	Base struct {
		http.ResponseWriter

		Start    time.Time
		Duration time.Duration

		Caller proxyArgs
		Target proxyArgs

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

func GetBase(w http.ResponseWriter) (*Base, http.ResponseWriter) {
	b := basePool.Get().(*Base)

	*b = Base{
		ResponseWriter: w,

		Start:  time.Now(),
		Status: http.StatusOK,
	}

	return b, wrapWriter(b, w)
}

func (b *Base) Release() {
	b.ResponseWriter = nil
	b.Scheme = ""
	b.Host = ""
	basePool.Put(b)
}

var basePool = sync.Pool{
	New: func() any { return new(Base) },
}

func (b *Base) WriteHeader(code int) {
	b.Status = code
	b.ResponseWriter.WriteHeader(code)
}

func (b *Base) Write(p []byte) (int, error) {
	n, err := b.ResponseWriter.Write(p)
	b.Bytes += uint64(n)
	return n, err
}

var publicPortStr = fmt.Sprintf(":%v", mess.PublicPort)

func (b *Base) ToLocal() {
	b.Scheme = "http"
	b.Host = "localhost"
}

func (b *Base) ToRemote(tn *mess.Node) {
	b.Scheme = "https"
	b.Host = tn.Address() + publicPortStr
	b.Target.NodeID = tn.ID
}

func (b *Base) FromLocal(nodeid uint64, r *http.Request) error {
	b.Caller.NodeID = nodeid
	b.Caller.Realm = r.Header.Get(mess.CallerRealmHeader)
	b.Caller.Service = r.Header.Get(mess.CallerServiceHeader)

	if h := r.Header.Get(mess.TargetNodeHeader); h != "" {
		id, err := strconv.ParseUint(h, 10, 64)
		if err != nil {
			return err
		}
		b.Target.NodeID = id
	}
	b.Target.Realm = r.Header.Get(mess.TargetRealmHeader)
	b.Target.Service = r.URL.Hostname()

	if h := r.Header.Get(mess.TargetServiceHeader); h != "" {
		b.Target.Service = h
	}

	return nil
}

func (b *Base) FromRemote(r *http.Request) error {
	if h := r.Header.Get(mess.CallerNodeHeader); h != "" {
		id, err := strconv.ParseUint(h, 10, 64)
		if err != nil {
			return err
		}
		b.Caller.NodeID = id
	}
	b.Caller.Realm = r.Header.Get(mess.CallerRealmHeader)
	b.Caller.Service = r.Header.Get(mess.CallerServiceHeader)

	if h := r.Header.Get(mess.TargetNodeHeader); h != "" {
		id, err := strconv.ParseUint(h, 10, 64)
		if err != nil {
			return err
		}
		b.Target.NodeID = id
	}
	b.Target.Realm = r.Header.Get(mess.TargetRealmHeader)
	b.Target.Service = r.Header.Get(mess.TargetServiceHeader)

	return nil
}

//
// @#$%!
//

func wrapWriter(bw *Base, w http.ResponseWriter) http.ResponseWriter {

	_, f := w.(http.Flusher)
	_, h := w.(http.Hijacker)
	_, p := w.(http.Pusher)
	_, r := w.(io.ReaderFrom)

	if f {
		if h {
			if p {
				if r {
					return &lrwFHPR{bw}
				} else {
					return &lrwFHP{bw}
				}
			} else {
				if r {
					return &lrwFHR{bw}
				} else {
					return &lrwFH{bw}
				}
			}
		} else {
			if p {
				if r {
					return &lrwFPR{bw}
				} else {
					return &lrwFP{bw}
				}
			} else {
				if r {
					return &lrwFR{bw}
				} else {
					return &lrwF{bw}
				}
			}
		}
	} else {
		if h {
			if p {
				if r {
					return &lrwHPR{bw}
				} else {
					return &lrwHP{bw}
				}
			} else {
				if r {
					return &lrwHR{bw}
				} else {
					return &lrwH{bw}
				}
			}
		} else {
			if p {
				if r {
					return &lrwPR{bw}
				} else {
					return &lrwP{bw}
				}
			} else {
				if r {
					return &lrwR{bw}
				} else {
					return bw
				}
			}
		}
	}
}

/**/

type lrwF struct{ *Base }

func (w *lrwF) Flush() { w.ResponseWriter.(http.Flusher).Flush() }

/**/

type lrwH struct{ *Base }

func (w *lrwH) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

/**/

type lrwP struct{ *Base }

func (w *lrwP) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}

/**/

type lrwR struct{ *Base }

func (w *lrwR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}

/**/

type lrwFH struct{ *Base }

func (w *lrwFH) Flush() { w.ResponseWriter.(http.Flusher).Flush() }
func (w *lrwFH) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

/**/

type lrwFP struct{ *Base }

func (w *lrwFP) Flush() { w.ResponseWriter.(http.Flusher).Flush() }
func (w *lrwFP) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}

/**/

type lrwFR struct{ *Base }

func (w *lrwFR) Flush() { w.ResponseWriter.(http.Flusher).Flush() }
func (w *lrwFR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}

/**/

type lrwHP struct{ *Base }

func (w *lrwHP) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}
func (w *lrwHP) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}

/**/

type lrwHR struct{ *Base }

func (w *lrwHR) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}
func (w *lrwHR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}

/**/

type lrwPR struct{ *Base }

func (w *lrwPR) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}
func (w *lrwPR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}

/**/

type lrwFHP struct{ *Base }

func (w *lrwFHP) Flush() { w.ResponseWriter.(http.Flusher).Flush() }
func (w *lrwFHP) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}
func (w *lrwFHP) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}

/**/

type lrwFHR struct{ *Base }

func (w *lrwFHR) Flush() { w.ResponseWriter.(http.Flusher).Flush() }
func (w *lrwFHR) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}
func (w *lrwFHR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}

/**/

type lrwFPR struct{ *Base }

func (w *lrwFPR) Flush() { w.ResponseWriter.(http.Flusher).Flush() }
func (w *lrwFPR) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}
func (w *lrwFPR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}

/**/

type lrwHPR struct{ *Base }

func (w *lrwHPR) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}
func (w *lrwHPR) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}
func (w *lrwHPR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}

/**/

type lrwFHPR struct{ *Base }

func (w *lrwFHPR) Flush() { w.ResponseWriter.(http.Flusher).Flush() }
func (w *lrwFHPR) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}
func (w *lrwFHPR) Push(target string, opts *http.PushOptions) error {
	return w.ResponseWriter.(http.Pusher).Push(target, opts)
}
func (w *lrwFHPR) ReadFrom(r io.Reader) (int64, error) {
	b, e := w.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
	w.Bytes += uint64(b)
	return b, e
}
