package proxy

import (
	"context"
	"net/http/httputil"
	"strconv"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
)

type ctxKey string

const ctxProxyBaseKey ctxKey = "proxyBase"

// func NewContext(parent context.Context, w *Wrapper) context.Context {
// 	return context.WithValue(parent, ctxProxyBaseKey, w)
// }

func FromContext(ctx context.Context) *Wrapper {
	w, _ := ctx.Value(ctxProxyBaseKey).(*Wrapper)
	return w
}

func Rewrite(pr *httputil.ProxyRequest) {
	w := FromContext(pr.In.Context())

	pr.Out.URL.Scheme = w.Scheme
	pr.Out.URL.Host = w.Host
	pr.Out.Host = w.Host

	pr.Out.Header.Set(mess.CallerHeader, internal.ConstructCaller(w.Caller.NodeID, w.Caller.Realm, w.Caller.Service))

	if w.Target.NodeID > 0 {
		pr.Out.Header.Set(mess.TargetNodeHeader, strconv.FormatUint(w.Target.NodeID, 10))
	}
	if w.Target.Realm != "" {
		pr.Out.Header.Set(mess.TargetRealmHeader, w.Target.Realm)
	}
	if w.Target.Service != "" {
		pr.Out.Header.Set(mess.TargetServiceHeader, w.Target.Service)
	}
}
