package proxy

import (
	"context"
	"net/http/httputil"
	"strconv"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
)

type ctxKey string

const ctxProxyKey ctxKey = "proxyBase"

func FromContext(ctx context.Context) *Wrapper {
	w, _ := ctx.Value(ctxProxyKey).(*Wrapper)
	return w
}

func Rewrite(pr *httputil.ProxyRequest) {
	w := FromContext(pr.In.Context())

	if w == nil {
		panic("proxy.Rewrite: no wrapper in context (bug)")
	}

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
