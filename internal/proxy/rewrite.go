package proxy

import (
	"context"
	"mess"
	"net/http/httputil"
	"strconv"
)

type ctxKey string

const ctxProxyBaseKey ctxKey = "proxyBase"

func WithBase(parent context.Context, b *Base) context.Context {
	return context.WithValue(parent, ctxProxyBaseKey, b)
}

func BaseFromContext(ctx context.Context) *Base {
	v, _ := ctx.Value(ctxProxyBaseKey).(*Base)
	return v
}

func Rewrite(pr *httputil.ProxyRequest) {
	b := BaseFromContext(pr.In.Context())
	// b := pr.In.Context().Value(ctxProxyBaseKey).(*Base)

	pr.Out.URL.Scheme = b.Scheme
	pr.Out.URL.Host = b.Host
	pr.Out.Host = b.Host

	if b.Caller.NodeID > 0 {
		pr.Out.Header.Set(mess.CallerNodeHeader, strconv.FormatUint(b.Caller.NodeID, 10))
	}
	if b.Target.NodeID > 0 {
		pr.Out.Header.Set(mess.TargetNodeHeader, strconv.FormatUint(b.Target.NodeID, 10))
	}

	if b.Caller.Realm != "" {
		pr.Out.Header.Set(mess.CallerRealmHeader, b.Caller.Realm)
	}
	if b.Target.Realm != "" {
		pr.Out.Header.Set(mess.TargetRealmHeader, b.Target.Realm)
	}

	if b.Caller.Service != "" {
		pr.Out.Header.Set(mess.CallerServiceHeader, b.Caller.Service)
	}
	if b.Target.Service != "" {
		pr.Out.Header.Set(mess.TargetServiceHeader, b.Target.Service)
	}
}
