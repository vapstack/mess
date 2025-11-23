package mess

import (
	"context"
)

type ctxKey string

// func addContextHeaders(ctx context.Context, h http.Header) {
// 	if nodeID, ok := targetNodeFromContext(ctx); ok {
// 		h.Set(TargetNodeHeader, strconv.FormatUint(nodeID, 10))
// 	}
// 	if realm, ok := targetRealmFromContext(ctx); ok {
// 		h.Set(TargetRealmHeader, realm)
// 	}
// }

/**/

const ctxTargetNode ctxKey = "messTargetNode"

func WithTargetNode(ctx context.Context, nodeID uint64) context.Context {
	return context.WithValue(ctx, ctxTargetNode, nodeID)
}

func targetNodeFromContext(ctx context.Context) (uint64, bool) {
	s, ok := ctx.Value(ctxTargetNode).(uint64)
	return s, ok
}

// func setTargetNode(r *http.Request, nodeID uint64) {
// 	r.Header.Set(TargetNodeHeader, strconv.FormatUint(nodeID, 10))
// }

/**/

const ctxTargetRealm ctxKey = "messTargetRealm"

func WithTargetRealm(ctx context.Context, realm string) context.Context {
	return context.WithValue(ctx, ctxTargetRealm, realm)
}

func targetRealmFromContext(ctx context.Context) (string, bool) {
	s, ok := ctx.Value(ctxTargetRealm).(string)
	return s, ok
}

// func setTargetRealm(r *http.Request, nodeID uint64) {
// 	r.Header.Set(TargetRealmHeader, strconv.FormatUint(nodeID, 10))
// }
