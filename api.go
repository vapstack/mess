package mess

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
)

type SearchRequest struct {
	Service string
	Realm   string
}

func Info(ctx context.Context) (*NodeData, error) {
	return api.Info(ctx)
}
func FindNodes(ctx context.Context, search string) ([]*Node, error) {
	return api.Find(ctx, search)
}
func FindNodesIn(ctx context.Context, search, realm string) ([]*Node, error) {
	return api.FindIn(ctx, search, realm)
}
func Closest(ctx context.Context, service string) (*Service, error) {
	return api.Closest(ctx, service)
}
func ClosestIn(ctx context.Context, service, realm string) (*Service, error) {
	return api.ClosestIn(ctx, service, realm)
}

type API struct{ *http.Client }

var api = API{NewClient()}

func (a API) Info(ctx context.Context) (*NodeData, error) {
	nd := new(NodeData)
	return nd, a.send(ctx, "/info", nil, nd)
}

func (a API) Find(ctx context.Context, search string) ([]*Node, error) {
	return a.FindIn(ctx, search, env.Realm)
}

func (a API) FindIn(ctx context.Context, search, realm string) ([]*Node, error) {
	var nodes []*Node
	nd := new(NodeData)
	if err := a.send(ctx, "/info", nil, nd); err != nil {
		return nil, err
	}
	if nd.Node.Services.HasIn(search, realm) {
		nodes = append(nodes, nd.Node)
	}
	for _, n := range nd.Map {
		if n.Services.HasIn(search, realm) {
			nodes = append(nodes, n)
		}
	}
	slices.SortFunc(nodes, nd.Node.ProximitySort)
	return nodes, nil
}

func (a API) Closest(ctx context.Context, service string) (*Service, error) {
	nd := new(NodeData)
	if err := a.send(ctx, "/info", nil, nd); err != nil {
		return nil, err
	}
	if svc := nd.Node.Services.Get(service); svc != nil {
		return svc, nil
	}
	nodes := make([]*Node, 0)
	for _, n := range nd.Map {
		if n.Has(service) {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	slices.SortFunc(nodes, nd.Node.ProximitySort)
	return nodes[0].Get(service), nil
}

func (a API) ClosestIn(ctx context.Context, realm, service string) (*Service, error) {
	nd := new(NodeData)
	if err := a.send(ctx, "/info", nil, nd); err != nil {
		return nil, err
	}
	if svc := nd.Node.Services.GetIn(service, realm); svc != nil {
		return svc, nil
	}
	nodes := make([]*Node, 0)
	for _, n := range nd.Map {
		if n.HasIn(service, realm) {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	slices.SortFunc(nodes, nd.Node.ProximitySort)
	return nodes[0].Get(service), nil
}

func (a API) MustPublish(events ...*Event) {
	err := a.send(context.Background(), "/publish", events, nil)
	if err != nil {
		panic(err)
	}
}

func (a API) Publish(ctx context.Context, events ...*Event) error {
	if len(events) == 0 {
		return nil
	}
	for _, event := range events {
		if event.Topic == "" {
			return fmt.Errorf("all events must have a topic")
		}
	}
	return a.send(ctx, "/publish", events, nil)
}

// func (a API) PublishIn(ctx context.Context, events ...*Event) error {
// 	if len(events) == 0 {
// 		return nil
// 	}
// 	for _, event := range events {
// 		if event.Topic == "" {
// 			return fmt.Errorf("all events must have a topic")
// 		}
// 	}
// 	return a.send(ctx, "/publish", events, nil)
// }

func (a API) Subscribe(ctx context.Context, fn func(*Event) EventResult) {

}

var gobContentType = []string{"application/gob"}

func (a API) send(ctx context.Context, endpoint string, request, response any) error {

	buf := getbuf()
	defer putbuf(buf)

	if request != nil {
		if err := gob.NewEncoder(buf).Encode(request); err != nil {
			return fmt.Errorf("encode error: %w", err)
		}
	}

	req := newRequest(ctx, endpoint, buf)

	res, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	defer func() { _, _ = io.Copy(io.Discard, res.Body) }()

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status error: %v, cannot read body: %w", res.StatusCode, e)
		}
		return fmt.Errorf("status error: %v, body: %v", res.StatusCode, string(b))
	}

	if response != nil {
		if err = gob.NewDecoder(res.Body).Decode(response); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
	}
	return nil
}

func newRequest(ctx context.Context, method string, body *bytes.Buffer) *http.Request {
	req := defaultRequest.WithContext(ctx)

	*req.URL = url.URL{
		Scheme: "http",
		Host:   "mess",
		Path:   method,
	}
	req.Host = "mess"

	req.Header = http.Header{
		"Content-Type": gobContentType,
	}

	if nodeID, ok := targetNodeFromContext(ctx); ok {
		req.Header.Set(TargetNodeHeader, strconv.FormatUint(nodeID, 10))
	}
	if realm, ok := targetRealmFromContext(ctx); ok {
		req.Header.Set(TargetRealmHeader, realm)
	}

	if body != nil {
		req.Body = io.NopCloser(body)
		req.ContentLength = int64(body.Len())
		buf := body.Bytes()
		req.GetBody = func() (io.ReadCloser, error) {
			r := bytes.NewReader(buf)
			return io.NopCloser(r), nil
		}
	}

	return req
}

var defaultRequest = &http.Request{
	URL: &url.URL{
		Scheme:      "",
		Opaque:      "",
		User:        nil,
		Host:        "",
		Path:        "",
		RawPath:     "",
		OmitHost:    false,
		ForceQuery:  false,
		RawQuery:    "",
		Fragment:    "",
		RawFragment: "",
	},
	Method: http.MethodPost,
}
