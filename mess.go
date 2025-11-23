package mess

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/vapstack/monotime"
)

const PublicPort = 2701

const ServiceName = "mess"

const (
	TargetNodeHeader    = "X-Mess-Target-Node"
	TargetRealmHeader   = "X-Mess-Target-Realm"
	TargetServiceHeader = "X-Mess-Target-Service"

	CallerHeader = "X-Mess-Caller"

	// CallerNodeHeader    = "X-Mess-Caller-Node"
	// CallerRealmHeader   = "X-Mess-Caller-Realm"
	// CallerServiceHeader = "X-Mess-Caller-Service"
)

var (
	ErrInterrupt        = errors.New("system interrupt")
	ErrInvalidNode      = errors.New("invalid node")
	ErrNoCertProvided   = errors.New("no certificates provided")
	ErrInternalEndpoint = errors.New("internal endpoint")
)

type Node struct {
	ID          uint64     `json:"id"`
	Region      string     `json:"region"`
	Country     string     `json:"country"`
	Datacenter  string     `json:"datacenter"`
	Addr        string     `json:"addr"`
	Bind        string     `json:"bind"`
	CertExpires int64      `json:"certExpires"`
	Services    Services   `json:"services"`
	LastSync    int64      `json:"lastSync,omitempty"`
	Publish     PublishMap `json:"publish,omitempty"`
	Consume     ConsumeMap `json:"consume,omitempty"`

	Meta map[string]string `json:"meta"` // any additional fields
}

type Service struct {
	Name    string   `json:"name"`    // service name
	Alias   []string `json:"alias"`   // service aliases
	Realm   string   `json:"realm"`   // service realm (namespace)
	Manual  bool     `json:"manual"`  // no auto-start, no auto-restart
	Active  bool     `json:"active"`  // is running
	Passive bool     `json:"passive"` // no incoming connections
	Start   string   `json:"start"`   // binary to start
	Order   int      `json:"order"`   // start order
	Args    []string `json:"args"`    // command-line arguments
	Env     []string `json:"env"`     // env variables
	Listen  string   `json:"listen"`  // listens HTTP on (127.0.0.1:8080 or /path/to/my.sock)
	Proxy   string   `json:"proxy"`   // on production: proxy network ("tcp" or "unix"); on dev - port or socket file
	Timeout int      `json:"timeout"` // shutdown timeout (to wait before kill, zero means wait indefinitely)

	Meta map[string]string `json:"meta"` // any additional fields
}

type LogRecord struct {
	ID   int64           `json:"id"`
	Data json.RawMessage `json:"data"`
}

/**/

type NodeState struct {
	Node *Node   `json:"node,omitempty"`
	Map  NodeMap `json:"map"`
}

// 1. клиент сам трекает, сервер только отдаёт откуда запросили
// 2. --серввер трекает, но тогда нужно подтверждать как-то--
// 3. клиентская либа трекает

func (ns *NodeState) Clone() *NodeState {
	x := &NodeState{
		Node: ns.Node.Clone(),
		Map:  ns.Map.Clone(),
	}
	return x
}

func (ns *NodeState) GetNode(id uint64) *Node {
	if ns.Node.ID == id {
		return ns.Node
	}
	for nid, node := range ns.Map {
		if nid == id {
			return node
		}
	}
	return nil
}

// Filter returns a NodeList holding nodes for which the provided fn returns true.
// The resulting slice is ordered by proximity to the current node (closest to farthest).
func (ns *NodeState) Filter(fn func(n *Node) bool) NodeList {
	nodes := make([]*Node, 0, len(ns.Map)/2)
	if fn(ns.Node) {
		nodes = append(nodes, ns.Node)
	}
	for _, node := range ns.Map {
		if fn(node) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// NodesByProximity returns a NodeList ordered by proximity to the current node,
// including the current node itself as the first element.
// If NodeState does not hold a valid Node, nil is returned.
func (ns *NodeState) NodesByProximity() NodeList {
	if ns.Node == nil || len(ns.Map) == 0 {
		return nil
	}
	nodes := make(NodeList, 0, len(ns.Map)+1)
	nodes = append(nodes, ns.Node)
	for _, node := range ns.Map {
		nodes = append(nodes, node)
	}
	slices.SortStableFunc(nodes, ns.Node.ProximitySort)
	return nodes
}

/**/

func (s *Service) Clone() *Service {
	x := new(Service)
	*x = *s
	x.Args = slices.Clone(s.Args)   // append(make([]string, 0, len(s.Args)), s.Args...)
	x.Env = slices.Clone(s.Env)     // append(make([]string, 0, len(s.Env)), s.Env...)
	x.Alias = slices.Clone(s.Alias) // append(make([]string, 0, len(s.Alias)), s.Alias...)
	x.Meta = nil
	if len(s.Meta) > 0 {
		x.Meta = make(map[string]string, len(s.Meta))
		for k, v := range s.Meta {
			x.Meta[k] = v
		}
	}
	return x
}

/**/

type Services []*Service

func (s Services) Clone() Services {
	x := make(Services, len(s))
	for i, svc := range s {
		x[i] = svc.Clone()
	}
	return x
}

func (s Services) Has(service string) bool {
	return s.Get(service) != nil
}

func (s Services) Get(service string) *Service {
	for _, svc := range s {
		if svc.Active && !svc.Passive && svc.Realm == env.Realm {
			if svc.Name == service {
				return svc
			}
			for _, alias := range svc.Alias {
				if alias == service {
					return svc
				}
			}
		}
	}
	return nil
}

/**/

func (n *Node) Location() string {
	s := []string{
		n.Region,
		n.Country,
		n.Datacenter,
	}
	for _, v := range s {
		if v == "" {
			return "none"
		}
	}
	return strings.Join(s, ".")
}

func (n *Node) proximityTo(x *Node) int {
	switch {
	case x.Region == n.Region && x.Country == n.Country && x.Datacenter == n.Datacenter:
		return 0
	case x.Region == n.Region && x.Country == n.Country:
		return 1
	case x.Region == n.Region:
		return 2
	default:
		return 3
	}
}

func (n *Node) ProximitySort(a, b *Node) int {
	sa, sb := n.proximityTo(a), n.proximityTo(b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	if a.ID < b.ID {
		return 1 // -1
	}
	if a.ID > b.ID {
		return -1 // 1
	}
	return 0
}

func (n *Node) Has(service string) bool     { return n.Services.Has(service) }
func (n *Node) Get(service string) *Service { return n.Services.Get(service) }

func (n *Node) Address() string {
	if n.Bind != "" {
		return n.Bind
	}
	return n.Addr
}

func (n *Node) Clone() *Node {
	x := new(Node)
	*x = *n
	x.Services = n.Services.Clone()
	return x
}

/**/

type NodeList []*Node

// Filter returns a NodeList holding nodes for which the provided fn returns true.
func (nl NodeList) Filter(fn func(n *Node) bool) NodeList {
	nodes := make(NodeList, 0, len(nl)/2)
	for _, node := range nl {
		if fn(node) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

/**/

type PublishMap map[string]map[string]monotime.UUID

func (pm PublishMap) Clone() PublishMap {
	x := make(PublishMap)
	for k, v := range pm {
		x[k] = v
	}
	return x
}

func (pm PublishMap) Has(realm, topic string) bool {
	tm, ok := pm[realm]
	if !ok {
		return false
	}
	_, ok = tm[topic]
	return ok
}

type ConsumeMap map[string][]string

func (cm ConsumeMap) Clone() ConsumeMap {
	x := make(ConsumeMap)
	for k, v := range cm {
		x[k] = v
	}
	return x
}

func (cm ConsumeMap) Has(realm, topic string) bool {
	tl, ok := cm[realm]
	if !ok {
		return false
	}
	for _, t := range tl {
		if t == topic {
			return true
		}
	}
	return false
}

/**/

type NodeMap map[uint64]*Node

// List returns a NodeList holding all nodes from the map.
func (nm NodeMap) List() NodeList {
	nodes := make(NodeList, 0, len(nm))
	for _, node := range nm {
		nodes = append(nodes, node)
	}
	return nodes
}

// Filter returns a NodeList holding nodes for which the provided fn returns true.
func (nm NodeMap) Filter(fn func(n *Node) bool) NodeList {
	if fn == nil || len(nm) == 0 {
		return nil
	}
	nodes := make(NodeList, 0, len(nm)/2)
	for _, node := range nm {
		if fn(node) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (nm NodeMap) Clone() NodeMap {
	x := make(NodeMap)
	for k, rec := range nm {
		x[k] = rec.Clone()
	}
	return x
}

/**/

type (
	LogsRequest struct {
		Realm   string `json:"realm"`
		Service string `json:"service"`
		Offset  int64  `json:"offset"`
		Limit   uint64 `json:"limit"`
		Stream  bool   `json:"stream"`
	}
)
