package mess

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/vapstack/monotime"
)

const PublicPort = 2701

const ServiceName = "mess"

const MaxNodeID = ^uint16(0)

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
	ID          uint64     `json:"id"`          // node id in the mess
	Region      string     `json:"region"`      // node location: region
	Country     string     `json:"country"`     // node location: country
	Datacenter  string     `json:"datacenter"`  // node location: datacenter
	Addr        string     `json:"addr"`        // node address
	Bind        string     `json:"bind"`        // bind interface (if set)
	CertExpires int64      `json:"certExpires"` // certificate expiration date (unixtime)
	Services    Services   `json:"services"`    // registered services
	LastSync    int64      `json:"lastSync"`    // last sync time (unixtime)
	Publish     PublishMap `json:"publish"`     // topics published by services running on this node
	Listen      ListenMap  `json:"listen"`      // topics that services running on this node listen to

	Meta map[string]string `json:"meta"` // any additional fields
}

type Service struct {
	Name    string   `json:"name"`    // service name
	Alias   []string `json:"alias"`   // service aliases
	Realm   string   `json:"realm"`   // service realm (namespace)
	Manual  bool     `json:"manual"`  // no auto-start, no auto-restart
	Active  bool     `json:"active"`  // is running
	Private bool     `json:"private"` // no incoming routing, no resolve
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

// NodeState holds information about the node and the cluster.
type NodeState struct {
	// Node is the current state of the requested node.
	Node *Node `json:"node,omitempty"`
	// Map is a map of all known nodes in the cluster.
	Map NodeMap `json:"map"`
}

func (ns *NodeState) Clone() *NodeState {
	x := &NodeState{
		Node: ns.Node.Clone(),
		Map:  ns.Map.Clone(),
	}
	return x
}

func (ns *NodeState) GetNode(id uint64) *Node {
	if ns.Node != nil && ns.Node.ID == id {
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
func (ns *NodeState) Filter(fn func(n *Node) bool) NodeList {
	nodes := make([]*Node, 0, len(ns.Map)/2)
	if ns.Node != nil && fn(ns.Node) {
		nodes = append(nodes, ns.Node)
	}
	for _, node := range ns.Map {
		if fn(node) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// Peers returns a list of nodes with services matching the specified service name.
// The current service (the one from the environment) is excluded.
// Only services in the current realm are considered.
// Services are returned regardless of their current availability or runtime state.
func (ns *NodeState) Peers(service string) NodeList {
	var nodes []*Node

	if ns.Node != nil {
		var n *Node
		for _, svc := range ns.Node.Services {
			if svc.Private {
				continue
			}
			if svc.Realm != env.Realm {
				continue
			}
			if svc.Name == env.Service {
				continue
			}
			if svc.HasAlias(service) {
				if n == nil {
					n = ns.Node.Clone()
					n.Services = make(Services, 0)
					nodes = append(nodes, n)
				}
				n.Services = append(n.Services, svc)
			}
		}
	}

	for _, node := range ns.Map {
		var n *Node
		for _, svc := range node.Services {
			if svc.Private {
				continue
			}
			if svc.Realm != env.Realm {
				continue
			}
			if svc.Name == service || svc.HasAlias(service) {
				if n == nil {
					n = node.Clone()
					n.Services = make(Services, 0)
					nodes = append(nodes, n)
				}
				n.Services = append(n.Services, svc)
			}
		}
	}
	return nodes
}

// Resolve returns a list of node with services matching the specified service name
// and available at the time of the call.
// Only services in the current realm are considered.
func (ns *NodeState) Resolve(service string) NodeList {
	var nodes []*Node

	if ns.Node != nil {
		var n *Node
		for _, svc := range ns.Node.Services {
			if svc.Private || !svc.Active {
				continue
			}
			if svc.Realm != env.Realm {
				continue
			}
			if svc.Name == service || svc.HasAlias(service) {
				if n == nil {
					n = ns.Node.Clone()
					n.Services = make(Services, 0)
					nodes = append(nodes, n)
				}
				n.Services = append(n.Services, svc)
			}
		}
	}

	for _, node := range ns.Map {
		var n *Node
		for _, svc := range node.Services {
			if svc.Private || !svc.Active {
				continue
			}
			if svc.Realm != env.Realm {
				continue
			}
			if svc.Name == service || svc.HasAlias(service) {
				if n == nil {
					n = node.Clone()
					n.Services = make(Services, 0)
					nodes = append(nodes, n)
				}
				n.Services = append(n.Services, svc)
			}
		}
	}
	return nodes
}

func (ns *NodeState) collect(service string, activeOnly bool, localNameMatch bool) NodeList {
	var nodes []*Node

	addNode := func(node *Node, excludeName string, allowNameMatch bool) {
		if node == nil {
			return
		}

		var out *Node
		outNode := func() *Node {
			if out == nil {
				out = node.Clone()
				out.Services = make(Services, 0)
				nodes = append(nodes, out)
			}
			return out
		}

		for _, svc := range node.Services {
			if svc.Private {
				continue
			}
			if svc.Realm != env.Realm {
				continue
			}
			if activeOnly && !svc.Active {
				continue
			}
			if excludeName != "" && svc.Name == excludeName {
				continue
			}

			if allowNameMatch && svc.Name == service {
				o := outNode()
				o.Services = append(o.Services, svc)
				continue
			}

			for _, alias := range svc.Alias {
				if alias == service {
					o := outNode()
					o.Services = append(o.Services, svc)
					break
				}
			}
		}
	}

	// Local node: exclude env.Service. Name match on local is controlled by localNameMatch.
	addNode(ns.Node, env.Service, localNameMatch)

	// Remote nodes: no exclusion, always allow name match.
	for _, node := range ns.Map {
		addNode(node, "", true)
	}

	return nodes
}

// NodesByProximity returns a NodeList ordered by proximity to the current node,
// including the current node itself as the first element.
// If NodeState does not hold a valid Node, nil is returned.
func (ns *NodeState) NodesByProximity() NodeList {
	if ns.Node == nil {
		return nil
	}
	nodes := make(NodeList, 0, len(ns.Map)+1)
	if ns.Node != nil {
		nodes = append(nodes, ns.Node)
	}
	for _, node := range ns.Map {
		nodes = append(nodes, node)
	}
	slices.SortStableFunc(nodes, ns.Node.ProximitySort)
	return nodes
}

/**/

func (s *Service) HasAlias(alias string) bool {
	for _, a := range s.Alias {
		if a == alias {
			return true
		}
	}
	return false
}

func (s *Service) Clone() *Service {
	x := new(Service)
	*x = *s
	x.Args = slices.Clone(s.Args)
	x.Env = slices.Clone(s.Env)
	x.Alias = slices.Clone(s.Alias)
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

/*
func (s Services) Has(service string) bool {
	return s.Get(service) != nil
}

func (s Services) Get(service string) *Service {
	for _, svc := range s {
		if svc.Active && !svc.Private && svc.Realm == env.Realm {
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
*/

func (s Services) Filter(fn func(*Service) bool) Services {
	result := make(Services, 0, len(s))
	for _, svc := range s {
		if fn(svc) {
			result = append(result, svc)
		}
	}
	return result
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

/*
func (n *Node) Has(service string) bool     { return n.Services.Has(service) }
func (n *Node) Get(service string) *Service { return n.Services.Get(service) }
*/

func (n *Node) Address() string {
	if n.Bind != "" {
		return n.Bind
	}
	return n.Addr
}

func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	x := new(Node)
	*x = *n
	x.Services = n.Services.Clone()
	x.Publish = n.Publish.Clone()
	x.Listen = n.Listen.Clone()
	if n.Meta != nil {
		x.Meta = make(map[string]string)
		for k, v := range n.Meta {
			x.Meta[k] = v
		}
	}
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

// Clone returns a deep copy of the NodeList.
func (nl NodeList) Clone() NodeList {
	nodes := make(NodeList, len(nl))
	for i, node := range nl {
		nodes[i] = node.Clone()
	}
	return nodes
}

/**/

type PublishMap map[string]map[string]monotime.UUID

func (pm PublishMap) Clone() PublishMap {
	x := make(PublishMap)
	for k, v := range pm {
		x[k] = maps.Clone(v)
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

type ListenMap map[string][]string

func (cm ListenMap) Clone() ListenMap {
	x := make(ListenMap)
	for k, v := range cm {
		x[k] = slices.Clone(v)
	}
	return x
}

func (cm ListenMap) Has(realm, topic string) bool {
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

// Get returns a Node by ID or nil if not found.
func (nm NodeMap) Get(id uint64) *Node {
	if len(nm) == 0 {
		return nil
	}
	for _, node := range nm {
		if node.ID == id {
			return node
		}
	}
	return nil
}

func (nm NodeMap) Clone() NodeMap {
	if nm == nil {
		return nil
	}
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
