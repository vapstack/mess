package mess

import (
	"errors"
	"slices"
	"strings"
)

const PublicPort = 2701

const NodeService = "mess"

const (
	TargetNodeHeader    = "X-Mess-Target-Node"
	TargetRealmHeader   = "X-Mess-Target-Realm"
	TargetServiceHeader = "X-Mess-Target-Service"

	CallerNodeHeader    = "X-Mess-Caller-Node"
	CallerRealmHeader   = "X-Mess-Caller-Realm"
	CallerServiceHeader = "X-Mess-Caller-Service"
)

var (
	ErrInterrupt      = errors.New("system interrupt")
	ErrInvalidNode    = errors.New("invalid node")
	ErrNoCertProvided = errors.New("no certificates provided")
)

type Node struct {
	ID          uint64   `json:"id"`
	Region      string   `json:"region"`
	Country     string   `json:"country"`
	Datacenter  string   `json:"datacenter"`
	Addr        string   `json:"addr"`
	Bind        string   `json:"bind"`
	CertExpires int64    `json:"certExpires"`
	Services    Services `json:"services"`
	LastSync    int64    `json:"lastSync,omitempty"`
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

type StreamRecord struct {
	ID   uint64
	Data []byte
}

type LogRecord struct {
	ID   uint64 `json:"id"`
	Data []byte `json:"data"`
}

/**/

type NodeState struct {
	Node *Node `json:"node,omitempty"`
	Map  Map   `json:"map"`
}

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

// NodesByProximity returns a NodeList ordered by proximity to the current node (closest to farthest).
// If NodeState does not hold a valid Node, nil is returned.
func (ns *NodeState) NodesByProximity() NodeList {
	return ns.Map.NodesByProximityTo(ns.Node)
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

const (
	findSuffix = 1
	findPrefix = 2
)

func (s Services) Has(search string) bool { return s.HasIn(search, env.Realm) }
func (s Services) HasIn(search string, realm string) bool {
	p := 0
	if strings.HasPrefix(search, "*") {
		search = strings.TrimPrefix(search, "*")
		p |= findSuffix
	}
	if strings.HasSuffix(search, "*") {
		search = strings.TrimSuffix(search, "*")
		p |= findPrefix
	}
	for _, svc := range s {
		if !svc.Passive && svc.Active && svc.Realm == realm {
			if svc.Name == search ||
				(p&findSuffix > 0 && strings.HasSuffix(svc.Name, search)) ||
				(p&findPrefix > 0 && strings.HasPrefix(svc.Name, search)) {
				return true
			}
			for _, a := range svc.Alias {
				if a == search ||
					(p&findSuffix > 0 && strings.HasSuffix(a, search)) ||
					(p&findPrefix > 0 && strings.HasPrefix(a, search)) {
					return true
				}
			}
		}
	}
	return false
}

func (s Services) Get(name string) *Service { return s.GetIn(name, env.Realm) }
func (s Services) GetIn(name string, realm string) *Service {
	for _, svc := range s {
		if svc.Name == name && svc.Realm == realm {
			return svc
		}
	}
	return nil
}

func (s Services) Find(search string) []*Service { return s.FindIn(search, env.Realm) }
func (s Services) FindIn(search string, realm string) []*Service {
	p := 0
	if strings.HasPrefix(search, "*") {
		search = strings.TrimPrefix(search, "*")
		p |= findSuffix
	}
	if strings.HasSuffix(search, "*") {
		search = strings.TrimSuffix(search, "*")
		p |= findPrefix
	}
	var services []*Service
	for _, svc := range s {
		if !svc.Passive && svc.Active && svc.Realm == realm {
			if svc.Name == search ||
				(p&findSuffix > 0 && strings.HasSuffix(svc.Name, search)) ||
				(p&findPrefix > 0 && strings.HasPrefix(svc.Name, search)) {
				services = append(services, svc)
				continue
			}
			for _, a := range svc.Alias {
				if a == search ||
					(p&findSuffix > 0 && strings.HasSuffix(a, search)) ||
					(p&findPrefix > 0 && strings.HasPrefix(a, search)) {
					services = append(services, svc)
					break
				}
			}
		}
	}
	return services
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

func (n *Node) LocationProximity(x *Node) int {
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
	sa, sb := n.LocationProximity(a), n.LocationProximity(b)
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

func (n *Node) Has(search string) bool                 { return n.Services.Has(search) }
func (n *Node) HasIn(search string, realm string) bool { return n.Services.HasIn(search, realm) }
func (n *Node) Get(service string) *Service            { return n.Services.Get(service) }
func (n *Node) GetIn(service, realm string) *Service   { return n.Services.GetIn(service, realm) }

func (n *Node) Find(search string) []*Service          { return n.Services.Find(search) }
func (n *Node) FindIn(search, realm string) []*Service { return n.Services.FindIn(search, realm) }

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

type Map map[uint64]*Node

// NodesByProximityTo returns a NodeList ordered by proximity to n, including n itself as the first element.
// If n is nil, NodesByProximityTo returns nil.
func (nm Map) NodesByProximityTo(n *Node) NodeList {
	if n == nil || len(nm) == 0 {
		return nil
	}
	nodes := make(NodeList, 0, len(nm)+1)
	nodes = append(nodes, n)
	for _, node := range nm {
		nodes = append(nodes, node)
	}
	slices.SortStableFunc(nodes, n.ProximitySort)
	return nodes
}

// Filter returns a NodeList holding nodes for which the provided fn returns true.
func (nm Map) Filter(fn func(n *Node) bool) NodeList {
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

func (nm Map) Clone() Map {
	x := make(Map)
	for k, rec := range nm {
		x[k] = rec.Clone()
	}
	return x
}
