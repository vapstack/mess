package mess

import (
	"errors"
	"strings"
)

const PublicPort = 2701

// const DevPort = 4003

const MessService = "mess"

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
	ID          uint64   `json:"id"` // unique identifier
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

type NodeData struct {
	Bind string `json:"bind,omitempty"`
	Node *Node  `json:"node,omitempty"`
	Map  Map    `json:"map"`
}

func (nd *NodeData) Clone() *NodeData {
	x := &NodeData{
		Node: nd.Node.Clone(),
		Map:  nd.Map.Clone(),
	}
	return x
}

func (s *Service) Clone() *Service {
	x := new(Service)
	*x = *s
	x.Args = append(make([]string, 0, len(s.Args)), s.Args...)
	x.Env = append(make([]string, 0, len(s.Env)), s.Env...)
	x.Alias = append(make([]string, 0, len(s.Alias)), s.Alias...)
	return x
}

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

type Map map[uint64]*Node

func (nm Map) Clone() Map {
	x := make(Map)
	for k, rec := range nm {
		x[k] = rec.Clone()
	}
	return x
}
