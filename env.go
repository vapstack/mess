package mess

import (
	"os"
	"strconv"
	"strings"

	"github.com/vapstack/mess/internal"
)

const (
	EnvMode    = "MESS_MODE"
	EnvNodeID  = "MESS_NODE_ID"
	EnvRealm   = "MESS_REALM"
	EnvService = "MESS_SERVICE"
	EnvAlias   = "MESS_ALIAS"
	EnvDataDir = "MESS_DATA_DIR"
	EnvProxy   = "MESS_PROXY"
)

type Environment struct {
	// Dev is true when EnvMode is "dev" or not set.
	Dev bool
	// Mode holds the value of the EnvMode variable.
	Mode string
	// NodeID is the unique identifier of the mess node, taken from EnvNodeID.
	NodeID uint64
	// Service holds the service name from EnvService.
	Service string
	// Alias holds service aliases from EnvAlias.
	Alias []string
	// Realm holds the realm (namespace) name from EnvRealm.
	Realm string
	// DataDir points to the persistent directory for service data, taken from EnvDataDir.
	DataDir string
	// Proxy configuration (port os socket) from EnvProxy.
	Proxy string

	// ProxyNetwork is a helper field that holds the network type for net.Listen.
	// Typically, this field should not be used directly; prefer functions such as
	// NewClient, NewTransport, NewCustomClient, or NewCustomTransport instead.
	ProxyNetwork string
	// ProxyAddr is a helper field that holds the address for net.Listen.
	// Typically, this field should not be used directly; prefer functions such as
	// NewClient, NewTransport, NewCustomClient, or NewCustomTransport instead.
	ProxyAddr string
}

// func (e Environment) NodeString() string { return strconv.FormatUint(e.NodeID, 10) }

var env *Environment

func Env() Environment { return *env }

func init() {
	mode := strings.ToLower(os.Getenv(EnvMode))
	alias := strings.Split(os.Getenv(EnvAlias), ",")
	for i, v := range alias {
		alias[i] = strings.TrimSpace(v)
	}
	env = &Environment{
		Mode:    mode,
		Dev:     mode == "" || mode == "dev",
		Realm:   os.Getenv(EnvRealm),
		Service: os.Getenv(EnvService),
		Alias:   alias,
		DataDir: os.Getenv(EnvDataDir),
		Proxy:   os.Getenv(EnvProxy),
	}

	env.NodeID, _ = strconv.ParseUint(os.Getenv(EnvNodeID), 10, 64)

	env.ProxyNetwork, env.ProxyAddr, _ = internal.ParseNetworkAddr(env.Proxy)
}
