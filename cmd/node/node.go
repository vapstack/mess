package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"mess"
	"mess/internal"
	"mess/internal/proc"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rosedblabs/rosedb/v2"
)

func main() {

	binName, err := os.Executable()
	if err != nil {
		log.Printf("fatal: failed to get binary name: %v\n", err)
		return
	}
	binPath := filepath.Dir(binName)

	if _, err = os.Stat(filepath.Join(binPath, "mess.key")); err == nil {
		log.Println("fatal: cannot start under the mess root")
		return
	}

	/**/

	started := false
	defer func() {
		if started {
			log.Println("node stopped")
		}
	}()

	/**/

	n := &node{
		// sock:   binPath + ".sock",
		path:   binPath,
		svcdir: filepath.Join(binPath, "svc"),
		logdir: filepath.Join(binPath, "log"),
		tmpdir: filepath.Join(binPath, "tmp"),
		dev:    isDev(),
	}

	if !n.dev && runtime.GOOS != "linux" {
		log.Println("fatal: only linux is supported outside of dev mode")
		return
	}

	n.ctx, _ = signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)

	/**/

	if n.dev {
		log.Println("running in dev mode")
		if err = n.loadDevState(); err != nil {
			log.Println("fatal:", err)
			return
		}

	} else {
		if err = n.loadCert(); err != nil {
			log.Println("fatal:", err)
			return
		}
		if err = n.loadState(); err != nil {
			log.Println("fatal:", err)
			return
		}
		defer func() {
			n.logs.Range(func(key, value any) bool {
				db, ok := value.(*rosedb.DB)
				if !ok {
					log.Println("error closing logs: sync.Map value is not *rosedb.DB")
				}
				if e := db.Close(); e != nil {
					log.Printf("error closing log db [%v]: %v", key, e)
				}
				return true
			})
		}()
	}

	/**/

	n.seq = newSeqGen(n.id)

	/**/

	if err = os.MkdirAll(n.tmpdir, 0700); err != nil {
		log.Println("fatal:", err)
		return
	}
	if err = os.MkdirAll(n.logdir, 0700); err != nil {
		log.Println("fatal:", err)
		return
	}
	if err = os.MkdirAll(n.svcdir, 0700); err != nil {
		log.Println("fatal:", err)
		return
	}

	/**/

	// localStop, localServerErr, err := n.setupLocalServer()
	// if err != nil {
	// 	log.Println("fatal:", err)
	// 	return
	// }
	// defer localStop()
	//
	// log.Println("local listener started")

	/**/

	defer func() {
		if started {
			log.Println("closing services...")
			if err := n.close(); err != nil {
				log.Println("node shutdown error:", err)
			}
		}
	}()

	/**/

	var publicServerErr chan error

	// if !n.dev {
	var publicStop func()
	publicStop, publicServerErr, err = n.setupPublicServer()
	if err != nil {
		log.Println("fatal:", err)
		return
	}
	defer publicStop()

	// log.Println("public listener started")
	// }

	/**/

	if !n.dev {
		n.setupProxyClient()
	}

	/**/

	log.Println("initializing services...")

	if err = n.start(); err != nil {
		log.Println("fatal:", err)
		return
	}

	log.Println("node started")
	started = true

	// if n.dev {
	// 	if err = n.addDevServices(); err != nil {
	// 		log.Println("fatal:", err)
	// 		return
	// 	}
	// }

	select {
	case <-n.ctx.Done():
		log.Println(mess.ErrInterrupt)

	// case e := <-localServerErr:
	// 	if e != nil {
	// 		log.Println("fatal: local server error:", e)
	// 	}

	case e := <-publicServerErr:
		if e != nil {
			log.Println("fatal: public server error:", e)
		}
	}
}

/**/

type node struct {
	id     uint64
	path   string
	svcdir string
	logdir string
	tmpdir string
	dev    bool
	seq    *seqgen

	state atomic.Pointer[mess.NodeState]
	cert  atomic.Pointer[tls.Certificate]
	pool  *x509.CertPool

	mu sync.Mutex // single-flight for API methods

	localServices  atomic.Pointer[lsMap]
	localAliases   atomic.Pointer[lsAliasMap]
	remoteServices atomic.Pointer[rsMap] // ordered by location proximity

	rrCounter atomic.Uint64

	client *http.Client
	proxy  *httputil.ReverseProxy

	logs sync.Map // map["realm_service"]*rosedb.DB

	ctx  context.Context
	stop func()

	// devmap map[string]*httputil.ReverseProxy

	wg sync.WaitGroup
}

/**/

type lsMap map[string]map[string]*proc.Manager

func (m lsMap) get(s *mess.Service) *proc.Manager {
	return m.getByRealmAndName(s.Realm, s.Name)
}

func (m lsMap) getByRealmAndName(realm, name string) *proc.Manager {
	if sm, ok := m[realm]; ok {
		if pm, ok := sm[name]; ok {
			return pm
		}
	}
	return nil
}

func (m lsMap) set(s *mess.Service, pm *proc.Manager) {
	sm, ok := m[s.Realm]
	if !ok {
		sm = make(map[string]*proc.Manager)
		m[s.Realm] = sm
	}
	sm[s.Name] = pm
}

func (m lsMap) delete(s *mess.Service) {
	if sm, ok := m[s.Realm]; ok {
		delete(sm, s.Name)
	}
}

func (m lsMap) clone() lsMap {
	x := make(lsMap)
	for k, v := range m {
		sm := make(map[string]*proc.Manager)
		for key, val := range v {
			sm[key] = val
		}
		x[k] = sm
	}
	return x
}

/**/

type rsMap map[string]map[string][]*mess.Node // ordered by location proximity

func (m rsMap) getByRealmAndName(realm, name string) []*mess.Node {
	if sm, ok := m[realm]; ok {
		if nodes, ok := sm[name]; ok {
			return nodes
		}
	}
	return nil
}

func (m rsMap) add(realm, service string, node *mess.Node) {
	sm, ok := m[realm]
	if !ok {
		sm = make(map[string][]*mess.Node)
		m[realm] = sm
	}
	// sm[service] = append(sm[service], node)

	nodes, ok := sm[service]
	if !ok {
		sm[service] = []*mess.Node{node}
		return
	}
	for _, existing := range nodes {
		if existing.ID == node.ID {
			return
		}
	}
	sm[service] = append(nodes, node)
}

/**/

type aliasRoundRobin struct {
	rr  atomic.Uint64
	pms []*proc.Manager
}

type lsAliasMap map[string]map[string]*aliasRoundRobin

func (m lsAliasMap) getByRealmAndName(realm, name string) *aliasRoundRobin {
	if sm, ok := m[realm]; ok {
		if pm, ok := sm[name]; ok {
			return pm
		}
	}
	return nil
}

/**/

func (n *node) start() error {
	n.ctx, n.stop = context.WithCancel(n.ctx)

	if !n.dev {
		n.wg.Add(1)
		go n.pulsing()
	}

	n.wg.Add(1)
	go n.recalcRemoteServices()

	n.runServices()

	return nil
}

func (n *node) runServices() {

	n.localServices.Store(&lsMap{})

	services := append(make(mess.Services, 0), n.state.Load().Node.Services...)
	slices.SortStableFunc(services, func(a, b *mess.Service) int {
		if a.Order > b.Order {
			return 1
		} else if a.Order < b.Order {
			return -1
		}
		return 0
		// return strings.Compare(a.Name, b.Name)
	})

	// rm := make(lsMap)

	for _, s := range services {
		select {
		case <-n.ctx.Done():
			continue
		default:
		}
		// if rm.get(s) != nil {
		// 	n.messlogf("duplicate service declaration: %v", s.Name)
		// 	continue
		// }

		if err := n.runService(s); err != nil {
			n.logf("failed to initialize process manager for %v@%v: %v", s.Name, s.Realm, err)
			continue
		}
		// pm, err := proc.NewManager(n.dev, n.svcdir, s) // newPM(n, s)
		// if err != nil {
		// 	n.messlogf("failed to initialize process manager for %v: %v", s.Name, err)
		// 	continue
		// }
		// rm.set(s, pm)
	}

	// n.updateLocalServices(rm)
	// n.localServices.Store(&rm)
}

func (n *node) runService(s *mess.Service) error {

	log.Printf("starting process manager for %v@%v...\n", s.Name, s.Realm)

	rm := *n.localServices.Load()
	if rm.get(s) != nil {
		return fmt.Errorf("service %v@%v is already running", s.Name, s.Realm)
	}

	xm := rm.clone()
	pm, err := proc.NewManager(n.dev, n.id, n.svcdir, n.localHandler, s)
	if err != nil {
		return err
	}
	xm.set(s, pm)

	n.startLogWriters(pm)

	n.updateLocalServices(xm)
	return nil
}

func (n *node) startLogWriters(pm *proc.Manager) {

	go func() {
		logs := make([][]byte, 0, 256)
		svc := pm.Service()
		ch := pm.BinLogs()

		tick := time.NewTicker(time.Second)
		defer tick.Stop()

		for {
			select {
			case l, ok := <-ch:
				if !ok {
					if len(logs) > 0 {
						n.writeLogs(svc.Realm, svc.Name, logs...)
					}
					return
				}
				logs = append(logs, l)
			DRAIN:
				for len(logs) < 1000 {
					select {
					case l, ok = <-ch:
						if !ok {
							break DRAIN
						}
						logs = append(logs, l)
					default:
						break DRAIN
					}
				}
				n.writeLogs(svc.Realm, svc.Name, logs...)
				logs = logs[:0]

			case <-tick.C:
			}
		}
	}()

	go func() {
		for e := range pm.ManagerLogs() {
			n.logErr(e)
		}
	}()
}

func (n *node) stopServices() {

	pms := make([]*proc.Manager, 0)
	for _, sm := range *n.localServices.Load() {
		for _, pm := range sm {
			pms = append(pms, pm)
		}
	}

	slices.SortFunc(pms, func(a, b *proc.Manager) int {
		aOrder, bOrder := a.CurrentOrder(), b.CurrentOrder()
		if aOrder > bOrder {
			return -1
		} else if aOrder < bOrder {
			return 1
		}
		return 0
	})

	for _, pm := range pms {
		wasRunning := !pm.Running()

		s := pm.Service()
		log.Printf("closing process manager for %v@%v...\n", s.Name, s.Realm)

		if err := pm.Shutdown(0); err != nil {
			if wasRunning {
				n.logf("shutting down %v: %v", pm.Service().Name, err)
			}
		}
	}
}

func (n *node) recalcRemoteServices() {
	defer n.wg.Done()

	for {
		d := n.state.Load()

		rm := make(rsMap)
		for _, nd := range d.Map {
			if time.Since(time.Unix(nd.LastSync, 0)) > 2*time.Minute {
				continue
			}
			for _, svc := range nd.Services {
				if svc.Active && !svc.Passive {
					rm.add(svc.Realm, svc.Name, nd)

					for _, alias := range svc.Alias {
						if alias != "" {
							rm.add(svc.Realm, alias, nd)
						}
					}
				}
			}
		}

		for _, sm := range rm {
			for _, recs := range sm {
				slices.SortStableFunc(recs, d.Node.ProximitySort)
			}
		}

		n.remoteServices.Store(&rm)

		select {
		case <-n.ctx.Done():
			return
		case <-time.After(17 * time.Second):
		}
	}
}

func (n *node) getStatefullData() *mess.NodeState {
	state := n.stateClone()

	sm := *n.localServices.Load()

	state.Node.CertExpires = n.cert.Load().Leaf.NotAfter.Unix()

	for _, s := range state.Node.Services {
		if pm := sm.get(s); pm != nil {
			s.Active = pm.Running()
			s.Passive = pm.Passive()
		}
	}

	return state
}

func (n *node) close() error {
	n.stop()
	n.stopServices()
	n.wg.Wait()
	return nil
}

func (n *node) upgrade(bindata []byte) error {
	if len(bindata) < 1<<20 {
		return fmt.Errorf("file too small: %v", len(bindata))
	}

	binName, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable name: %w", err)
	}

	tmpName := binName + ".temp"
	f, err := os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0700)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	l, err := f.Write(bindata)
	if err != nil {
		return fmt.Errorf("write error: %w", err)
	} else if l != len(bindata) {
		return fmt.Errorf("bytes written (%v) != bytes sent (%v)", l, len(bindata))
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	if err = os.Rename(tmpName, binName); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

func (n *node) updateService(s *mess.Service) error {
	d := n.stateClone()
	for i, rec := range d.Node.Services {
		if rec.Name == s.Name {
			d.Node.Services[i] = s
			break
		}
	}
	return n.storeState(d)
}

func (n *node) deleteService(s *mess.Service) error {
	d := n.stateClone()
	x := make(mess.Services, 0, len(d.Node.Services))
	for _, rec := range d.Node.Services {
		if rec.Name == s.Name && rec.Realm == s.Realm {
			continue
		}
		x = append(x, rec)
	}

	d.Node.Services = x

	if err := n.storeState(d); err != nil {
		return err
	}

	rm := (*n.localServices.Load()).clone()
	rm.delete(s)
	n.updateLocalServices(rm)

	go func() {
		<-time.After(time.Second)
		v, ok := n.logs.Load(fmt.Sprintf("%v:%v", s.Realm, s.Name))
		if !ok {
			return
		}
		rose, ok := v.(*rosedb.DB)
		if !ok {
			n.logf("error deleting logs: sync.Map value is not *rosedb.DB")
			return
		}
		realm := s.Realm
		if realm == "" {
			realm = "default"
		}
		filename := filepath.Join(n.logdir, realm, s.Name)
		if e := rose.Close(); e != nil {
			n.logf("error closing logs db %v: %v", filename, e)
		}
		if e := os.RemoveAll(filename); e != nil {
			n.logf("error deleting logs db %v: %v", filename, e)
		}
	}()
	return nil
}

func (n *node) stateClone() *mess.NodeState {
	return n.state.Load().Clone()
}

func (n *node) storeState(d *mess.NodeState) error {
	if err := internal.WriteObject("node.json", d); err != nil {
		return err
	}
	n.state.Store(d)
	return nil
}

func (n *node) loadDevState() error {
	services, err := internal.LoadObject[mess.Services]("node.dev.json")
	if err != nil {
		return fmt.Errorf("reading node.json: %w", err)
	}
	state := &mess.NodeState{
		Node: &mess.Node{
			ID:         1,
			Region:     "dev",
			Country:    "dev",
			Datacenter: "dev",
			Services:   *services,
		},
		Map: nil,
	}
	n.id = state.Node.ID
	n.state.Store(state)
	return nil
}

func (n *node) loadState() error {
	state, err := internal.LoadObject[mess.NodeState]("node.json")
	if err != nil {
		return fmt.Errorf("reading node.json: %w", err)
	}
	if state.Node == nil || state.Node.ID == 0 {
		return fmt.Errorf("node state is missing or incomplete")
	}
	n.id = state.Node.ID
	n.state.Store(state)
	return nil
}

func (n *node) updateLocalServices(m lsMap) {
	n.localServices.Store(&m)
	n.rebuildAliasMap()
}

func (n *node) rebuildAliasMap() {
	n.mu.Lock()
	defer n.mu.Unlock()

	mptr := n.localServices.Load()
	if mptr == nil {
		return
	}

	am := make(lsAliasMap)
	for realm, services := range *mptr {
		for _, pm := range services {
			if !pm.Running() || pm.Stopped() {
				continue
			}
			svc := pm.Service()
			if svc == nil || svc.Passive {
				continue
			}
			for _, alias := range svc.Alias {
				if alias == "" {
					continue
				}
				sm, ok := am[realm]
				if !ok {
					sm = make(map[string]*aliasRoundRobin)
					am[realm] = sm
				}
				rr, ok := sm[alias]
				if !ok {
					rr = new(aliasRoundRobin)
					sm[alias] = rr
				}
				rr.pms = append(rr.pms, pm)
			}
		}
	}
	n.localAliases.Store(&am)
}

func isDev() bool {
	var dev bool
	for _, arg := range os.Args[1:] {
		if dev = strings.TrimPrefix(arg, "-") == "dev"; dev {
			break
		}
	}
	return dev
}
