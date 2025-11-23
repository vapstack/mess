package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/mess/internal/proc"

	"github.com/rosedblabs/rosedb/v2"
	"github.com/vapstack/monotime"
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

	dev := isDev()

	if !dev && runtime.GOOS != "linux" {
		log.Println("fatal: only linux is supported outside of dev mode")
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
		busdir: filepath.Join(binPath, "bus"),
		dev:    dev,

		// busdb: make(map[dkey]*dbi),
		// logdb: make(map[dkey]*dbi),
	}

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
	if err = os.MkdirAll(n.busdir, 0700); err != nil {
		log.Println("fatal:", err)
		return
	}

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
		// defer func() {
		//
		// }()
	}
	// defer func() {
	//
	// }()

	// lgen := monotime.NewGen(filepath.Join(n.busdir, "log.last"))
	// if err != nil {
	// 	log.Println("fatal: error creating log sequence:", err)
	// 	return
	// }
	// n.logids = monotime.NewGen(0)

	bgen, err := newMonoBolt(int(n.id), filepath.Join(n.busdir, "last"))
	if err != nil {
		log.Println("fatal: error initializing bus sequence:", err)
		return
	}
	n.busids = bgen
	n.busTopics = new(topicTracker)

	n.ctx, _ = signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)

	/**/

	/**/

	// n.logseq = newSeqGen(n.id)

	/**/

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
	id   uint64
	path string

	svcdir string
	logdir string
	tmpdir string
	busdir string

	dev bool

	state atomic.Pointer[mess.NodeState]

	cert atomic.Pointer[tls.Certificate]
	pool *x509.CertPool
	pubk ed25519.PublicKey

	mu sync.Mutex // single-flight for API methods

	localServices  atomic.Pointer[lsMap]
	localAliases   atomic.Pointer[lsAliasMap]
	remoteServices atomic.Pointer[rsMap] // ordered by location proximity

	client *http.Client
	proxy  *httputil.ReverseProxy

	logmu sync.Mutex
	logdb sync.Map

	busmu  sync.Mutex
	busdb  sync.Map
	busids *monobolt

	busTopics *topicTracker

	ctx  context.Context
	stop func()

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
	next atomic.Uint64
	pms  []*proc.Manager
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

		n.wg.Add(1)
		go n.refreshBusTopics()
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

		if err := pm.Shutdown(); err != nil {
			if wasRunning {
				n.logf("shutting down %v@%v: %v", s.Name, s.Realm, err)
			}
		}
	}
}

func (n *node) refreshBusTopics() {
	defer n.wg.Done()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(pulseInterval):
			n.updateBusTopics()
		}
	}
}

func (n *node) updateBusTopics() {

	n.mu.Lock()
	defer n.mu.Unlock()

	d := n.stateClone()

	if d.Node.Publish == nil {
		d.Node.Publish = make(mess.PublishMap)
	}
	for _, tm := range d.Node.Publish {
		for topic, uuid := range tm {
			if time.Since(uuid.Time()) > eventTTL {
				delete(tm, topic)
			}
		}
	}

	n.busTopics.each(func(realm, topic string, uuid monotime.UUID) {
		tm, ok := d.Node.Publish[realm]
		if !ok {
			tm = make(map[string]monotime.UUID)
			d.Node.Publish[realm] = tm
		}
		tm[topic] = uuid
	})

	if err := n.saveState(d); err != nil {
		n.logf("failed to save state: %v", err)
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

func (n *node) getState() *mess.NodeState {
	state := n.stateClone()

	sm := *n.localServices.Load()

	state.Node.CertExpires = n.cert.Load().Leaf.NotAfter.Unix()

	for _, s := range state.Node.Services {
		if pm := sm.get(s); pm != nil {
			s.Active = pm.Running()
			s.Passive = pm.Passive()
		}
	}

	// n.busTopics.each(func(realm, topic string, t int64) bool {
	//
	// })

	return state
}

func (n *node) close() error {
	n.stop()

	n.stopServices()
	n.wg.Wait()

	n.busdb.Range(func(key, db any) bool {
		k := key.(dkey)
		if err := db.(*dbval).Close(); err != nil {
			n.logf("error closing bus db for topic %v@%v: %v", k.name, k.realm, err)
			// log.Printf("error closing bus db for topic %v@%v: %v", k.name, k.realm, e)
		}
		return true
	})
	if err := n.busids.close(); err != nil {
		n.logf("error closing bus sequence: %v", err)
	}

	if !n.dev {
		n.logdb.Range(func(key, db any) bool {
			k := key.(dkey)
			if e := db.(*dbval).Close(); e != nil {
				log.Printf("error closing log db for %v@%v: %v", k.name, k.realm, e)
			}
			return true
		})
	}

	return nil
}

func (n *node) updateService(s *mess.Service) error {
	// lock acquired in handler
	d := n.stateClone()
	for i, rec := range d.Node.Services {
		if rec.Name == s.Name {
			d.Node.Services[i] = s
			break
		}
	}
	return n.saveState(d)
}

func (n *node) deleteService(s *mess.Service) error {
	// lock acquired in handler
	d := n.stateClone()
	x := make(mess.Services, 0, len(d.Node.Services))
	for _, rec := range d.Node.Services {
		if rec.Name == s.Name && rec.Realm == s.Realm {
			continue
		}
		x = append(x, rec)
	}

	d.Node.Services = x

	if err := n.saveState(d); err != nil {
		return err
	}

	rm := (*n.localServices.Load()).clone()
	rm.delete(s)
	n.updateLocalServices(rm)

	if err := n.deleteLog(s.Realm, s.Name); err != nil {
		n.logf("error deleting log db for %v@%v: %v", s.Name, s.Realm, err)
	}

	return nil
}

func (n *node) stateClone() *mess.NodeState {
	return n.state.Load().Clone()
}

func (n *node) saveState(d *mess.NodeState) error {
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
		return fmt.Errorf("node state is missing, incomplete or misconfigured")
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
	// will acquire lock when it becomes available
	go n.rebuildAliasMapLocked()
}

func (n *node) rebuildAliasMapLocked() {
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
			sm, ok := am[realm]
			if !ok {
				sm = make(map[string]*aliasRoundRobin)
				am[realm] = sm
			}
			rr, ok := sm[svc.Name]
			if !ok {
				rr = new(aliasRoundRobin)
				sm[svc.Name] = rr
			}
			rr.pms = append(rr.pms, pm)
			for _, alias := range svc.Alias {
				if alias == "" {
					continue
				}
				rr, ok = sm[alias]
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

/**/

type Producer[T any] func(context.Context, chan<- T)

func isDev() bool {
	var dev bool
	for _, arg := range os.Args[1:] {
		if dev = strings.TrimPrefix(arg, "-") == "dev"; dev {
			break
		}
	}
	return dev
}

func toCronMinute(s string) string {
	var h int
	for i := 0; i < len(s); i++ {
		h += int(s[i])
	}
	return strconv.Itoa(h % 60)
}

type (
	dkey struct {
		realm string
		name  string
	}
	dbval struct {
		*rosedb.DB
		seq *monotime.Gen
	}
)

func loggedClose(f *os.File) {
	n := f.Name()
	if err := f.Close(); err != nil {
		log.Printf("error closing %v: %v\n", n, err)
	}
}

func silentClose(f *os.File) {
	_ = f.Close()
}

func loggedRemove(name string) {
	if err := os.Remove(name); err != nil {
		log.Printf("error removing %v: %v\n", name, err)
	}
}

func silentRemove(name string) {
	_ = os.Remove(name)
}
