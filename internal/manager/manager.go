package manager

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/mess/internal/proc"
	"github.com/vapstack/mess/internal/proxy"
	"github.com/vapstack/mess/internal/storage"
)

type Manager struct {
	Dev     bool
	NodeID  uint64
	RootDir string
	Handler http.HandlerFunc
	ProcLog func(realm, service string, msgs ...[]byte)
	NodeLog func(format string, args ...any)

	svcdir  string
	datadir string

	meta *metadata

	current atomic.Pointer[mess.Service]
	updated atomic.Pointer[mess.Service] // swapped on restart

	process atomic.Pointer[proc.Proc]
	stopped atomic.Bool // process stopped manually

	s2n atomic.Pointer[proxy.Proxy] // service -> node
	n2s atomic.Pointer[proxy.Proxy] // node -> service

	mu sync.Mutex // for apis
}

type metadata struct {
	Version string
}

func (m *Manager) Manage(s *mess.Service) error {

	realm := s.Realm
	if realm == "" {
		realm = "default"
	}

	if m.NodeLog == nil {
		return errors.New("NodeLog is nil")
	}
	if m.ProcLog == nil {
		return errors.New("ProcLog is nil")
	}

	m.svcdir = filepath.Join(m.RootDir, realm, s.Name)
	m.datadir = filepath.Join(m.svcdir, "data")

	if !m.Dev {
		if err := os.MkdirAll(m.datadir, 0o700); err != nil {
			return err
		}
	}

	m.current.Store(s)
	m.updated.Store(s)

	meta, err := storage.LoadObject[metadata](filepath.Join(m.svcdir, "meta.json"))
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("error reading metadata: %w", err)
		} else if !m.Dev {
			m.logf("version information is missing")
		}
		m.meta = new(metadata)
	} else {
		m.meta = meta
	}

	if !s.Manual {
		if err = m.Start(); err != nil {
			m.logf("failed to start process: %v", err)
		}
	}
	return nil
}

func (m *Manager) logf(format string, args ...any) {
	s := m.current.Load()
	m.NodeLog("%v manager: %v", internal.ServiceName(s.Name, s.Realm), fmt.Errorf(format, args...))
}

func (m *Manager) Cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.meta == nil || m.meta.Version == "" {
		return nil
	}
	entries, err := os.ReadDir(m.svcdir)
	if err != nil {
		return fmt.Errorf("error reading dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name == m.meta.Version {
			continue
		}
		if name == "data" {
			continue
		}
		if err = os.RemoveAll(filepath.Join(m.svcdir, e.Name())); err != nil {
			return fmt.Errorf("error removing %v: %w", e.Name(), err)
		}
	}
	return nil
}

func (m *Manager) Running() bool          { return m.Dev || m.process.Load().Running() }
func (m *Manager) StoppedManually() bool  { return !m.Dev && m.stopped.Load() }
func (m *Manager) Passive() bool          { return m.current.Load().Private }
func (m *Manager) Order() int             { return m.current.Load().Order }
func (m *Manager) Service() *mess.Service { return m.current.Load() }
func (m *Manager) Proxy() *proxy.Proxy    { return m.n2s.Load() }

func (m *Manager) saveMeta() error {
	return storage.WriteObject(filepath.Join(m.svcdir, "meta.json"), m.meta)
}

func (m *Manager) Start() error {

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.Dev && m.Running() {
		return nil
	}

	if m.meta.Version == "" && !m.Dev {
		return fmt.Errorf("no service files found")
	}

	svc := m.updated.Load()
	if svc.Start == "" {
		return errors.New("service has no \"start\" field")
	}
	m.stopped.Store(false)
	m.current.Store(svc)

	exeDir := filepath.Join(m.svcdir, m.meta.Version)

	/**/

	s2n, err := proxy.ServiceToNode(svc, m.NodeID, exeDir, m.Handler)
	if err != nil {
		return fmt.Errorf("failed to create S2N proxy: %w", err)
	}

	n2s, err := proxy.NodeToService(svc, exeDir)
	if err != nil {
		if e := s2n.Close(); e != nil {
			m.logf("error closing S2N proxy: %v", e)
		}
		return fmt.Errorf("failed to create N2S proxy: %w", err)
	}

	/**/

	if !m.Dev {

		var exe string
		if filepath.IsAbs(svc.Start) {
			exe = svc.Start
		} else {
			exe = filepath.Join(exeDir, svc.Start)
		}

		p := &proc.Proc{
			FileName: exe,
			Args:     svc.Args,
			Env: map[string]string{
				mess.EnvMode:    "public",
				mess.EnvNodeID:  strconv.FormatUint(m.NodeID, 10),
				mess.EnvRealm:   svc.Realm,
				mess.EnvService: svc.Name,
				mess.EnvAlias:   strings.Join(svc.Alias, ","),
				mess.EnvDataDir: m.datadir,
				mess.EnvProxy:   s2n.Addr(),
			},
			BinLog: func(msgs ...[]byte) {
				m.ProcLog(svc.Realm, svc.Name, msgs...)
			},
			RunLog: m.NodeLog,
		}

		if err = p.Start(); err != nil {

			if e := s2n.Close(); e != nil {
				m.logf("error closing S2N proxy: %v", e)
			}
			if e := n2s.Close(); e != nil {
				m.logf("error closing N2S proxy: %v", e)
			}
			return fmt.Errorf("failed to start service: %w", err)
		}

		m.process.Store(p)
	}

	m.n2s.Store(n2s)
	m.s2n.Store(s2n)

	if !m.Dev {
		go m.autoRestart(1)
	}

	return nil
}

func (m *Manager) autoRestart(backoff int) {
	<-m.process.Load().Done()
	m.restart(backoff)
}

func (m *Manager) restart(backoff int) {
	if !m.StoppedManually() && !m.current.Load().Manual {
		if err := m.Start(); err != nil {
			m.logf("failed to restart process: %v", err)
			go func() {
				<-time.After(time.Duration(backoff) * time.Second)
				m.restart(max(backoff*2, 60))
			}()
		}
	}
}

func (m *Manager) Stop() {

	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopped.Store(true)

	if err := m.n2s.Load().Close(); err != nil {
		m.logf("error closing N2S proxy: %v", err)
	}
	m.n2s.Store(nil)

	/**/

	m.process.Load().Close(m.current.Load().Timeout)

	/**/

	if err := m.s2n.Load().Close(); err != nil {
		m.logf("error closing S2N proxy: %v", err)
	}
	m.s2n.Store(nil)
}

func (m *Manager) UpdateDefinition(s *mess.Service) {
	m.updated.Store(s)
}

func (m *Manager) StorePackage(filename string) error {

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(f)

	if _, err = f.Stat(); err != nil {
		return err
	}

	d := make([]byte, 16)
	if _, err = io.ReadFull(f, d); err != nil {
		return err
	}
	if _, err = f.Seek(0, 0); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ver := time.Now().UTC().Format("v20060102150405")
	dir := filepath.Join(m.svcdir, ver)
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	name := m.current.Load().Name

	switch {

	case isGzip(d):
		if err = writeTarGzInto(f, dir); err != nil {
			return fmt.Errorf("untar.gz: %w", err)
		}

	case isZip(d):
		s, err := f.Stat()
		if err != nil {
			return err
		}
		if err = writeZipInto(f, s.Size(), dir); err != nil {
			return fmt.Errorf("unzip: %w", err)
		}

	default:
		binPath := filepath.Join(dir, name)
		if err = f.Close(); err != nil {
			return err
		}
		if err = os.Rename(filename, binPath); err != nil {
			return err
		}
		if err = os.Chmod(binPath, 0o700); err != nil {
			m.logf("chmod: %v", err)
		}
	}

	m.meta.Version = ver

	if err = m.saveMeta(); err != nil {
		return fmt.Errorf("saving meta: %w", err)
	}
	return nil
}

func (m *Manager) Delete() error {
	m.Stop()
	if err := os.RemoveAll(m.svcdir); err != nil {
		return err
	}
	return nil
}
