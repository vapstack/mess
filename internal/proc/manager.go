package proc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/mess/internal/proxy"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Manager struct {
	dev     bool
	node    uint64
	path    string
	datadir string
	handler http.HandlerFunc
	service atomic.Pointer[mess.Service]

	order atomic.Int64

	running atomic.Bool
	stopped atomic.Bool
	passive atomic.Bool

	listener net.Listener
	server   *http.Server
	unixsock string

	proxy atomic.Pointer[httputil.ReverseProxy]

	binLogs chan []byte
	mgrLogs chan error

	versionPath string

	mu sync.Mutex // for apis

	cmd *exec.Cmd
}

func NewManager(dev bool, nodeID uint64, svcroot string, hh http.HandlerFunc, s *mess.Service) (*Manager, error) {
	realm := s.Realm
	if realm == "" {
		realm = "default"
	}
	pm := &Manager{
		dev:  dev,
		node: nodeID,

		path:    filepath.Join(svcroot, realm, s.Name),
		datadir: filepath.Join(svcroot, realm, s.Name, "data"),

		handler: hh,

		binLogs: make(chan []byte, 32),
		mgrLogs: make(chan error, 32),
	}

	pm.order.Store(int64(s.Order))

	pm.service.Store(s)

	if err := os.MkdirAll(pm.path, 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(pm.datadir, 0700); err != nil {
		return nil, err
	}

	if err := pm.loadMeta(); err != nil && !pm.dev {
		log.Printf("no meta information found for %v: %v\n", s.Name, err)
	}

	pm.cleanup()

	if !s.Manual {
		err := pm.Start()
		if err != nil {
			pm.mgrLogs <- fmt.Errorf("failed to start %v@%v: %v", s.Name, s.Realm, err)
		}
	}

	return pm, nil
}

type metadata struct {
	Current string `json:"current"`
}

func (pm *Manager) Running() bool                 { return pm.running.Load() }
func (pm *Manager) Stopped() bool                 { return pm.stopped.Load() }
func (pm *Manager) Passive() bool                 { return pm.passive.Load() }
func (pm *Manager) Service() *mess.Service        { return pm.service.Load() }
func (pm *Manager) Proxy() *httputil.ReverseProxy { return pm.proxy.Load() }
func (pm *Manager) CurrentOrder() int             { return int(pm.order.Load()) }

func (pm *Manager) BinLogs() <-chan []byte    { return pm.binLogs }
func (pm *Manager) ManagerLogs() <-chan error { return pm.mgrLogs }

func (pm *Manager) saveMeta() error {
	md := metadata{
		Current: pm.versionPath,
	}
	return internal.WriteObject(filepath.Join(pm.path, "meta.json"), md)
}

func (pm *Manager) loadMeta() error {
	m, err := internal.LoadObject[metadata](filepath.Join(pm.path, "meta.json"))
	if err != nil {
		return err
	}
	if m.Current == "" {
		return nil
	}
	pm.versionPath = filepath.Join(pm.path, m.Current)
	return nil
}

func (pm *Manager) Start() error {
	if pm.running.Load() {
		return nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.versionPath == "" && !pm.dev {
		return fmt.Errorf("no service files found")
	}

	pm.stopped.Store(false)

	svc := pm.service.Load()

	pm.passive.Store(svc.Passive)

	var binpath string

	if !pm.dev {

		if svc.Start != "" {
			if filepath.IsAbs(svc.Start) {
				binpath = svc.Start
			} else {
				binpath = filepath.Join(pm.versionPath, svc.Start)
			}
		} else {
			binpath = filepath.Join(pm.versionPath, svc.Name)
		}

		if _, err := os.Stat(binpath); err != nil {
			return fmt.Errorf("%v not found: %w", binpath, err)
		}
	}

	outProxy, err := pm.createServiceOutgoingProxy(svc)
	if err != nil {
		return fmt.Errorf("failed to create outgoing proxy: %w", err)
	}

	var cmd *exec.Cmd

	if !pm.dev {

		rep := strings.NewReplacer(
			"{PROXY}", outProxy,
			"{DATA_DIR}", pm.datadir,
			"{SERVICE}", svc.Name,
			"{REALM}", svc.Realm,
			"{NODE}", strconv.FormatUint(pm.node, 10),
			"{MODE}", "public", // mode,
		)

		args := make([]string, 0, len(svc.Args))
		for _, arg := range svc.Args {
			args = append(args, rep.Replace(arg))
		}

		cmd = exec.Command(binpath, args...)
		cmd.Dir = filepath.Dir(binpath)

		// env := os.Environ()
		env := make([]string, 0, len(svc.Env)+4)
		for _, s := range svc.Env {
			env = append(env, rep.Replace(s))
		}
		env = append(env, fmt.Sprintf("%v=%v", mess.EnvMode, "public")) //  mode))
		env = append(env, fmt.Sprintf("%v=%v", mess.EnvNode, pm.node))
		env = append(env, fmt.Sprintf("%v=%v", mess.EnvRealm, svc.Realm))
		env = append(env, fmt.Sprintf("%v=%v", mess.EnvService, svc.Name))
		env = append(env, fmt.Sprintf("%v=%v", mess.EnvDataDir, pm.datadir))
		env = append(env, fmt.Sprintf("%v=%v", mess.EnvProxy, outProxy))

		cmd.Env = env

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return fmt.Errorf("stdout pipe: %w", err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return fmt.Errorf("stderr pipe: %w", err)
		}

		if err = cmd.Start(); err != nil {
			return fmt.Errorf("start failed: %w", err)
		}

		go pm.streamLogs(stdout)
		go pm.streamLogs(stderr)
	}

	if inProxy, err := createServiceIncomingProxy(svc, binpath); err != nil {
		pm.mgrLogs <- fmt.Errorf("failed to create incoming proxy for %v: %v", svc.Name, err)
	} else {
		pm.proxy.Store(inProxy)
	}

	pm.running.Store(true)

	pm.cmd = cmd

	if !pm.dev {
		go func(c *exec.Cmd) {
			if e := c.Wait(); e != nil {
				pm.mgrLogs <- fmt.Errorf("%v@%v exited with non-zero code: %v", svc.Name, svc.Realm, e)
			}
			pm.running.Store(false)
			go pm.autorestart(1)
		}(cmd)
	}

	return nil
}

func (pm *Manager) autorestart(backoff int) {
	s := pm.service.Load()
	if !pm.stopped.Load() && !s.Manual {
		if e := pm.Start(); e != nil {
			if backoff > 60 {
				backoff = 60
			} else {
				pm.mgrLogs <- fmt.Errorf("failed to restart %v@%v: %v", s.Name, s.Realm, e)
			}
			<-time.After(time.Duration(backoff) * time.Second)
			go pm.autorestart(backoff * 5)
		}
	}
}

func (pm *Manager) streamLogs(r io.Reader) {
	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadBytes('\n')
		clean := bytes.TrimRight(line, "\n\r")
		if len(clean) > 0 {
			pm.binLogs <- bytes.Clone(clean)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			s := pm.service.Load()
			pm.mgrLogs <- fmt.Errorf("log scan error for %v@%v: %v", s.Name, s.Realm, err)
			break
		}
	}
}

func (pm *Manager) Stop() error {
	if !pm.running.Load() {
		return nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.stopped.Store(true)

	var proc *os.Process

	if !pm.dev {
		proc = pm.cmd.Process
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			if !errors.Is(err, os.ErrProcessDone) {
				// _ = proc.Kill()
				return fmt.Errorf("signal error: %w", err)
			}
		}
	}

	pm.proxy.Store(nil)

	defer func() {
		if pm.listener != nil {
			_ = pm.listener.Close()
			if pm.unixsock != "" {
				_ = os.Remove(pm.unixsock)
			}
			pm.listener = nil
		}
		pm.server = nil
		pm.unixsock = ""
	}()

	svc := pm.service.Load()

	if pm.dev {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pm.server.Shutdown(ctx); err != nil {
			pm.mgrLogs <- fmt.Errorf("%v@%v: server shutdown error: %v", svc.Name, svc.Realm, err)
		}
		return nil
	}

	if pm.server != nil {
		go func() {
			if err := pm.server.Shutdown(context.Background()); err != nil {
				pm.mgrLogs <- fmt.Errorf("%v@%v: server shutdown error: %v", svc.Name, svc.Realm, err)
			}
		}()
	}

	timeout := svc.Timeout
	if timeout <= 0 {
		timeout = 60
	}

	done := make(chan error, 1)
	go func() {
		_, err := proc.Wait()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			pm.mgrLogs <- fmt.Errorf("%v@%v: process shutdown error: %v", svc.Name, svc.Realm, err)
		}

	case <-time.After(time.Duration(timeout) * time.Second):
		if err := proc.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			pm.mgrLogs <- fmt.Errorf("%v@%v: timeout reached, failed to kill process: %w", svc.Name, svc.Realm, err)
		} else {
			pm.mgrLogs <- fmt.Errorf("%v@%v: timeout reached, process killed", svc.Name, svc.Realm)
		}
		<-done
	}
	return nil
}

func (pm *Manager) Update(s *mess.Service) error {
	if s == nil {
		return errors.New("service is nil")
	}
	pm.service.Store(s)
	return nil
}

func (pm *Manager) Store(filename string) error {

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if s, serr := f.Stat(); serr != nil {
		return err
	} else if s.Size() < 1<<20 {
		return fmt.Errorf("file too small: %v", s.Size())
	}

	d := make([]byte, 16)
	if _, err = io.ReadFull(f, d); err != nil {
		return err
	}
	if _, err = f.Seek(0, 0); err != nil {
		return err
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	verpath := filepath.Join(pm.path, time.Now().UTC().Format("v20060102150405"))
	if err = os.MkdirAll(verpath, 0700); err != nil {
		return err
	}

	svc := pm.service.Load()

	if isGzip(d) {
		if err = writeTarGzInto(f, verpath); err != nil {
			return fmt.Errorf("untar.gz: %w", err)
		}

	} else if isZip(d) {
		s, serr := f.Stat()
		if serr != nil {
			return err
		}
		if err = writeZipInto(f, s.Size(), verpath); err != nil {
			return fmt.Errorf("unzip: %w", err)
		}

	} else {
		binPath := filepath.Join(verpath, svc.Name)
		if err = f.Close(); err != nil {
			return err
		}
		if err = os.Rename(filename, binPath); err != nil {
			return err
		}
		if err = os.Chmod(binPath, 0700); err != nil {
			pm.mgrLogs <- fmt.Errorf("failed to chmod binary file for %v: %v", svc.Name, err)
		}
	}

	current := filepath.Base(verpath)
	pm.versionPath = filepath.Join(pm.path, current)

	if err = pm.saveMeta(); err != nil {
		return fmt.Errorf("save meta: %w", err)
	}
	return nil
}

func (pm *Manager) Shutdown() error {
	if err := pm.Stop(); err != nil {
		return err
	}
	close(pm.mgrLogs)
	close(pm.binLogs)
	return nil
}

func (pm *Manager) Delete() error {
	if err := pm.Shutdown(); err != nil {
		return fmt.Errorf("shutdown error: %w", err)
	}
	if err := os.RemoveAll(pm.path); err != nil {
		return err
	}
	return nil
}

func (pm *Manager) cleanup() {
	if pm.versionPath == "" {
		return
	}
	entries, err := os.ReadDir(pm.path)
	if err != nil {
		pm.mgrLogs <- fmt.Errorf("cleanup: %w", err)
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		verdir := filepath.Join(pm.path, e.Name())
		if samePath(verdir, pm.versionPath) {
			continue
		}
		if err = os.RemoveAll(verdir); err != nil {
			pm.mgrLogs <- fmt.Errorf("cleanup: %w", err)
		}
	}
}

func (pm *Manager) createServiceOutgoingProxy(svc *mess.Service) (string, error) {

	var network, addr string
	var err error

	switch p := strings.ToLower(svc.Proxy); p {

	case "", "tcp":
		network = p
		addr = "127.0.0.1:0"

	case "unix":
		network = p
		addr, err = filepath.Abs(filepath.Join(pm.path, "proxy.sock"))
		if err != nil {
			return "", fmt.Errorf("error getting absolute path for socket: %v", err)
		}

	default:
		network, addr, err = internal.ParseNetworkAddr(svc.Proxy)
		if err != nil {
			return "", fmt.Errorf("error parsing proxy string %v: %w", svc.Proxy, err)
		}
	}
	if network == "unix" {
		if err = os.Remove(addr); err != nil && !errors.Is(err, os.ErrNotExist) {
			pm.mgrLogs <- fmt.Errorf("error deleting sock file %v: %w", addr, err)
		}
	}

	l, err := net.Listen(network, addr)
	if err != nil {
		return "", err
	}
	pm.listener = l
	if network == "unix" {
		pm.unixsock = addr
	}

	pm.server = &http.Server{
		Handler: h2c.NewHandler(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Header.Set(mess.CallerHeader, internal.ConstructCaller(pm.node, svc.Realm, svc.Name))
				// r.Header.Set(mess.CallerNodeHeader, strconv.FormatUint(pm.node, 10))
				// r.Header.Set(mess.CallerRealmHeader, svc.Realm)
				// r.Header.Set(mess.CallerServiceHeader, svc.Name)
				pm.handler(w, r)
			}),
			new(http2.Server),
		),
		ErrorLog: log.New(io.Discard, "", 0),
	}
	if err = http2.ConfigureServer(pm.server, nil); err != nil {
		return "", fmt.Errorf("http/2 server configuration error: %w", err)
	}

	go func() {
		if e := pm.server.Serve(pm.listener); e != nil && !errors.Is(e, http.ErrServerClosed) {
			pm.mgrLogs <- fmt.Errorf("%v@%v: http server error: %v", svc.Name, svc.Realm, e)
		}
	}()

	if network == "unix" {
		return addr, nil
	} else {
		return "127.0.0.1:" + strings.Split(addr, ":")[1], nil
	}
}

func createServiceIncomingProxy(svc *mess.Service, binpath string) (*httputil.ReverseProxy, error) {
	network, addr, err := internal.ParseNetworkAddr(svc.Listen)
	if err != nil {
		return nil, fmt.Errorf("invalid Listen: %w", err)
	}

	transport := &http.Transport{
		ForceAttemptHTTP2:   true,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     2 * time.Minute,
	}

	switch network {
	case "unix":
		if !filepath.IsAbs(addr) {
			if binpath == "" {
				return nil, fmt.Errorf("dev mode requires absolute path for unix sockets")
			}
			addr = filepath.Join(binpath, addr)
		}
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, "unix", addr)
		}

	case "tcp":
		host, port, e := net.SplitHostPort(addr)
		if e != nil {
			return nil, fmt.Errorf("invalid Listen: %w", e)
		}
		if host == "" || host == "localhost" {
			host = "127.0.0.1"
		}
		// host = "127.0.0.1"
		dst := net.JoinHostPort(host, port)
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, "tcp", dst)
		}
	}

	p := &httputil.ReverseProxy{
		Rewrite:       proxy.Rewrite,
		Transport:     transport,
		FlushInterval: -1,
		BufferPool:    proxy.BufferPool,
	}

	return p, nil
}

func samePath(a, b string) bool {
	ap, err1 := filepath.EvalSymlinks(a)
	bp, err2 := filepath.EvalSymlinks(b)
	if err1 != nil || err2 != nil {
		return filepath.Clean(a) == filepath.Clean(b)
	}
	return ap == bp
}
