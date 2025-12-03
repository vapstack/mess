package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/gob"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/klauspost/compress/gzhttp"
	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/mess/internal/sign"
	"github.com/vapstack/mess/internal/storage"
	"github.com/vapstack/mess/internal/tlsutil"
)

func main() {
	log.SetFlags(0)

	binName, err := os.Executable()
	if err != nil {
		log.Fatalln("fatal: failed to get binary name:", err)
	}
	binPath := filepath.Dir(binName)

	cli := &CLI{
		path:     binPath,
		messFile: filepath.Join(binPath, "mess.json"),
	}

	/**/

	cli.state = &State{
		Map: make(mess.NodeMap),
	}
	if _, err = os.Stat(cli.messFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalln("fatal:", err)
		}
		if err = cli.saveState(); err != nil {
			log.Fatalln("fatal: saving mess state:", err)
		}
	} else {
		if err = storage.ReadObject(cli.messFile, &cli.state); err != nil {
			log.Fatalln("fatal: reading mess nodes:", err)
		}
	}

	/**/

	messCrtFile := filepath.Join(binPath, "mess.crt")
	messKeyFile := filepath.Join(binPath, "mess.key")
	if _, err = os.Stat(messCrtFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalln("fatal:", err)
		}
		if err = createCA(messKeyFile, messCrtFile); err != nil {
			log.Fatalln("fatal: creating CA:", err)
		}
	}

	if _, err = os.Stat(messKeyFile); err == nil {
		cli.keyBytes, err = storage.ReadFile(messKeyFile)
		if err != nil {
			log.Fatalln("fatal:", err)
		}
		cli.rootMode = true
	}

	cli.crtBytes, err = storage.ReadFile(messCrtFile)
	if err != nil {
		log.Fatalln("fatal:", err)
	}

	if cli.rootMode {
		cli.key, cli.crt, err = parseKeyCert(cli.keyBytes, cli.crtBytes)
		if err != nil {
			log.Fatalln("fatal: parsing CA:", err)
		}
	}

	if len(os.Args) < 2 {
		printUsage()
		return
	}

	cli.ctx, _ = signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)

	cmd := &command{
		name: os.Args[1],
		args: os.Args[2:],
	}

	noArgs := map[string]struct{}{
		"sync":   {},
		"map":    {},
		"rec":    {},
		"rotate": {},
	}

	if len(os.Args) < 3 {
		if _, ok := noArgs[cmd.name]; !ok {
			printCommandUsage(os.Args[1], 0)
			return
		}
	}

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(cli.crtBytes); !ok {
		log.Println("fatal: failed to append certificate to the pool")
		return
	}

	crt, err := cli.getClientCert()
	if err != nil {
		log.Println("fatal:", err)
		return
	}

	cli.client = &http.Client{
		Transport: gzhttp.Transport(&http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify:    true,
				Certificates:          []tls.Certificate{crt},
				VerifyPeerCertificate: tlsutil.PeerCertVerifier(0, pool),
				RootCAs:               pool,
				MinVersion:            tls.VersionTLS13,
			},
		}),
	}

	if code, err := cli.runCommand(cmd); err != nil {
		log.Fatalln("error:", err)
	} else if code > 0 {
		os.Exit(code)
	}
}

/**/

type command struct {
	name string
	args []string
}

type State struct {
	RequireRealm bool         `json:"requireRealm"`
	Map          mess.NodeMap `json:"map"`
	LastID       uint64       `json:"lastNode"`
}

type CLI struct {
	path     string
	messFile string

	keyBytes []byte
	key      any
	crtBytes []byte
	crt      *x509.Certificate

	state *State

	client *http.Client
	ctx    context.Context

	rootMode bool
}

func (cli *CLI) saveState() error {
	return storage.WriteObject(cli.messFile, cli.state)
}

func (cli *CLI) loadOpsCert() ([]byte, []byte, error) {
	keyBytes, e := storage.ReadFile(filepath.Join(cli.path, "ops.key"))
	if e != nil {
		return nil, nil, fmt.Errorf("reading ops.key: %w", e)
	}
	crtBytes, e := storage.ReadFile(filepath.Join(cli.path, "ops.crt"))
	if e != nil {
		return nil, nil, fmt.Errorf("reading ops.crt: %w", e)
	}
	return keyBytes, crtBytes, nil
}

func (cli *CLI) getClientCert() (tls.Certificate, error) {

	if cli.rootMode {
		root, err := tls.X509KeyPair(cli.crtBytes, cli.keyBytes)
		if err != nil {
			return root, fmt.Errorf("failed to create x509 key pair: %w", err)
		}
		return root, nil
	}

	var crt tls.Certificate

	keyPEM, crtPEM, err := cli.loadOpsCert()
	if err != nil {
		return crt, fmt.Errorf("no mess.key found;  %w", err)
	}
	crt, err = tls.X509KeyPair(crtPEM, keyPEM)
	if err != nil {
		return crt, fmt.Errorf("failed to create x509 key pair for ops certificate: %w", err)
	}
	return crt, nil
}

func (cli *CLI) createNodeCert(nodeID uint64, days int) ([]byte, []byte, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate key: %w", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))

	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   strconv.FormatUint(nodeID, 10),
			Organization: []string{mess.ServiceName},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(0, 0, days),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, cli.crt, publicKey, cli.key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %w", err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal key: %w", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyBytes,
	})
	crtPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	return keyPEM, crtPEM, nil
}

/**/

func (cli *CLI) fetchState(addr string) error {
	state := new(mess.NodeState)
	if err := cli.call(addr, "state", nil, state); err != nil {
		return err
	}
	return cli.applyState(state, addr)
}

func (cli *CLI) applyState(state *mess.NodeState, addr string) error {

	if state.Node != nil {
		state.Node.Addr = addr
		state.Node.LastSync = time.Now().Unix()
		cli.state.Map[state.Node.ID] = state.Node
		if state.Node.ID > cli.state.LastID {
			cli.state.LastID = state.Node.ID
		}
	}

	for _, rec := range state.Map {
		if _, exist := cli.state.Map[rec.ID]; !exist {
			cli.state.Map[rec.ID] = rec
		}
		if rec.ID > cli.state.LastID {
			cli.state.LastID = rec.ID
		}
	}

	if err := cli.saveState(); err != nil {
		return fmt.Errorf("call succeeded, but saving mess failed: %w", err)
	}
	return nil
}

func (cli *CLI) eachNodeProgress(fn func(rec *mess.Node) error) int {
	ec := 0
	ids := make([]uint64, 0, len(cli.state.Map))
	for id := range cli.state.Map {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	for _, id := range ids {
		if cli.ctx.Err() != nil {
			return ec
		}
		rec := cli.state.Map[id]
		ec += nodeProgress(rec).cover(func() error { return fn(rec) })
	}
	return ec
}

func (cli *CLI) addr(node string) string {
	nid, _ := strconv.ParseUint(node, 10, 64)
	if rec, ok := cli.state.Map[nid]; ok {
		return rec.Address()
	}
	for _, rec := range cli.state.Map {
		if addr := rec.Address(); addr == node {
			return addr
		}
	}
	return node
}

func (cli *CLI) nodeProgress(node string) *pprinter {
	nid, _ := strconv.ParseUint(node, 10, 64)
	if rec, ok := cli.state.Map[nid]; ok {
		return nodeProgress(rec)
	}
	for _, rec := range cli.state.Map {
		if rec.Address() == node {
			return nodeProgress(rec)
		}
	}
	return pstartf(node)
}

/**/

func (cli *CLI) post(host, endpoint, query string, filename string) error {
	dst := fmt.Sprintf("https://%v:%v/%v%v", host, mess.PublicPort, endpoint, query)

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	req, err := http.NewRequestWithContext(cli.ctx, http.MethodPost, dst, f)
	if err != nil {
		return err
	}
	req.Header.Set(mess.CallerHeader, internal.ConstructCaller(0, "", mess.ServiceName))
	req.Header.Set(mess.TargetServiceHeader, mess.ServiceName)

	if cli.key != nil {
		req.Header.Set(sign.Header, sign.Timestamp(cli.key.(ed25519.PrivateKey)))
	}

	res, err := cli.client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer internal.DrainAndCloseBody(res)

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status: %v, cannot read body: %w", res.StatusCode, e)
		}
		if len(b) > 0 {
			return fmt.Errorf("status: %v, body: %v", res.StatusCode, string(b))
		} else {
			return fmt.Errorf("status: %v - %v", res.StatusCode, http.StatusText(res.StatusCode))
		}
	}
	return nil

}

func (cli *CLI) call(host, endpoint string, data any, result any) error {
	if host == "" {
		return errors.New("no host address")
	}

	dst := fmt.Sprintf("https://%v:%v/%v", host, mess.PublicPort, endpoint)

	buf := new(bytes.Buffer)

	if data != nil {
		if err := gob.NewEncoder(buf).Encode(data); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(cli.ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, buf)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/gob")
	req.Header.Set(mess.CallerHeader, internal.ConstructCaller(0, "", mess.ServiceName))
	req.Header.Set(mess.TargetServiceHeader, mess.ServiceName)

	if cli.key != nil {
		req.Header.Set(sign.Header, sign.Timestamp(cli.key.(ed25519.PrivateKey)))
	}

	res, err := cli.client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer internal.DrainAndCloseBody(res)

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status: %v, cannot read body: %w", res.StatusCode, e)
		}
		if len(b) > 0 {
			return fmt.Errorf("status: %v, body: %v", res.StatusCode, string(b))
		} else {
			return fmt.Errorf("status: %v - %v", res.StatusCode, http.StatusText(res.StatusCode))
		}
	}

	if result != nil {
		if err = gob.NewDecoder(res.Body).Decode(result); err != nil {
			return fmt.Errorf("reading body: %w", err)
		}
	}
	return nil
}

/**/

func printUsage(section ...string) {
	if len(section) == 0 {
		var keys []string
		maxlen := 0
		for k := range commandUsage {
			keys = append(keys, k)
			if maxlen < len(k) {
				maxlen = len(k)
			}
		}
		maxlen += 4
		slices.Sort(keys)

		fmt.Println()
		for _, key := range keys {
			printCommandUsage(key, maxlen, true)
		}
	}
}

func printCommandUsage(name string, maxlen int, skipline ...bool) {
	lines := commandUsage[name]
	if len(lines) == 0 {
		fmt.Printf("unknown command: %v\n", name)
		return
	}
	var buf strings.Builder
	if maxlen == 0 {
		maxlen = len(name) + 2 + 2
	}
	fill := []byte(strings.Repeat(" ", maxlen))
	copy(fill[2:], name)
	if len(skipline) == 0 {
		buf.WriteString("\n")
	}
	buf.WriteString(string(fill))
	buf.WriteString("- ")
	buf.WriteString(lines[0])
	buf.WriteString("\n")
	for _, line := range lines[1:] {
		buf.WriteString(strings.Repeat(" ", maxlen+2))
		buf.WriteString(line)
		buf.WriteString("\n")
	}
	fmt.Println(buf.String())
}

/**/

func createCA(messKeyFile, messCrtFile string) error {

	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return fmt.Errorf("failed to generate key: %w", err)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 122))

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   strconv.FormatUint(0, 10),
			Organization: []string{mess.ServiceName},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(100, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		SignatureAlgorithm:    x509.PureEd25519,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey, privateKey)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}

	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("failed to marshal key: %w", err)
	}

	keyFile, err := os.OpenFile(messKeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = keyFile.Close() }()

	if err = pem.Encode(keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}); err != nil {
		return fmt.Errorf("failed to encode PEM (key): %w", err)
	}

	crtFile, err := os.OpenFile(messCrtFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = crtFile.Close() }()

	if err = pem.Encode(crtFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return fmt.Errorf("failed to encode PEM (certificate): %w", err)
	}

	return nil
}

func parseKeyCert(keyBytes, certBytes []byte) (pk any, cert *x509.Certificate, err error) {
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		err = fmt.Errorf("invalid PEM (key)")
		return
	}
	pk, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		err = fmt.Errorf("private key: %w", err)
		return
	}

	block, _ = pem.Decode(certBytes)
	if block == nil {
		err = fmt.Errorf("invalid PEM (cert)")
		return
	}
	cert, err = x509.ParseCertificate(block.Bytes)

	return
}

/**/

const (
	colorReset    = "\033[0m"
	colorProgress = "\033[1;97m"
	colorError    = "\033[1;91m"
	colorWarn     = "\033[1;93m"
	colorSkip     = "\033[90m"
	cleanReturn   = "\033[F\033[2K"
)

var errSkip = errors.New("skipped")

type pprinter struct {
	lastMessage string
}

func nodeProgress(rec *mess.Node) *pprinter {
	pp := new(pprinter)
	pp.startf(fmt.Sprintf("%v - %v - %v", rec.ID, rec.Location(), rec.Address()))
	return pp
}

func pstartf(format string, args ...any) *pprinter {
	pp := new(pprinter)
	pp.startf(format, args...)
	return pp
}

func (p *pprinter) startf(format string, args ...any) {
	p.lastMessage = fmt.Sprintf(format, args...)
	fmt.Printf("%s[  >>  ] %s%s\n", colorProgress, p.lastMessage, colorReset)
}

func (p *pprinter) printf(format string, args ...any) {
	p.lastMessage = fmt.Sprintf(format, args...)
	fmt.Printf("%s%s[  >>  ] %s%s\n", cleanReturn, colorProgress, p.lastMessage, colorReset)
}

func (p *pprinter) ok(override ...string) {
	if len(override) == 0 {
		fmt.Printf("%s[  OK  ] %s\n", cleanReturn, p.lastMessage)
	} else {
		fmt.Printf("%s[  OK  ] %s\n", cleanReturn, override[0])
	}
}

func (p *pprinter) skip() {
	fmt.Printf("%s%s[ SKIP ] %s%s\n", cleanReturn, colorSkip, p.lastMessage, colorReset)
}

func (p *pprinter) fail(v any) {
	fmt.Printf("%s%s[ FAIL ] %s\n", cleanReturn, colorError, p.lastMessage)
	fmt.Printf("         %v%s\n", v, colorReset)
}

func (p *pprinter) warn(v any) {
	fmt.Printf("%s%s[ WARN ] %s\n", cleanReturn, colorWarn, p.lastMessage)
	fmt.Printf("         %v%s\n", v, colorReset)
}

func (p *pprinter) cover(fn func() error) (errcnt int) {
	defer func() {
		if v := recover(); v != nil {
			p.fail(fmt.Errorf("panic: %v", v))
			errcnt = 1
		}
	}()
	if err := fn(); err != nil {
		if errors.Is(err, errSkip) {
			p.skip()
		} else {
			p.fail(err)
			errcnt = 1
		}
	} else {
		p.ok()
	}
	return
}
