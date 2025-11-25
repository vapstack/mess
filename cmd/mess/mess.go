package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
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

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
)

func main() {
	log.SetFlags(0)

	binName, err := os.Executable()
	if err != nil {
		log.Fatalln("fatal: failed to get binary name:", err)
	}
	binPath := filepath.Dir(binName)

	m := &messConfig{
		messFile: filepath.Join(binPath, "mess.json"),
	}

	/**/

	m.state = &messState{
		Map: make(mess.NodeMap),
	}
	if _, err = os.Stat(m.messFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalln("fatal:", err)
		}
		if err = m.saveMess(); err != nil {
			log.Fatalln("fatal: saving mess state:", err)
		}
	} else {
		if err = internal.ReadObject(m.messFile, &m.state); err != nil {
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
		m.keyBytes, err = internal.ReadFile(messKeyFile)
		if err != nil {
			log.Fatalln("fatal:", err)
		}
		m.rootMode = true
	}
	m.crtBytes, err = internal.ReadFile(messCrtFile)
	if err != nil {
		log.Fatalln("fatal:", err)
	}

	if m.rootMode {
		m.key, m.crt, err = parseKeyCert(m.keyBytes, m.crtBytes)
		if err != nil {
			log.Fatalln("fatal: parsing CA:", err)
		}
	}

	if len(os.Args) < 2 {
		printUsage()
		return
	}

	cmd := parseCommand(os.Args[1:])

	if len(os.Args) < 3 && cmd.name != "sync" && cmd.name != "map" && cmd.name != "rec" {
		printCommandUsage(os.Args[1], 0)
		return
	}
	cmd.mess = m

	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(m.crtBytes); !ok {
		log.Println("fatal: failed to append certificate to the pool")
		return
	}

	crt, err := cmd.mess.getClientCert()
	if err != nil {
		log.Println("fatal:", err)
		return
	}

	tlsVerifyOptions := x509.VerifyOptions{
		Roots: pool,
	}
	cmd.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{crt},
				VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
					if len(raw) == 0 || len(raw[0]) == 0 {
						return mess.ErrNoCertProvided
					}
					cert, e := x509.ParseCertificate(raw[0])
					if e != nil {
						return e
					}
					nid, e := strconv.ParseUint(cert.Subject.CommonName, 10, 64)
					if e != nil || nid == 0 {
						return mess.ErrInvalidNode
					}
					if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] != mess.ServiceName {
						return fmt.Errorf("invalid")
					}
					_, e = cert.Verify(tlsVerifyOptions)
					return e
				},
				RootCAs:    pool,
				MinVersion: tls.VersionTLS13,
			},
		},
	}

	if code, err := runCommand(cmd); err != nil {
		log.Fatalln("error:", err)
	} else if code > 0 {
		os.Exit(code)
	}
}

/**/

type command struct {
	name   string
	args   []string
	mess   *messConfig
	client *http.Client
	ctx    context.Context
}

type messState struct {
	RequireRealm bool         `json:"requireRealm"`
	Map          mess.NodeMap `json:"map"`
	LastID       uint64       `json:"lastNode"`
}

type messConfig struct {
	messFile string

	keyBytes []byte
	key      any
	crtBytes []byte
	crt      *x509.Certificate

	state *messState

	rootMode bool
}

func (mc *messConfig) saveMess() error {
	return internal.WriteObject(mc.messFile, mc.state)
}

func (mc *messConfig) loadNodeCert() ([]byte, []byte, error) {
	keyBytes, e := internal.ReadFile("node.key")
	if e != nil {
		return nil, nil, fmt.Errorf("reading node.key: %w", e)
	}
	crtBytes, e := internal.ReadFile("node.crt")
	if e != nil {
		return nil, nil, fmt.Errorf("reading node.crt: %w", e)
	}
	return keyBytes, crtBytes, nil
}

func (mc *messConfig) getClientCert() (tls.Certificate, error) {
	if mc.rootMode {
		root, err := tls.X509KeyPair(mc.crtBytes, mc.keyBytes)
		if err != nil {
			return root, fmt.Errorf("failed to create x509 key pair: %w", err)
		}
		return root, nil
	}

	var crt tls.Certificate

	keyPEM, crtPEM, err := mc.loadNodeCert()
	if err != nil {
		return crt, fmt.Errorf("no mess.key found; reading node.key: %w", err)
	}
	crt, err = tls.X509KeyPair(keyPEM, crtPEM)
	if err != nil {
		return crt, fmt.Errorf("failed to create x509 key pair: %w", err)
	}
	return crt, nil
}

func (mc *messConfig) createNodeCert(nodeID uint64, days int) ([]byte, []byte, error) {
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

	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, mc.crt, publicKey, mc.key)
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

func (cmd *command) fetchState(addr string) error {
	state := new(mess.NodeState)
	if err := cmd.call(addr, "state", nil, state); err != nil {
		return err
	}
	return cmd.applyState(state, addr)
}

func (cmd *command) applyState(state *mess.NodeState, addr string) error {
	if state.Node != nil {
		state.Node.Addr = addr
		state.Node.LastSync = time.Now().Unix()
		cmd.mess.state.Map[state.Node.ID] = state.Node
		if state.Node.ID > cmd.mess.state.LastID {
			cmd.mess.state.LastID = state.Node.ID
		}
	}

	for _, rec := range state.Map {
		if _, exist := cmd.mess.state.Map[rec.ID]; !exist {
			cmd.mess.state.Map[rec.ID] = rec
		}
		if rec.ID > cmd.mess.state.LastID {
			cmd.mess.state.LastID = rec.ID
		}
	}

	if err := cmd.mess.saveMess(); err != nil {
		return fmt.Errorf("call succeeded, but saving mess failed: %w", err)
	}
	return nil
}

func (cmd *command) eachNodeProgress(fn func(rec *mess.Node) error) int {
	ec := 0
	for _, rec := range cmd.mess.state.Map {
		ec += nodeProgress(rec).cover(func() error { return fn(rec) })
	}
	return ec
}

func (cmd *command) addr(node string) string {
	nid, _ := strconv.ParseUint(node, 10, 64)
	if rec, ok := cmd.mess.state.Map[nid]; ok {
		return rec.Address()
	}
	for _, rec := range cmd.mess.state.Map {
		if addr := rec.Address(); addr == node {
			return addr
		}
	}
	return node
}

func (cmd *command) nodeProgress(node string) *pprinter {
	nid, _ := strconv.ParseUint(node, 10, 64)
	if rec, ok := cmd.mess.state.Map[nid]; ok {
		return nodeProgress(rec)
	}
	for _, rec := range cmd.mess.state.Map {
		if rec.Address() == node {
			return nodeProgress(rec)
		}
	}
	return pstartf(node)
}

/**/

func (cmd *command) post(host, endpoint, query string, filename string) error {
	dst := fmt.Sprintf("https://%v:%v/%v%v", host, mess.PublicPort, endpoint, query)

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	req, err := http.NewRequestWithContext(cmd.ctx, http.MethodPost, dst, f)
	if err != nil {
		return err
	}
	req.Header.Set(mess.TargetServiceHeader, mess.ServiceName)

	res, err := cmd.client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer internal.DrainAndCloseBody(res)
	// defer func() { _ = res.Body.Close() }()
	// defer func() { _, _ = io.Copy(io.Discard, res.Body) }()

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status: %v, cannot read body: %w", res.StatusCode, e)
		}
		return fmt.Errorf("status: %v, body: %v", res.StatusCode, string(b))
	}
	return nil

}

func (cmd *command) call(host, endpoint string, data any, result any) error {
	return cmd.calltype("gob", host, endpoint, data, result)
}

func (cmd *command) calltype(ctype, host, endpoint string, data any, result any) error {
	if host == "" {
		return errors.New("no host address")
	}
	dst := fmt.Sprintf("https://%v:%v/%v", host, mess.PublicPort, endpoint)
	buf := new(bytes.Buffer)
	if data != nil {
		if ctype == "gob" {
			if err := gob.NewEncoder(buf).Encode(data); err != nil {
				return err
			}
		} else {
			if err := json.NewEncoder(buf).Encode(data); err != nil {
				return err
			}
		}
	}

	req, err := http.NewRequestWithContext(cmd.ctx, http.MethodPost, dst, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/"+ctype)
	req.Header.Set(mess.CallerHeader, internal.ConstructCaller(0, "", mess.ServiceName))
	req.Header.Set(mess.TargetServiceHeader, mess.ServiceName)
	req.Header.Set(internal.SigHeader, signatureHeader(cmd.mess.key.(ed25519.PrivateKey)))

	res, err := cmd.client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer internal.DrainAndCloseBody(res)
	// defer func() { _ = res.Body.Close() }()
	// defer func() { _, _ = io.Copy(io.Discard, res.Body) }()
	// defer func(rsp *http.Response) { _ = rsp.Body.Close() }(res)
	// defer func(rsp *http.Response) { _, _ = io.Copy(io.Discard, rsp.Body) }(res)

	if res.StatusCode >= 300 {
		b, e := io.ReadAll(res.Body)
		if e != nil {
			return fmt.Errorf("status: %v, cannot read body: %w", res.StatusCode, e)
		}
		return fmt.Errorf("status: %v, body: %v", res.StatusCode, string(b))
	}

	if result != nil {
		if ctype == "gob" {
			if err = gob.NewDecoder(res.Body).Decode(result); err != nil {
				return fmt.Errorf("reading body: %w", err)
			}
		} else {
			if err = json.NewDecoder(res.Body).Decode(result); err != nil {
				return fmt.Errorf("reading body: %w", err)
			}
		}
	}
	return nil
}

/**/

func parseCommand(args []string) *command {
	cmd := new(command)
	cmd.ctx, _ = signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)

	cmd.name, cmd.args = args[0], args[1:]
	return cmd
}

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

	keyFile, err := os.OpenFile(messKeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = keyFile.Close() }()

	if err = pem.Encode(keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}); err != nil {
		return fmt.Errorf("failed to encode PEM (key): %w", err)
	}

	crtFile, err := os.OpenFile(messCrtFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
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

/*
func SignTimestamp(privPem []byte) ([]byte, []byte, error) {
	block, _ := pem.Decode(privPem)
	if block == nil {
		return nil, nil, errors.New("invalid private key PEM")
	}

	// Ed25519 private key in PKCS8 format
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse private key: %w", err)
	}

	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, nil, errors.New("not an ed25519 private key")
	}

	// timestamp 8 bytes big-endian
	ts := time.Now().Unix()
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(ts))

	// Ed25519 signs raw message (no hashing)
	sig := ed25519.Sign(priv, payload)

	return payload, sig, nil
}
*/

func signatureHeader(key ed25519.PrivateKey) string {
	data := make([]byte, 8, 48)
	binary.BigEndian.PutUint64(data, uint64(time.Now().Unix()))
	b := append(data, ed25519.Sign(key, data)...)
	return hex.EncodeToString(b)
}

/**/

const (
	colorReset    = "\033[0m"
	colorProgress = "\033[97m"
	colorError    = "\033[31m"
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

func (p *pprinter) ok() {
	fmt.Printf("%s[  OK  ] %s\n", cleanReturn, p.lastMessage)
}

func (p *pprinter) skip() {
	fmt.Printf("%s%s[ SKIP ] %s%s\n", cleanReturn, colorSkip, p.lastMessage, colorReset)
}

func (p *pprinter) fail(v any) {
	fmt.Printf("%s%s[ FAIL ] %s\n", cleanReturn, colorError, p.lastMessage)
	fmt.Printf("         error: %v%s\n", v, colorReset)
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

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}
