package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
	"github.com/vapstack/mess/internal/storage"
)

func (cli *CLI) runCommand(cmd *command) (int, error) {
	switch cmd.name {
	case "sync":
		return cli.cmdSync(cmd)
	case "map":
		return cli.cmdMap(cmd)
	case "rec":
		return cli.cmdShowRec(cmd)

	case "new":
		return cli.cmdNew(cmd)
	case "gen":
		return cli.cmdGen(cmd)
	case "add":
		return cli.cmdAdd(cmd)
	case "rotate":
		return cli.cmdRotate(cmd)
	case "upgrade":
		return cli.cmdUpgrade(cmd)
	case "shutdown":
		return cli.cmdShutdown(cmd)

	case "put":
		return cli.cmdPut(cmd)
	case "start", "stop", "restart", "delete":
		return cli.cmdServiceCommand(cmd)
	case "upload", "store", "deploy":
		return cli.cmdDeploy(cmd)

	default:
		return 1, fmt.Errorf("unknown command: %v", cmd.name)
	}
}

func (cli *CLI) cmdNew(cmd *command) (int, error) {

	if len(cmd.args) < 1 || len(cmd.args) > 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	if !cli.rootMode {
		return 1, fmt.Errorf("this operation requires a root mess key")
	}

	loc := strings.Split(cmd.args[0], ".")
	if len(loc) != 3 {
		return 1, fmt.Errorf("location must be in form <region>.<country>.<datacenter>")
	}

	/**/

	cli.state.LastID++
	if cli.state.LastID > uint64(mess.MaxNodeID) {
		return 1, fmt.Errorf("node id exceeds %v (16-bit)", mess.MaxNodeID)
	}
	nodeID := cli.state.LastID

	days := 365
	if len(cmd.args) == 2 {
		if d, err := strconv.ParseUint(cmd.args[1], 10, 64); err != nil {
			return 1, fmt.Errorf("invalid cert lifetime: %w", err)
		} else if d < 2 {
			return 1, fmt.Errorf("cert lifetime must be >= 2")
		} else {
			days = int(d)
		}
	}

	keyBytes, crtBytes, err := cli.createNodeCert(nodeID, days)
	if err != nil {
		return 1, err
	}
	if err = storage.WriteFile("node.key", keyBytes); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}
	if err = storage.WriteFile("node.crt", crtBytes); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}

	/**/

	if _, err = os.Stat("node.json"); err == nil {
		if err = os.Remove("node.json"); err != nil {
			return 1, fmt.Errorf("failed to delete existing node.json: %w", err)
		}
	}
	state := &mess.NodeState{
		Node: &mess.Node{
			ID:         nodeID,
			Region:     strings.ToLower(loc[0]),
			Country:    strings.ToLower(loc[1]),
			Datacenter: strings.ToLower(loc[2]),
			Services:   make([]*mess.Service, 0),
		},
		Map: make(mess.NodeMap),
	}
	if err = storage.WriteObject("node.json", state); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}

	if err = cli.saveState(); err != nil {
		_ = os.Remove("node.json")
		_ = os.Remove("node.key")
		_ = os.Remove("node.crt")
		return 1, fmt.Errorf("failed to update mess state: %w", err)
	}

	return 0, nil
}

func (cli *CLI) cmdGen(cmd *command) (int, error) {

	if l := len(cmd.args); l < 1 || l > 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	if !cli.rootMode {
		return 1, fmt.Errorf("this operation requires a root mess key")
	}

	nodeID := uint64(0)

	if arg := cmd.args[0]; arg != "ops" {
		var err error
		nodeID, err = strconv.ParseUint(cmd.args[0], 10, 64)
		if err != nil {
			return 1, fmt.Errorf("parse node id: %w", err)
		}
		if cli.state.Map.Get(nodeID) == nil {
			return 1, fmt.Errorf("node %v not found", nodeID)
		}
	}

	days := 365
	if len(cmd.args) == 2 {
		if d, err := strconv.ParseUint(cmd.args[1], 10, 64); err != nil {
			return 1, fmt.Errorf("invalid cert lifetime: %w", err)
		} else if d < 2 {
			return 1, fmt.Errorf("cert lifetime must be >= 2")
		} else {
			days = int(d)
		}
	}

	keyBytes, crtBytes, err := cli.createNodeCert(nodeID, days)
	if err != nil {
		return 1, err
	}

	keyFile := "node.key"
	crtFile := "node.crt"

	if nodeID == 0 {
		keyFile = "ops.key"
		crtFile = "ops.crt"
	}

	if err = storage.WriteFile(keyFile, keyBytes); err != nil {
		return 1, fmt.Errorf("write error (key): %w", err)
	}
	if err = storage.WriteFile(crtFile, crtBytes); err != nil {
		return 1, fmt.Errorf("write error (crt): %w", err)
	}

	return 0, nil
}

func (cli *CLI) cmdRotate(cmd *command) (int, error) {
	if len(cmd.args) > 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	if !cli.rootMode {
		return 1, fmt.Errorf("this operation requires a root mess key")
	}

	days := 365

	if len(cmd.args) > 0 {
		d, err := strconv.ParseUint(cmd.args[0], 10, 32)
		if err != nil {
			return 1, fmt.Errorf("invalid cert lifetime: %w", err)
		}
		if d < 1 {
			return 1, fmt.Errorf("invalid cert lifetime: %v", d)
		}
		days = int(d)
	}

	var force bool
	for _, arg := range cmd.args {
		if force = strings.ToLower(arg) == "force"; force {
			break
		}
	}

	updated := 0

	cli.lazySync()

	ec := cli.eachNodeProgress(func(rec *mess.Node) error {

		if !force {
			remainDays := time.Until(time.Unix(rec.CertExpires, 0)).Hours() / 24
			if remainDays > float64(days)*0.8 {
				return errSkip
			}
		}

		keyPEM, crtPEM, e := cli.createNodeCert(rec.ID, days)
		if e != nil {
			return e
		}

		e = cli.call(rec.Address(), "rotate", internal.RotateRequest{
			Key: string(keyPEM),
			Crt: string(crtPEM),
		}, nil)
		if e != nil {
			return e
		}

		rec.CertExpires = time.Now().AddDate(0, 0, days).Unix()
		updated++

		return nil
	})

	if updated > 0 {
		if err := cli.saveState(); err != nil {
			return 1, fmt.Errorf("updating mess state: %w", err)
		}
	}

	return ec, nil
}

func (cli *CLI) cmdAdd(cmd *command) (int, error) {
	if len(cmd.args) != 1 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}

	addr := cmd.args[0]

	if net.ParseIP(addr) == nil {
		return 1, fmt.Errorf("cannot parse IP: %v", addr)
	}

	ec := pstartf(addr).cover(func() error {
		state := new(mess.NodeState)
		if err := cli.call(addr, "state", nil, state); err != nil {
			return err
		}
		if state.Node == nil {
			return fmt.Errorf("no state information returned by the node (invalid response)")
		}
		if state.Node.ID == 0 {
			return fmt.Errorf("node is running with id 0, such id cannot be used by a node (invalid id)")
		}
		if cli.state.Map.Get(state.Node.ID) != nil {
			return fmt.Errorf("the node is either already added or running with a node id already taken (duplicate node id)")
		}
		if err := cli.call(addr, "pulse", cli.state, state); err != nil {
			return err
		}
		return cli.applyState(state, addr)
	})

	return ec, nil
}

func (cli *CLI) cmdSync(cmd *command) (int, error) {

	if len(cmd.args) > 0 {

		if len(cmd.args) > 1 {
			return 1, fmt.Errorf("too many arguments")
		}
		addr := cmd.args[0]
		if net.ParseIP(addr) == nil {
			return 1, fmt.Errorf("cannot parse IP: %v", addr)
		}
		return pstartf(addr).cover(func() error {
			return cli.fetchState(addr)
		}), nil
	}

	if len(cli.state.Map) == 0 {
		return 1, fmt.Errorf("no known nodes")
	}

	ec := cli.eachNodeProgress(func(rec *mess.Node) error {
		return cli.fetchState(rec.Address())
	})
	return ec, nil
}

func (cli *CLI) lazySync() {
	pp := pstartf("Sync: ")
	defer func() {
		if v := recover(); v != nil {
			pp.fail(fmt.Errorf("panic: %v", v))
		}
	}()

	ids := make([]uint64, 0, len(cli.state.Map))
	for id := range cli.state.Map {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	var failed []string

	total := len(ids)
	padding := len(strconv.Itoa(total))

	for i, id := range ids {
		if err := cli.ctx.Err(); err != nil {
			pp.fail(err)
			return
		}

		rec := cli.state.Map[id]

		pp.printf("Sync: %*v/%v - %v - %v", padding, i, total, rec.ID, rec.Address())

		if err := cli.fetchState(rec.Address()); err != nil {
			failed = append(failed, strconv.FormatUint(rec.ID, 10))
		}
	}

	pp.printf(fmt.Sprintf("Sync: %*v/%v", padding, total-len(failed), total))

	if len(failed) > 0 {
		pp.warn(fmt.Sprintf("unreachable: %v", strings.Join(failed, ", ")))
	} else {
		pp.ok()
	}
}

func (cli *CLI) cmdMap(cmd *command) (int, error) {
	if len(cmd.args) > 1 {
		return 1, fmt.Errorf("too many arguments")
	}
	if len(cmd.args) > 0 {
		if cmd.args[0] == "json" {
			b, err := json.MarshalIndent(cli.state, "", "    ")
			if err != nil {
				return 1, err
			}
			fmt.Println(string(b))
			return 0, nil
		} else {
			return 1, fmt.Errorf("unknown argument: %v", cmd.args[0])
		}
	}
	if len(cli.state.Map) == 0 {
		return 1, fmt.Errorf("no known nodes")
	}

	recs := make([]*mess.Node, 0, len(cli.state.Map))
	for _, rec := range cli.state.Map {
		recs = append(recs, rec)
	}

	slices.SortFunc(recs, func(a, b *mess.Node) int {
		return strings.Compare(a.Location(), b.Location())
	})

	for _, rec := range recs {
		fmt.Printf(" %-5v - %-15v - %v - crt/%v\n", rec.ID, rec.Address(), rec.Location(), time.Unix(rec.CertExpires, 0).Format("2006-01-02"))
		for _, svc := range rec.Services {
			var (
				status string
				rtype  string
			)
			if svc.Active {
				status = "UP"
			} else {
				status = "DOWN"
			}
			if svc.Manual {
				rtype = "MANUAL"
			} else {
				rtype = "AUTO"
			}
			fmt.Printf("    %v - %v - %v\n", internal.ServiceName(svc.Name, svc.Realm), rtype, status)
			if len(svc.Alias) > 0 {
				fmt.Printf("    [%v]\n", strings.Join(svc.Alias, ", "))
			}
		}
	}
	return 0, nil
}

func (cli *CLI) cmdShowRec(cmd *command) (int, error) {
	if len(cmd.args) > 1 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	b, err := json.MarshalIndent(mess.Service{
		Name:    "ServiceName",
		Alias:   []string{"friendly-name", "http-api"},
		Realm:   "namespace",
		Manual:  false,
		Private: false,
		Start:   "binary_filename",
		Order:   10,
		Args: []string{
			"-example-arg=value",
		},
		Env: []string{
			"EXAMLE_ENV=value",
		},
		Listen:  ":8090",
		Proxy:   "tcp",
		Timeout: 30,
		Meta: map[string]string{
			"custom": "field",
		},
	}, "", "    ")
	if err != nil {
		return 1, err
	}
	fmt.Println(string(b))
	return 0, nil
}

func (cli *CLI) cmdUpgrade(cmd *command) (int, error) {
	if len(cmd.args) < 2 || len(cmd.args) > 3 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	if !cli.rootMode {
		return 1, fmt.Errorf("this operation requires a root mess key")
	}

	file := cmd.args[0]
	node := cmd.args[1]

	now := len(cmd.args) == 3 && cmd.args[2] == "now"

	s, err := os.Stat(file)
	if err != nil {
		return 1, err
	}
	if s.Size() < 1<<20 || s.Size() > 64<<20 {
		return 1, fmt.Errorf("suspicious file size: %v", s.Size())
	}

	ec := 0
	if node == "all" {
		cli.lazySync()
		ec = cli.eachNodeProgress(func(rec *mess.Node) error {
			if e := cli.post(rec.Address(), "upgrade", "", file); e != nil {
				return e
			}
			if now {
				return cli.call(rec.Address(), "shutdown", nil, nil)
			}
			return nil
		})

	} else {
		ec = cli.nodeProgress(node).cover(func() error {
			if e := cli.post(cli.addr(node), "upgrade", "", file); e != nil {
				return e
			}
			if now {
				return cli.call(cli.addr(node), "shutdown", nil, nil)
			}
			return nil
		})
	}
	return ec, nil
}

func (cli *CLI) cmdShutdown(cmd *command) (int, error) {
	if len(cmd.args) != 1 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	node := cmd.args[0]

	if node == "all" {
		cli.lazySync()
		return cli.eachNodeProgress(func(rec *mess.Node) error {
			return cli.call(rec.Address(), "shutdown", nil, nil)
		}), nil
	}
	return cli.nodeProgress(node).cover(func() error {
		return cli.call(cli.addr(node), "shutdown", nil, nil)
	}), nil
}

func (cli *CLI) cmdPut(cmd *command) (int, error) {
	if len(cmd.args) != 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	file := cmd.args[0]
	node := cmd.args[1]

	if _, err := os.Stat(file); err != nil {
		return 1, err
	}

	s := new(mess.Service)
	if err := storage.ReadObject(file, s); err != nil {
		return 1, err
	}
	if cli.state.RequireRealm && s.Realm == "" {
		return 1, fmt.Errorf("realm is empty")
	}
	if s.Name == "" {
		return 1, fmt.Errorf("name is empty")
	}
	if s.Start == "" {
		return 1, fmt.Errorf("start is empty")
	}
	if s.Listen != "" {
		if _, _, err := internal.ParseNetworkAddr(s.Listen); err != nil {
			return 1, fmt.Errorf("invalid Listen: %w", err)
		}
	}
	s.Active = false

	if node == "all" {
		cli.lazySync()
		return cli.eachNodeProgress(func(rec *mess.Node) error {
			return cli.call(rec.Address(), "put", s, nil)
		}), nil
	}
	return cli.nodeProgress(node).cover(func() error {
		return cli.call(cli.addr(node), "put", s, nil)
	}), nil
}

func (cli *CLI) cmdServiceCommand(cmd *command) (int, error) {
	if len(cmd.args) != 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	service, realm := internal.ParseServiceRealm(cmd.args[0])

	if cli.state.RequireRealm && realm == "" {
		return 1, fmt.Errorf("realm is empty")
	}

	node := cmd.args[1]

	ep := fmt.Sprintf("%v?service=%v&realm=%v", cmd.name, realm, service)

	if node == "all" {
		cli.lazySync()
		return cli.eachNodeProgress(func(rec *mess.Node) error {
			return cli.call(rec.Address(), ep, nil, nil)
		}), nil
	}
	return cli.nodeProgress(node).cover(func() error {
		return cli.call(cli.addr(node), ep, nil, nil)
	}), nil
}

func (cli *CLI) cmdDeploy(cmd *command) (int, error) {
	if len(cmd.args) != 3 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	deploy := cmd.name == "deploy"
	file := cmd.args[0]

	service, realm := internal.ParseServiceRealm(cmd.args[1])

	if cli.state.RequireRealm && realm == "" {
		return 1, fmt.Errorf("realm is empty")
	}

	node := cmd.args[2]

	if fi, err := os.Stat(file); err != nil {
		return 1, err
	} else if fi.Size() > 1<<30 {
		return 1, fmt.Errorf("size too large: %v", fi.Size())
	}

	qry := fmt.Sprintf("?service=%v&realm=%v", service, realm)

	var ep string

	if deploy {
		ep = fmt.Sprintf("restart?realm=%v&service=%v", realm, service)
	}

	if node == "all" {
		cli.lazySync()
		return cli.eachNodeProgress(func(rec *mess.Node) error {
			if err := cli.post(rec.Address(), "store", qry, file); err != nil {
				return err
			}
			if deploy {
				return cli.call(rec.Address(), ep, nil, nil)
			}
			return nil
		}), nil
	}

	return cli.nodeProgress(node).cover(func() error {
		if err := cli.post(cli.addr(node), "store", qry, file); err != nil {
			return err
		}
		if deploy {
			return cli.call(cli.addr(node), ep, nil, nil)
		}
		return nil
	}), nil
}
