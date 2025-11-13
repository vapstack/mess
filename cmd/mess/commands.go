package main

import (
	"encoding/json"
	"fmt"
	"mess"
	"mess/internal"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

func runCommand(cmd *command) (int, error) {
	switch cmd.name {
	case "sync":
		return cmdSync(cmd)
	case "map":
		return cmdMap(cmd)
	case "rec":
		return cmdShowRec(cmd)

	case "new":
		return cmdNew(cmd)
	case "gen":
		return cmdGen(cmd)
	case "greet":
		return cmdGreet(cmd)
	case "rotate":
		return cmdRotate(cmd)
	case "upgrade":
		return cmdUpgrade(cmd)
	case "shutdown":
		return cmdShutdown(cmd)

	case "put":
		return cmdPut(cmd)
	case "start", "stop", "restart", "delete":
		return cmdServiceCommand(cmd)
	case "upload", "store", "deploy":
		return cmdDeploy(cmd)

	default:
		return 1, fmt.Errorf("unknown command: %v", cmd.name)
	}
}

func cmdNew(cmd *command) (int, error) {

	if len(cmd.args) < 1 || len(cmd.args) > 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	loc := strings.Split(cmd.args[0], ".")
	if len(loc) != 3 {
		return 1, fmt.Errorf("location must be in form <region>.<country>.<datacenter>")
	}

	/**/

	cmd.mess.data.LastID++
	nodeID := cmd.mess.data.LastID

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

	keyBytes, crtBytes, err := cmd.mess.createNodeCert(nodeID, days)
	if err != nil {
		return 1, err
	}
	if err = internal.WriteFile("node.key", keyBytes); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}
	if err = internal.WriteFile("node.crt", crtBytes); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}

	/**/

	if _, err = os.Stat("node.json"); err == nil {
		if err = os.Remove("node.json"); err != nil {
			return 1, fmt.Errorf("failed to delete existing node.json: %w", err)
		}
	}
	ndata := &mess.NodeData{
		Node: &mess.Node{
			ID:         nodeID,
			Region:     strings.ToLower(loc[0]),
			Country:    strings.ToLower(loc[1]),
			Datacenter: strings.ToLower(loc[2]),
			Services:   make([]*mess.Service, 0),
		},
		Map: make(mess.Map),
	}
	if err = internal.WriteObject("node.json", ndata); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}

	if err = cmd.mess.saveMess(); err != nil {
		_ = os.Remove("node.json")
		_ = os.Remove("node.key")
		_ = os.Remove("node.crt")
		return 1, fmt.Errorf("failed to update mess data: %w", err)
	}

	return 0, nil
}
func cmdGen(cmd *command) (int, error) {

	if len(cmd.args) != 1 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}

	nodeID, err := strconv.ParseUint(cmd.args[0], 10, 64)
	if err != nil {
		return 1, fmt.Errorf("parse node id: %w", err)
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

	keyBytes, crtBytes, err := cmd.mess.createNodeCert(nodeID, days)
	if err != nil {
		return 1, err
	}
	if err = internal.WriteFile("node.key", keyBytes); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}
	if err = internal.WriteFile("node.crt", crtBytes); err != nil {
		return 1, fmt.Errorf("write error: %w", err)
	}

	return 0, nil
}

func cmdRotate(cmd *command) (int, error) {
	if len(cmd.args) > 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	if !cmd.mess.rootMode {
		return 1, fmt.Errorf("no mess key found")
	}
	days := 365
	if len(cmd.args) > 1 {
		d, err := strconv.Atoi(cmd.args[1])
		if err != nil {
			return 1, fmt.Errorf("invalid cert lifetime: %w", err)
		}
		days = d
	}
	var force bool
	for _, arg := range cmd.args {
		if force = strings.ToLower(arg) == "force"; force {
			break
		}
	}
	updated := 0
	ec := cmd.eachNodeProgress(func(rec *mess.Node) error {
		if !force && time.Unix(rec.CertExpires, 0).AddDate(0, 0, -days).After(time.Now()) {
			return errSkip
		}
		keyPEM, crtPEM, e := cmd.mess.createNodeCert(rec.ID, days)
		if e != nil {
			return e
		}
		e = cmd.call(rec.Address(), "cert", internal.RotateRequest{
			Key: string(keyPEM),
			Crt: string(crtPEM),
		}, nil)
		if e != nil {
			return e
		}
		rec.CertExpires = time.Now().AddDate(1, 0, 0).Unix()
		updated++
		return nil
	})
	if updated > 0 {
		if err := cmd.mess.saveMess(); err != nil {
			return 1, fmt.Errorf("updating mess data: %w", err)
		}
	}
	return ec, nil
}

func cmdGreet(cmd *command) (int, error) {
	if len(cmd.args) != 1 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	addr := cmd.args[0]
	if net.ParseIP(addr) == nil {
		return 1, fmt.Errorf("cannot parse IP: %v", addr)
	}
	ec := pstartf(addr).cover(func() error {
		if err := cmd.call(addr, "pulse", cmd.mess.data, nil); err != nil {
			return err
		}
		return cmd.fetchMap(addr)
	})
	return ec, nil
}

func cmdSync(cmd *command) (int, error) {
	if len(cmd.args) > 0 {
		if len(cmd.args) > 1 {
			return 1, fmt.Errorf("too many arguments")
		}
		addr := cmd.args[0]
		if net.ParseIP(addr) == nil {
			return 1, fmt.Errorf("cannot parse IP: %v", addr)
		}
		return pstartf(addr).cover(func() error { return cmd.fetchMap(addr) }), nil
	}
	if len(cmd.mess.data.Map) == 0 {
		return 1, fmt.Errorf("no known nodes")
	}
	ec := 0
	for _, rec := range cmd.mess.data.Map {
		ec += nodeProgress(rec).cover(func() error { return cmd.fetchMap(rec.Address()) })
	}
	return ec, nil
}

func cmdMap(cmd *command) (int, error) {
	if len(cmd.args) > 1 {
		return 1, fmt.Errorf("too many arguments")
	}
	if len(cmd.args) > 0 {
		if cmd.args[0] == "json" {
			b, err := json.MarshalIndent(cmd.mess.data, "", "    ")
			if err != nil {
				return 1, err
			}
			fmt.Println(string(b))
			return 0, nil
		} else {
			return 1, fmt.Errorf("unknown argument: %v", cmd.args[0])
		}
	}
	if len(cmd.mess.data.Map) == 0 {
		return 1, fmt.Errorf("no known nodes")
	}
	recs := make([]*mess.Node, 0, len(cmd.mess.data.Map))
	for _, rec := range cmd.mess.data.Map {
		recs = append(recs, rec)
	}
	slices.SortFunc(recs, func(a, b *mess.Node) int {
		return strings.Compare(a.Location(), b.Location())
	})
	for _, rec := range recs {
		fmt.Printf("%-5v - %-15v - %v - crt/%v\n", rec.ID, rec.Address(), rec.Location(), time.Unix(rec.CertExpires, 0).Format("2006-01-02"))
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
			if svc.Realm != "" {
				fmt.Printf("    %v@%v - %v - %v\n", svc.Name, svc.Realm, rtype, status)
			} else {
				fmt.Printf("    %v - %v - %v\n", svc.Name, rtype, status)
			}
			if len(svc.Alias) > 0 {
				fmt.Printf("    %v - %v - %v\n", svc.Name, rtype, status)
			}
		}
	}
	return 0, nil
}

func cmdShowRec(cmd *command) (int, error) {
	if len(cmd.args) > 1 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	b, err := json.MarshalIndent(mess.Service{
		Name:    "service-name",
		Realm:   "namespace",
		Alias:   []string{"friendly-name", "http-api"},
		Manual:  false,
		Passive: false,
		Start:   "binary_filename",
		Order:   10,
		Args: []string{
			"-example-arg=value",
		},
		Env: []string{
			"EXAMLE_ENV=value",
		},
		Listen:  "tcp://127.0.0.1:8090",
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

func cmdUpgrade(cmd *command) (int, error) {
	if len(cmd.args) != 2 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	node := cmd.args[0]
	file := cmd.args[1]

	s, err := os.Stat(file)
	if err != nil {
		return 1, err
	}
	if s.Size() < 1<<20 || s.Size() > 1<<26 {
		return 1, fmt.Errorf("suspicious file size: %v", s.Size())
	}

	ec := 0
	if node != "all" {
		ec = cmd.nodeProgress(node).cover(func() error {
			return cmd.post(cmd.addr(node), "upgrade", "", file)
		})
	} else {
		ec = cmd.eachNodeProgress(func(rec *mess.Node) error {
			return cmd.call(rec.Address(), "upgrade", "", nil)
		})
	}
	return ec, nil
}

func cmdShutdown(cmd *command) (int, error) {
	if len(cmd.args) != 1 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	node := cmd.args[0]
	ec := 0
	if node != "all" {
		ec = cmd.nodeProgress(node).cover(func() error {
			return cmd.call(cmd.addr(node), "shutdown", nil, nil)
		})
	} else {
		ec = cmd.eachNodeProgress(func(rec *mess.Node) error {
			return cmd.call(rec.Address(), "shutdown", nil, nil)
		})
	}
	return ec, nil
}

func cmdPut(cmd *command) (int, error) {
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
	if err := internal.ReadObject(file, s); err != nil {
		return 1, err
	}
	if cmd.mess.data.RequireRealm && s.Realm == "" {
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
	return cmd.nodeProgress(node).cover(func() error {
		return cmd.call(cmd.addr(node), "put", s, nil)
	}), nil
}

func cmdServiceCommand(cmd *command) (int, error) {
	if len(cmd.args) < 2 || len(cmd.args) > 3 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	service, realm := internal.ParseServiceRealm(cmd.args[0])

	if cmd.mess.data.RequireRealm && realm == "" {
		return 1, fmt.Errorf("realm is empty")
	}

	node := cmd.args[1]
	timeout := 0
	if len(cmd.args) == 3 {
		if v, _ := strconv.Atoi(cmd.args[2]); v > 0 {
			timeout = v
		}
	}

	sc := internal.ServiceCommand{
		Realm:   realm,
		Service: service,
		Timeout: timeout,
	}

	if node == "all" {
		return cmd.eachNodeProgress(func(rec *mess.Node) error {
			if e := cmd.call(rec.Address(), cmd.name, sc, nil); e != nil {
				return e
			}
			return nil
		}), nil
	}
	return cmd.nodeProgress(node).cover(func() error {
		if e := cmd.call(cmd.addr(node), cmd.name, sc, nil); e != nil {
			return e
		}
		return nil
	}), nil
}

func cmdDeploy(cmd *command) (int, error) {
	if len(cmd.args) < 2 || len(cmd.args) > 3 {
		printCommandUsage(cmd.name, 0)
		return 1, nil
	}
	deploy := cmd.name == "deploy"
	file := cmd.args[0]
	service, realm := internal.ParseServiceRealm(cmd.args[1])
	if cmd.mess.data.RequireRealm && realm == "" {
		return 1, fmt.Errorf("realm is empty")
	}
	node := "all"
	if len(cmd.args) == 3 {
		node = cmd.args[2]
	}
	if fi, err := os.Stat(file); err != nil {
		return 1, err
	} else if fi.Size() > 1<<30 {
		return 1, fmt.Errorf("size too large: %v", fi.Size())
	}

	qry := fmt.Sprintf("?service=%v&realm=%v&restart=%v", service, realm, btoi(deploy))

	if node == "all" {
		return cmd.eachNodeProgress(func(rec *mess.Node) error {
			return cmd.post(rec.Address(), "store", qry, file)
		}), nil
	}
	return cmd.nodeProgress(node).cover(func() error {
		return cmd.post(cmd.addr(node), "store", qry, file)
	}), nil
}
