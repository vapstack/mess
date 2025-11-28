package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
)

const pulseInterval = 13 * time.Second

func (n *node) pulsing() {
	defer n.wg.Done()
	b := new(bytes.Buffer)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(pulseInterval):
			b.Reset()
			d := n.getState()
			e := gob.NewEncoder(b).Encode(d)
			if e != nil {
				n.logf("pulse: encode: %v", e)
				continue
			}
			for _, rec := range n.state.Load().Map {
				if rec.ID > n.id || time.Since(time.Unix(rec.LastSync, 0)) > 2*time.Minute {
					go n.pulse(rec, b.Bytes())
				}
			}
		}
	}
}

func (n *node) pulse(rec *mess.Node, data []byte) {
	addr := rec.Address()
	if addr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(n.ctx, pulseInterval)
	defer cancel()

	dst := "https://" + addr + publicPortStr + "/pulse"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, bytes.NewReader(data))
	if err != nil {
		n.logf("pulse: creating request to %v: %v", dst, err)
		return
	}

	// req.Header.Set("Content-Type", "application/gob")
	// req.Header.Set(mess.CallerHeader, internal.ConstructCaller(n.id, "", mess.ServiceName))
	// req.Header.Set(mess.TargetServiceHeader, mess.ServiceName)
	// req.Header.Set(mess.CallerServiceHeader, mess.ServiceName)
	// req.Header.Set(mess.CallerNodeHeader, strconv.FormatUint(n.id, 10))

	req.Header.Set(mess.TargetNodeHeader, strconv.FormatUint(rec.ID, 10))

	res, err := n.client.Do(req)
	if err != nil {
		n.logf("pulse: %v", err)
		return
	}
	defer internal.DrainAndCloseBody(res)
	// defer func(rsp *http.Response) { _ = rsp.Body.Close() }(res)
	// defer func(rsp *http.Response) { _, _ = io.Copy(io.Discard, rsp.Body) }(res)

	if res.StatusCode != http.StatusOK {
		b, err := io.ReadAll(res.Body)
		if err != nil {
			n.logf("pulse: reading body: %v", err)
			return
		}
		if len(b) > 0 {
			n.logf("pulse: status: %v, body: %v", res.StatusCode, string(b))
			return
		}
		n.logf("pulse: status: %v", res.StatusCode)
		return
	}

	v := new(mess.NodeState)
	if err = gob.NewDecoder(res.Body).Decode(v); err != nil {
		n.logf("pulse: decoding body: %v", err)
		return
	}
	if _, err = n.applyPeerMap(v, addr); err != nil {
		n.logf("pulse: %v", err)
	}
}

func (n *node) applyPeerMap(req *mess.NodeState, remoteAddr string) (*mess.NodeState, error) {

	n.mu.Lock()
	defer n.mu.Unlock()

	// d := n.getState()
	d := n.stateClone()

	if req.Node != nil { // && !req.Node.Passive {
		req.Node.Addr = remoteAddr
		req.Node.LastSync = time.Now().Unix()
		d.Map[req.Node.ID] = req.Node
	}

	for _, remoteNode := range req.Map {
		if remoteNode.ID != d.Node.ID {
			if _, exist := d.Map[remoteNode.ID]; !exist {
				d.Map[remoteNode.ID] = remoteNode
			}
		}
	}

	return d, n.saveState(d)
}
