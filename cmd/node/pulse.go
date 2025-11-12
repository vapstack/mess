package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"mess"
	"net/http"
	"strconv"
	"time"
)

func (n *node) pulsing() {
	defer n.wg.Done()
	interval := 27 * time.Second
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(interval):
			d := n.getStatefullData()
			b := new(bytes.Buffer)
			e := gob.NewEncoder(b).Encode(d)
			if e != nil {
				n.messlogf("pulse: encode: %v", e)
				continue
			}
			for _, rec := range n.data.Load().Map {
				if time.Since(time.Unix(rec.LastSync, 0)) > interval {
					go n.pulse(rec, b.Bytes())
				}
			}
		}
	}
}

func (n *node) pulse(rec *mess.Node, data []byte) {
	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()

	dst := fmt.Sprintf("https://%v:%v/pulse", rec.Address(), mess.PublicPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dst, bytes.NewReader(data))
	if err != nil {
		n.messlogf("pulse: creating request to %v: %v", dst, err)
		return
	}

	req.Header.Set("Content-Type", "application/gob")
	req.Header.Set(mess.TargetServiceHeader, mess.MessService)
	req.Header.Set(mess.CallerServiceHeader, mess.MessService)
	req.Header.Set(mess.TargetNodeHeader, strconv.FormatUint(rec.ID, 10))
	req.Header.Set(mess.CallerNodeHeader, strconv.FormatUint(n.id, 10))

	res, err := n.client.Do(req)
	if err != nil {
		n.messlogf("pulse: request to %v: %v", dst, err)
		return
	}
	defer func(rsp *http.Response) { _ = rsp.Body.Close() }(res)
	defer func(rsp *http.Response) { _, _ = io.Copy(io.Discard, rsp.Body) }(res)

	if res.StatusCode != http.StatusOK {
		b, err := io.ReadAll(res.Body)
		if err != nil {
			n.messlogf("pulse: reading body: %v", err)
			return
		}
		if len(b) > 0 {
			n.messlogf("pulse: status: %v, body: %v", res.StatusCode, string(b))
			return
		}
		n.messlogf("pulse: status: %v", res.StatusCode)
		return
	}

	v := new(mess.NodeData)
	if err = gob.NewDecoder(res.Body).Decode(v); err != nil {
		n.messlogf("pulse: decoding body: %v", err)
		return
	}
	if _, err = n.applyPeerMap(v, rec.Address()); err != nil {
		n.messlogf("pulse: %v", err)
	}
}

func (n *node) applyPeerMap(req *mess.NodeData, remoteAddr string) (*mess.NodeData, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	d := n.dataClone()

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

	return d, n.storeData(d)
}
