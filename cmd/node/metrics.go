package main

import (
	"github.com/vapstack/mess/internal/proxy"
)

func (n *node) collectProxyMetrics(b *proxy.Wrapper) {
	// duration := time.Since(b.Start)

	// enc := json.NewEncoder(os.Stdout)
	// enc.SetIndent("", "  ")
	// _ = enc.Encode(map[string]any{
	// 	"start":     b.start,
	// 	"duration":  b.duration,
	// 	"caller":    b.caller,
	// 	"target":    b.target,
	// 	"srcNodeID": b.srcNodeID,
	// 	"dstNodeID": b.dstNodeID,
	// 	"status":    b.status,
	// 	"bytes":     b.bytes,
	// })

	// todo
	// n.metrics <- bw // + somewhere putWriter(bw)
}

func (n *node) collectClientMetrics(m *clientMeter) {
	// duration := time.Since(m.Start)

	// todo
	// n.metrics <- ...
}
