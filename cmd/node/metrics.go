package main

import (
	"mess/internal/proxy"
	"time"
)

func (n *node) collectMetrics(b *proxy.Base) {
	b.Duration = time.Since(b.Start)

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
