package main

import (
	"log"

	"github.com/cockroachdb/pebble/v2"

	"github.com/shirou/gopsutil/v4/mem"
)

var (
	pebbleCache *pebble.Cache

	hostRAM = uint64(16 << 30)
)

func init() {
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Println("error getting available memory:", err)
	}
	if v.Total > 0 {
		hostRAM = v.Total
	}
	size := float64(hostRAM) * 0.025
	if size >= 1<<30 {
		size = 1 << 30
	}
	pebbleCache = pebble.NewCache(int64(size))
}

func pebbleLogOptions() *pebble.Options {
	mtsize := uint64(2 << 20)
	if hostRAM >= 30<<30 {
		mtsize = 4 << 20
	}
	opts := &pebble.Options{
		Cache:        pebbleCache,
		MemTableSize: mtsize,
		// MemTableStopWritesThreshold: 2,
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 4,
		MaxOpenFiles:          50,
		Levels: [7]pebble.LevelOptions{
			{
				BlockSize:    64 << 10,
				FilterPolicy: nil,
			},
		},
	}
	return opts
}

func pebbleBusOptions() *pebble.Options {
	mtsize := uint64(4 << 20)
	if hostRAM >= 40<<30 {
		mtsize = 8 << 20
	}
	opts := &pebble.Options{
		Cache:        pebbleCache,
		MemTableSize: mtsize,
		// MemTableStopWritesThreshold: 2,
		// L0CompactionThreshold: 4,
		// L0StopWritesThreshold: 8,
		MaxOpenFiles: 50,
		Levels: [7]pebble.LevelOptions{
			{
				BlockSize:    64 << 10,
				FilterPolicy: nil,
			},
		},
	}
	return opts
}

func closeIter(iter *pebble.Iterator) {
	if err := iter.Error(); err != nil {
		log.Printf("pebble iterator error: %v\n", err)
	}
	if err := iter.Close(); err != nil {
		log.Printf("error closing pebble iterator: %v\n", err)
	}
}

func closeBatch(b *pebble.Batch) {
	if err := b.Close(); err != nil {
		log.Printf("error closing pebble batch: %v\n", err)
	}
}
