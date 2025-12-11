package main

import (
	"fmt"
	"log"
	"path/filepath"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/bbolt"

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

func (n *node) getSeq(realm, name string) (seq uint64, err error) {

	bolt := n.seqdb.Load()

	if bolt == nil {

		n.logmu.Lock()
		defer n.logmu.Unlock()

		if bolt = n.seqdb.Load(); bolt == nil {
			bolt, err = bbolt.Open(filepath.Join(n.path, "seq"), 0o600, nil)
			if err != nil {
				return 0, fmt.Errorf("error opening db: %w", err)
			}
		}
	}

	tx, err := bolt.Begin(true)
	if err != nil {
		return 0, fmt.Errorf("tx error: %w", err)
	}
	defer rollback(tx)

	if realm == "" {
		realm = "default"
	}

	bucketName := unsafe.Slice(unsafe.StringData(realm), len(realm))

	realmBucket, err := tx.CreateBucketIfNotExists(bucketName)
	if err != nil {
		return 0, fmt.Errorf("bucket error (realm): %w", err)
	}

	if name == "" {
		name = "default"
	}

	bucketName = unsafe.Slice(unsafe.StringData(name), len(name))

	targetBucket, err := realmBucket.CreateBucketIfNotExists(bucketName)
	if err != nil {
		return 0, fmt.Errorf("bucket error (name): %w", err)
	}

	seq, err = targetBucket.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("error acquiring next sequence: %w", err)
	}

	return nodeSeq(n.id, seq), tx.Commit()
}

const nodeBits = 16

func nodeSeq(nodeID uint64, counter uint64) uint64 {
	nodeMask := (uint64(1) << nodeBits) - 1
	counterBits := 64 - nodeBits
	counterMask := (uint64(1) << counterBits) - 1
	return ((nodeID & nodeMask) << counterBits) | (counter & counterMask)
}
