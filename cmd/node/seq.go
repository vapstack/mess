package main

import (
	"fmt"
	"sync"
	"time"
)

// mostly snowflake with minor adjustments

const (
	sfEpoch int64 = 1577836800000

	sfNodeBits     uint64 = 15
	sfSequenceBits uint64 = 7

	sfNodeMax     int64 = -1 ^ (-1 << sfNodeBits)
	sfSequenceMax int64 = -1 ^ (-1 << sfSequenceBits)

	sfNodeShift = sfSequenceBits
	sfTimeShift = sfNodeBits + sfSequenceBits
)

type seqgen struct {
	mu            sync.Mutex
	lastTimestamp int64
	sequence      int64
	nodeID        int64
}

func newSeqGen(nodeID uint64) *seqgen {
	if nodeID > uint64(sfNodeMax) {
		panic(fmt.Sprintf("node id %v out of range (max: %v)", nodeID, sfNodeMax))
	}
	return &seqgen{nodeID: int64(nodeID)}
}

func (g *seqgen) next() uint64 {

	g.mu.Lock()

	now := time.Now().UnixMilli() - sfEpoch

	for now < g.lastTimestamp { // clock ticked back (bad NTP sync or something)
		time.Sleep(time.Millisecond)
		now = time.Now().UnixMilli() - sfEpoch
	}

	if now == g.lastTimestamp {
		g.sequence = (g.sequence + 1) & sfSequenceMax
		if g.sequence == 0 {
			for now <= g.lastTimestamp {
				time.Sleep(time.Millisecond / 4)
				now = time.Now().UnixMilli() - sfEpoch
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastTimestamp = now

	id := (now << sfTimeShift) |
		(g.nodeID << sfNodeShift) |
		g.sequence

	g.mu.Unlock()

	return uint64(id)
}

func timeFromSeq(id uint64) time.Time {
	return time.UnixMilli(int64(id>>sfTimeShift) + sfEpoch)
}

func seqFromTime(t time.Time) uint64 {
	ms := t.UnixMilli() - sfEpoch
	if ms < 0 {
		ms = 0
	}
	return uint64(ms << sfTimeShift)
}
