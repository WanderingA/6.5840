package raft

import "time"


type PeerTracker struct {
	nextIndex  uint64
	matchIndex uint64

	lastAck time.Time
}

