package conf

import (
    "sync"
)

//per server info structure
type ServerInfo struct {
	currentVbuckets int16
	maxVbuckets     int16
}

type Conf struct {
	Port     int16
	Vbuckets int16
	Replica  int16
	Hash     string
	Capacity int16
	Servers  []string
}

type VBucketInfo struct {
	VBucketMap    [][]int
	HashAlgorithm string
	NumReplicas   int
	ServerList    []string
}

type DeadVbucketInfo struct {
	Server  string
	Active  []int
	Replica []int
}

type VbaEntry struct {
	Source      string
	VbId        []int
	Destination string
}

type stateEntry struct {
	server int
	vbid   int
	state  string
}

type ParsedInfo struct {
	C       Conf         //input Conf
	V       VBucketInfo  // vbucketMap to send to client
	S       []ServerInfo // per server information
	VbaInfo map[string]VbaEntry
	M       sync.RWMutex
}

func NewStateEntry(s int, vbid int, st string) stateEntry {
	se := stateEntry{
		server: s,
		vbid:   vbid,
		state:  st,
	}
	return se
}

