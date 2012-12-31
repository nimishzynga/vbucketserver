package config

import (
	"sync"
)

//per server info structure
type ServerInfo struct {
	currentVbuckets int16
	maxVbuckets     int16
	NumberOfDisk    int16
}

type Cluster struct {
	ContextMap map[string]*Context //cluster name to context
	ConfigMap  map[string]Config   //cluster name to config
	IpMap      map[string]string   //ip addess to cluster name
	M          sync.RWMutex
}

type Config struct {
	Port         int16
	Vbuckets     int16
	Replica      int16
	Hash         string
	Capacity     int16
	Servers      []string
	SecondaryIps []string
}

type VbucketServerMap struct {
	VBucketMap    [][]int  `json:"vBucketMap"`
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
}

type VBucketInfo struct {
	Port int16            `json:"port"`
	Name string           `json:"name"`
	Smap VbucketServerMap `json:"vBucketServerMap"`
}

type DeadVbucketInfo struct {
	Server      string
	DisksFailed int
	Active      []int
	Replica     []int
}

type VbaEntry struct {
	Source      string
	VbId        []int
	Destination string
}

type ServerUpDownInfo struct {
	Server string
}

type CapacityUpdateInfo struct {
	Server    string
	DiskAlive int16
}

type Context struct {
	C       Config       //input Config
	V       VBucketInfo  // vbucketMap to send to client
	S       []ServerInfo // per server information
	VbaInfo map[string]VbaEntry
	M       sync.RWMutex
}

func NewCluster() *Cluster {
	cl := &Cluster{
		ContextMap: make(map[string]*Context),
		ConfigMap:  make(map[string]Config),
		IpMap:      make(map[string]string),
	}
	return cl
}
