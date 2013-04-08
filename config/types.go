package config

import (
	"sync"
)

//per server info structure
type ServerInfo struct {
	currentVbuckets uint32
	MaxVbuckets     uint32
	NumberOfDisk    int16
    ReplicaVbuckets []int
    ckPointMap      map[int]int
}

type Cluster struct {
	ContextMap map[string]*Context //cluster name to context
	ConfigMap  map[string]Config   //cluster name to config
	IpMap      map[string]string   //ip addess to cluster name
	M          sync.RWMutex
}

type Config struct {
	Port         int16
	Vbuckets     uint16
	Replica      int16
	Hash         string
	Capacity     int16
	Servers      []string
	SecondaryIps []string
}

type VbucketServerMap struct {
	VBucketMap    [][]int       `json:"vBucketMap"`
	VBucketMapForward[][]int    `json:"vBucketMapForward"`
	HashAlgorithm string        `json:"hashAlgorithm"`
	NumReplicas   int           `json:"numReplicas"`
	ServerList    []string      `json:"serverList"`
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
    Transfer    []int //vbucket needs to be transferred
}

type VbaEntry struct {
	Source          string
	VbId            []int
	Destination     string
    CheckPoints     []int  //checkpoints corresponding to vbucket ids
    Transfer_VbId   []int
}

type ServerUpDownInfo struct {
	Server  []string
    SecIp   []string
}

type CapacityUpdateInfo struct {
	Server    string
	DiskAlive int16
}

type Vblist struct {
	Master  []int
	Replica []int
}

type VbucketCount struct {
    Master int
    Replica int
}

type VbucketCountBoth struct {
    primary VbucketCount
    secondary VbucketCount
}

//Have a map for ips here.
type Context struct {
	C               Config       //input Config
	V               VBucketInfo  // vbucketMap to send to client
	S               []ServerInfo // per server information
	VbaInfo         map[string]VbaEntry
    SecondaryIpMap  map[int]int
    Maxvbuckets     uint32
    Rebalance       bool
	M       sync.RWMutex
}

func NewContext() *Context {
    ct := &Context{
        SecondaryIpMap : make(map[int]int),
    }
    return ct
}

func NewCluster() *Cluster {
	cl := &Cluster{
		ContextMap: make(map[string]*Context),
		ConfigMap:  make(map[string]Config),
		IpMap:      make(map[string]string),
	}
	return cl
}

func NewServerInfo(max uint32, curr uint32) *ServerInfo {
    si := ServerInfo {
        MaxVbuckets:max,
        currentVbuckets:curr,
        ckPointMap:make(map[int]int),
    }
    return &si
}
