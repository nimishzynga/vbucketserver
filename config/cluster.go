package config

import (
	"log"
	"sync"
)

type Cluster struct {
	Name          string
	NumVbuckets   uint16
	NumReplica    uint16
	ClientHashing string
	VbucketMap    []*Vbucket
	ServerMap     map[string]*Server
	mu            *sync.RWMutex
}

type Vbucket struct {
	Id    uint16
	State uint8
	Srv   *Server
}

type Server struct {
	Addr     string
	Capacity uint8
	Az       string
}

type ClusterManager struct {
	Clusters map[string]*Cluster
}

func NewCluster(name string, vbcount uint16, replicas uint16, hash string) (cluster *Cluster) {
	if vbcount == 0 {
		log.Fatal("Vbucket count should be non-zero!")
	}
	cluster = &Cluster{
		Name:          name,
		NumVbuckets:   vbcount,
		NumReplica:    replicas,
		ClientHashing: hash,
		VbucketMap:    make([]*Vbucket, vbcount),
		ServerMap:     make(map[string]*Server),
	}

	return
}

func (cluster *Cluster) AddUpdateServer(addr string, capacity uint8, availzone string) {
	server := &Server{
		Addr:     addr,
		Capacity: capacity,
		Az:       availzone,
	}
	cluster.mu.Lock()
	cluster.ServerMap[addr] = server
	cluster.mu.Unlock()
}
