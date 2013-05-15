package net

import (
	"sync"
	"vbucketserver/config"
)

//file contains the struct for client handling

//information per client structure
type ClientInfo struct {
	C     chan string
	W     chan string //wait channel
	State int
}

//information for all the clients of a particular type
type ClientInfoMap struct {
	Mu sync.RWMutex
	Ma map[string]*ClientInfo
}

//information of all the clients
type Client struct {
	Started        bool
	Cond           *sync.Cond
	Moxi, Vba, Cli ClientInfoMap
}

//parse the client message into it
type RecvMsg struct {
	Cmd      string
	Agent    string
	Status   string
	Server   string
    Detail   []string
	Vbuckets config.Vblist
	Capacity int
    CheckPoints config.Vblist
    Destination string
    DisksFailed int
}

type InitMsg struct {
	Cmd string
}

type ClusterVbucketMap struct {
	Buckets []config.VBucketInfo `json:"buckets"`
}

type ConfigMsg struct {
	Cmd           string
	Data          ClusterVbucketMap
	HeartBeatTime int
}

type ConfigVbaMsg struct {
	Cmd                 string
	Data                []config.VbaEntry
    RestoreCheckPoints  []int
	HeartBeatTime       int
}

func NewClientInfoMap() ClientInfoMap {
	k := ClientInfoMap{
		Ma: make(map[string]*ClientInfo),
	}
	return k
}

func NewClient() *Client {
	cl := &Client{
		Started: false,
		Cond:    sync.NewCond(new(sync.Mutex)),
		Moxi:    NewClientInfoMap(),
		Vba:     NewClientInfoMap(),
		Cli:     NewClientInfoMap(),
	}
	return cl
}
