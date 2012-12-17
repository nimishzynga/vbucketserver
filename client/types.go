package client

import (
	"net"
	"sync"
	"vbucketserver/conf"
)

//file contains the struct for client handling

//information per client structure
type ClientInfo struct {
	C     chan string
	State int
	Conn  net.Conn
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

type Vblist struct {
	Master  []int
	Replica []int
}

//parse the client message into it
type RecvMsg struct {
	Cmd      string
	Agent    string
	Status   string
	Server   string
	Vbuckets Vblist
	Capacity int
}

type InitMsg struct {
	Cmd string
}

type ConfigMsg struct {
	Cmd           string
	Data          conf.VBucketInfo
	HeartBeatTime int
}

type ConfigVbaMsg struct {
	Cmd           string
	Data          []conf.VbaEntry
	HeartBeatTime int
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
		Cond:     sync.NewCond(new(sync.Mutex)),
		Moxi:    NewClientInfoMap(),
		Vba:     NewClientInfoMap(),
		Cli:     NewClientInfoMap(),
	}
	return cl
}
