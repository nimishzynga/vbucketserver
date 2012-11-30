package clientHandler

import (
    "sync"
    "vbucketserver/conf"
)

//file contains the struct for client handling

//information per client structure
type ClientInfo struct {
	C     chan []byte
	State int
}

//information for all the clients of a particular type
type ClientInfoMap struct {
	Mu sync.RWMutex
	Ma map[string]*ClientInfo
}

//information of all the clients
type Client struct {
	Started bool
	Con     sync.Cond
	Moxi,Vba, Cli   ClientInfoMap
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
	Vbuckets Vblist
}

type InitMsg struct {
	Cmd string
}

type ConfigMsg struct {
	Config  conf.VBucketInfo
	HeartBeatTime int
}


