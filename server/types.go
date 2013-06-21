/*
Copyright 2013 Zynga Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package server

import (
	"sync"
net "vbucketserver/net"
	"vbucketserver/config"
)

//file contains the struct for client handling

//information per client structure
type ClientInfo struct {
	C     chan string
	W     chan string //wait channel
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
