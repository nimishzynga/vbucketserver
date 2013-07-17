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
package net

import (
	"sync"
	"vbucketserver/config"
Net "net"
    "time"
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
	Data                []config.VbaEntry
    RestoreCheckPoints  []int
	HeartBeatTime       int
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

type clientI struct {
    ip string
    index int
    c string
    p func()
}

const (
    CLIENT1 = "127.0.0.1:11211"
    CLIENT2 = "127.0.0.2:11211"
    CLIENT3 = "127.0.0.3:11211"
    CLIENT4 = "127.0.0.4:11211"
    CLIENT5 = "127.0.0.5:11211"
    CLIENT6 = "127.0.0.11:11211"
)

type MetaData struct {
    state int
    active []int
    replica []int
}

type MyConn struct {
    w,r chan []byte
    ip string
    m *MetaData
    index int
    t time.Duration
    moxi bool
    Net.Conn
}

type Conn Net.Conn

type addVbuc struct {
    l sync.RWMutex
    vbList []int
}

type handler interface {
    handleInit(*MyConn)
    handleConfig(*RecvMsg, *MyConn)
}

type moxiClient clientI
type vbaClient clientI


