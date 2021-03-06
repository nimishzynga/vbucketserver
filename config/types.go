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
package config

import (
	"sync"
    "time"
)

//per server info structure
type ServerInfo struct {
	currentVbuckets uint32
	MaxVbuckets     uint32
	NumberOfDisk    int16
    ReplicaVbuckets []int
    ActiveVbuckets  []int
    ckPointMap      map[int]int
}

type Cluster struct {
	ContextMap map[string]*Context //cluster name to context
    ConfigMap  map[string]Config  //cluster name to config
    IpMap      map[string]string  //ip addess to cluster name
    State      string
    ActiveIp   string
    M          sync.RWMutex
}

type Config struct {
	Port         uint16
	Vbuckets     uint32
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
	Port uint16           `json:"port"`
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
    Capacity int
}

type CapacityUpdateInfo struct {
	Server    string
	DiskAlive int16
}

type Vblist struct {
	Active  []int
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

type callBackInfo struct {
    vbMap   map[int]int
    count   int
    data    interface{}
    cb      func(interface{}) interface{}
}

type FailureInfo struct {
    F []FailureEntry
    M sync.RWMutex
}

type ReshardInfo struct {
    status int
    dvi *[]DeadVbucketInfo
}

type ConfigInfo struct {
	V               VBucketInfo  // vbucketMap to send to client
	S               []ServerInfo // per server information
	VbaInfo         map[string]VbaEntry
    C               Config      //input Config
}

type Context struct {
    ConfigInfo
    SecondaryIpMap  map[int]int     `json:"-"`
    FailedNodes     map[string]int  `json:"-"`
    Maxvbuckets     uint32
    Rebalance       bool
    NotifyServers   map[string]int  `json:"-"`
    NodeFi          FailureInfo     `json:"-"`
    RepFi           FailureInfo     `json:"-"`
    MoxiFi          FailureInfo     `json:"-"`
    T               time.Time       `json:"-"`
    Cbi             *callBackInfo   `json:"-"`
    ReInfo          ReshardInfo     `json:"-"`
    M,InfoMutex     sync.RWMutex    `json:"-"`
}

type FailureEntry struct {
    Src            string
    Dst            string
    Verified       bool
    vb             Vblist
}

type Params struct {
    State     string
    LogLevel  int
}

func NewContext() *Context {
    ct := &Context{
        SecondaryIpMap : make(map[int]int),
        FailedNodes    : make(map[string]int),
        T  :  time.Now(),
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

func NewCallBackInfo(fn func(interface{}) interface{}) *callBackInfo {
    cbi := &callBackInfo{
        vbMap : make(map[int]int),
        cb : fn,
    }
    return cbi
}
