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
package main

import (
	"vbucketserver/log"
	"sort"
	"testing"
	"vbucketserver/config"
)

func verifyMap(c *config.Context, t *testing.T) {
	m := c.V.Smap.VBucketMap
	serArrActive := make([][]int, len(c.V.Smap.ServerList))
	serArrReplica := make([][]int, len(c.V.Smap.ServerList))
	for i := range m {
		for j := range m[i] {
			if j == 0 {
				serArrActive[m[i][j]] = append(serArrActive[m[i][j]], i)
			} else {
				serArrReplica[m[i][j]] = append(serArrReplica[m[i][j]], i)
			}
		}
	}

	for i := range serArrActive {
		logger.Debugf("Active :for i is", i, serArrActive[i])
	}
	for i := range serArrReplica {
		logger.Debugf("Replica:for i is", i, serArrReplica[i])
	}
	for i := range serArrActive {
		for j := range serArrActive[i] {
			for k := range serArrReplica[i] {
				if serArrReplica[i][k] == serArrActive[i][j] {
					t.Fail()
				}
			}
		}
	}
	sortedActive := []int{}
	sortedReplica := []int{}
	for i := range serArrActive {
		sortedActive = append(sortedActive, serArrActive[i]...)
	}
	for i := range serArrReplica {
		sortedReplica = append(sortedReplica, serArrReplica[i]...)
	}
	sort.Ints(sortedActive)
	sort.Ints(sortedReplica)
	for i := 0; i < len(sortedActive); i++ {
		if sortedActive[i] != sortedReplica[i] {
			t.Fail()
		}
	}
}

func getVbuckets(c *config.Context) ([][]int, [][]int) {
	m := c.V.Smap.VBucketMap
	serArrActive := make([][]int, len(c.C.Servers))
	serArrReplica := make([][]int, len(c.C.Servers))
	for i := range m {
		for j := range m[i] {
			if j == 0 {
				serArrActive[m[i][j]] = append(serArrActive[m[i][j]], i)
			} else {
				serArrReplica[m[i][j]] = append(serArrReplica[m[i][j]], i)
			}
		}
	}
	return serArrActive, serArrReplica
}

func getContext() *config.Config {
	Servers := []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"}
	Sip := []string{"4.4.4.4", "5.5.5.5", "6.6.6.6"}
	c := &config.Config{11111, 32, 1, "CRC", 70, Servers, Sip}
	return c
}

func Test_handleDown(t *testing.T) {
    c := getContext()
	cp := config.NewContext()
    cp.GenMap("test", c)
    entry := config.FailureEntry{
        Src : "2.2.2.2",
        Dst : "2.2.2.2",
    }
    cp.NodeFi.F = append(cp.NodeFi.F, entry)
    entry = config.FailureEntry{
        Src : "1.1.1.1",
        Dst : "2.2.2.2",
    }
    cp.NodeFi.F = append(cp.NodeFi.F, entry)

    entry = config.FailureEntry{
        Src : "3.3.3.3",
        Dst : "2.2.2.2",
    }
    cp.NodeFi.F = append(cp.NodeFi.F, entry)

     entry= config.FailureEntry{
        Src : "1.1.1.1",
        Dst : "2.2.2.2",
    }
    cp.NodeFi.F = append(cp.NodeFi.F, entry)
    ok, out := cp.HandleDown()
    if ok {
        logger.Debugf("list is", out)
    }
}


func Test_GenMap(t *testing.T) {
	c := getContext()
	ct := config.NewContext()
	ct.GenMap("test", c)
	verifyMap(ct, t)
}

func Test_HandleServerDown(t *testing.T) {
	c := getContext()
	ct := config.NewContext()
	ct.GenMap("test", c)
	_, mp := ct.HandleServerDown([]string{"1.1.1.1"})
	for val := range ct.S {
		logger.Debugf(ct.S[val].ReplicaVbuckets)
	}
	logger.Debugf("push config is", mp)
}

/*
func Test_HandleDeadVbuckets(t *testing.T) {
    c := getContext()
    ct := config.NewContext()
    ct.GenMap("test", c)
    activeMap, replicaMap := getVbuckets(ct)
    d := config.DeadVbucketInfo{}
    for i:= range activeMap[0] {
        d.Active = append(d.Active, activeMap[0][i])
    }

    for i:= range replicaMap[0] {
        d.Replica = append(d.Replica, replicaMap[0][i])
    }
    ct.HandleDeadVbuckets(d, c.Servers[0], false)
    verifyMap(ct, t)
}
*/
func Test_NeedRebalance(t *testing.T) {
	c := getContext()
	cp := config.NewContext()
	cp.GenMap("test", c)
	cp.HandleServerDown([]string{"1.1.1.1"})
	verifyMap(cp, t)
	cp.V.Smap.ServerList = append(cp.V.Smap.ServerList, "5.5.5.5")
	cp.S = append(cp.S, config.ServerInfo{})
	cp.Rebalance = true
	cp.NeedRebalance(len(cp.V.Smap.ServerList) - 1)
}

func Test_HandleCapacityUpdate(t *testing.T) {
	c := getContext()
	cp := config.NewContext()
	cp.GenMap("test", c)
	capa := config.CapacityUpdateInfo{"1.1.1.1", 1}
	cp.HandleCapacityUpdate(capa)
}

func Benchmark_GenMap(b *testing.B) {
	c := getContext()
	ct := config.NewContext()
	for i := 0; i < b.N; i++ {
		ct.GenMap("test", c)
	}
}
