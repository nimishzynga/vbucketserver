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
    "vbucketserver/log"
	"math/rand"
	"strings"
)

const (
	DEAD_NODE_IP     = "0.0.0.0:11211"
	REPLICA_RESTORE  = -2
	RESHARD_CONT     = 1
	RESHARD_DONE     = 0
	MAX_FAIL_TIME    = 30
	RESHARD_CONT_STR = "In progress"
	RESHARD_DONE_STR = "No resharding"
    LOCALHOST        = "127.0.0.1:11211"
    ACTIVE           = "active"
    MOXI_FAIL_WEIGHT = 1
    REP_FAIL_WEIGHT  = 1
    NODE_FAIL_WEIGHT = 3
    VBS_VERIFY_WEIGHT= 2*NODE_FAIL_WEIGHT
    FAIL_WEIGHT      = 3*NODE_FAIL_WEIGHT
    MAX_CKPOINT_DIFF = 2
    INDEX_UNASSIGNED = -1
    STATE_REPLICA = "Replica"
    STATE_ACTIVE  = "Active"
)

var logger *log.SysLog

func SetLogger(l *log.SysLog) {
    logger = l
}

//return the secondary ip given any ip
//return the same ip, if it is secondary
func (cp *Context) GetSecondaryIp(ip string) string {
	index := cp.getServerIndex(ip)
	if in := cp.getSecondaryIndex(index); in != -1 {
		return cp.V.Smap.ServerList[in]
	}
	return ""
}

func (cp *Context) getSecondaryIndex(i int) int {
	if index, ok := cp.SecondaryIpMap[i]; ok {
		if index == -1 {
			return i
		}
		if index < i {
			return i
		}
		return index
	}
	return -1
}

//return the primary ip
//return the same ip, if it is primary
func (cp *Context) GetPrimaryIp(ip string) string {
	index := cp.getServerIndex(ip)
	if in := cp.getPrimaryIndex(index); in != -1 {
		return cp.V.Smap.ServerList[in]
	}
	return ""
}

func (cp *Context) getTransferIndex(vb int) int {
    if cp.ReInfo.dvi != nil {
        for _,val := range *cp.ReInfo.dvi {
            for _,v := range val.Active {
                if v == vb {
                    return cp.getServerIndex(val.Server)
                }
            }
        }
    }
    return -1
}

func (cp *Context) getRestoreIndex(vb int) int {
    for _,val := range *cp.ReInfo.dvi {
        for _,v := range val.Replica {
            if v == vb {
                return cp.getServerIndex(val.Server)
            }
        }
    }
    return -1
}

func (cp *Context) getPrimaryIndex(i int) int {
	if index, ok := cp.SecondaryIpMap[i]; ok {
		if index == -1 {
			return i
		}
		if index < i {
			return index
		}
		return i
	}
	return -1
}

func (cp *Context) getIndex(s int, arr []VbucketCountBoth, isactive bool) int {
	pri := arr[s].primary
	sec := arr[s].secondary
	if isactive {
		if pri.Master > sec.Master {
			arr[s].secondary.Master++
			return cp.getSecondaryIndex(s)
		}
		arr[s].primary.Master++
		return cp.getPrimaryIndex(s)
	} else {
		if pri.Replica > sec.Replica {
			arr[s].secondary.Replica++
			return cp.getSecondaryIndex(s)
		}
		arr[s].primary.Replica++
		return cp.getPrimaryIndex(s)
	}
	return 0
}

//generate map using the actual number of servers.Not uses the dual interfaces
//in account
func (cp *Context) generatevBucketMap(c *Config) (*[][]int, *[]uint32, bool) {
	serv := len(c.Servers)
	logger.Debugf("Number of servers are", serv)
	capacityMap := make([]uint32, len(c.Servers))
	maxActive := int(c.Vbuckets) / serv
	logger.Debugf("maxActive is", maxActive, c.Vbuckets)
	countVbuckets := make([]VbucketCountBoth, len(c.Servers))
	if int(c.Vbuckets)%serv > 0 {
		maxActive += 1
	}
	maxReplica := maxActive * int(c.Replica)
	countReplica := make([]int, serv)
	if serv <= int(c.Replica) {
		return nil, nil, true
	}
	confMap := make([][]int, c.Vbuckets)

	for i := range confMap {
		confMap[i] = make([]int, c.Replica+1)
	}

	lastserver := -1
	//distribute the active vbuckets
	for i := 0; i < int(c.Vbuckets); i++ {
		s := i / maxActive
		confMap[i][0] = cp.getIndex(s, countVbuckets, true)
		capacityMap[s]++
		//distribute the replicas
		for j := 1; j <= int(c.Replica); j++ {
			for {
				lastserver++
				lastserver = lastserver % serv
				if lastserver == s {
					continue
				}
				if c := countReplica[lastserver]; c >= maxReplica {
					continue
				} else {
					c++
					countReplica[lastserver] = c
				}
				confMap[i][j] = cp.getIndex(lastserver, countVbuckets, false)
				capacityMap[lastserver]++
				break
			}
		}
	}
	logger.Debugf("conf map is", confMap)
	return &confMap, &capacityMap, false
}

func (cp *Context) generateVBAmap() {
	m := cp.V.Smap.VBucketMap
	logger.Debugf("map is", m)
	serverList := cp.V.Smap.ServerList
	vbaMap := make(map[string]VbaEntry)
	var entry VbaEntry
	var ok bool
	for i := range m {
		server := serverList[m[i][0]]
		if cp.V.Smap.NumReplicas == 0 {
			hashKey := server
			if entry, ok = vbaMap[hashKey]; ok {
				entry.VbId = append(entry.VbId, i)
			} else {
				entry = VbaEntry{
					Source:      server,
					VbId:        []int{i},
					Destination: "",
				}
			}
			vbaMap[hashKey] = entry
		} else {
			for j := 1; j < len(m[i]); j++ {
				hashKey := server + serverList[m[i][j]]
				if entry, ok = vbaMap[hashKey]; ok {
					entry.VbId = append(entry.VbId, i)
				} else {
					entry = VbaEntry{
						Source:      server,
						VbId:        []int{i},
						Destination: serverList[m[i][j]],
					}
				}
				vbaMap[hashKey] = entry
			}
		}
	}
	cp.VbaInfo = vbaMap
	//logger.Debugf("generateVBAmap : vba info is", cp.VbaInfo)
}

func (cp *Context) parseIps(cfg *Config) int {
	//populate the map in cp and append cp.V.Smap.ServerList
	// with servers and secondary ips 
	count := len(cp.V.Smap.ServerList)
	start := count
	cp.V.Smap.ServerList = append(cp.V.Smap.ServerList, cfg.Servers...)
	sec := 0
	for i, _ := range cfg.Servers {
		if len(cfg.SecondaryIps) > i && cfg.SecondaryIps[i] != "" {
			cp.SecondaryIpMap[sec+len(cfg.Servers)+ start] = count
			cp.SecondaryIpMap[count] = sec + start + len(cfg.Servers)
			cp.V.Smap.ServerList = append(cp.V.Smap.ServerList, cfg.SecondaryIps[i])
			sec++
		} else {
			cp.SecondaryIpMap[count] = -1
		}
		count++
	}
	return sec
}

func (cp *Context) GenMap(cname string, cfg *Config) {
	sec := cp.parseIps(cfg)
	logger.Debugf("config is", cfg)
	if rv, cm, err := cp.generatevBucketMap(cfg); err == false {
		cp.M.Lock()
		defer cp.M.Unlock()
		cp.V.Smap.VBucketMap = *rv
		cp.V.Smap.HashAlgorithm = cfg.Hash
		cp.V.Port = cfg.Port
		cp.V.Smap.NumReplicas = int(cfg.Replica)
		logger.Debugf("serverlist is", cfg.Servers)
		//cp.V.Smap.ServerList = cfg.Servers
		cp.V.Name = cname
		cp.C = *cfg //update the cfgfig
		cp.generateVBAmap()
		cp.copyVbucketMap() //create the vbucketmap forward
		logger.Debugf("capacity is", cfg.Capacity)
		cp.updateMaxCapacity(cfg.Capacity, len(cfg.Servers), cm, sec)
		//logger.Debugf("updated map ", cp.V)
	} else {
		logger.Fatalf("failed to generate config map ")
	}
}

func (cp *Context) updateMaxCapacity(capacity int16, totServers int,
	cm *[]uint32, sec int) {
	var cc uint32 = uint32(float32(uint32(cp.C.Replica+1)*uint32(cp.C.Vbuckets))*
		(1+(float32(capacity)/100))) / uint32(totServers)
	for i := 0; i < totServers; i++ {
		c := *NewServerInfo(cc, (*cm)[i])
		logger.Debugf("Capacity , server :", cc, i)
		cp.S = append(cp.S, c)
	}
	for k := 0; k < sec; k++ {
		cp.S = append(cp.S, *NewServerInfo(0, 0))
	}
	cp.Maxvbuckets = cc
}

func (cp *Context) SameServer(index1 int, index2 int) bool {
	return cp.getPrimaryIndex(index1) == cp.getPrimaryIndex(index2)
}

//return the free server
func (cp *Context) findFreeServer(s []int, s2 []int, s3 []int, lastindex *int) int {
	var arr []int
	logger.Debugf("in findFreeServer: s3 is", s3)
	if len(s3) == 0 {
		arr = make([]int, len(cp.V.Smap.ServerList))
		for i := 0; i < len(cp.V.Smap.ServerList); i++ {
			arr[i] = i
		}
	} else {
		arr = s3
	}
	s2 = append(s2, s...)
	for _, j := range s2 {
		for k := 0; k < len(arr); k++ {
			if cp.SameServer(arr[k], j) {
				logger.Debugf("removing", arr[k], j)
				arr = append(arr[:k], arr[k+1:]...)
			}
		}
	}
	logger.Debugf("lastindex is", lastindex)
	logger.Debugf("arr is", arr)
    if *lastindex  == INDEX_UNASSIGNED {
        *lastindex = int(rand.Int31n(int32(len(arr))))
    }
	//returnIndex = 0
	for k := 0; k < len(arr); k++ {
        *lastindex = (*lastindex+1) % len(arr)
        i := arr[*lastindex]
		serInfo := cp.S[cp.getPrimaryIndex(i)]
		if serInfo.currentVbuckets < serInfo.MaxVbuckets {
			cp.S[i].currentVbuckets++
			return i
		} else {
			logger.Debugf("failed current and max ,index", serInfo.currentVbuckets, serInfo.MaxVbuckets, i)
		}
	}
	//Need to have a list of vbuckets for which server is not available
	logger.Fatalf("freeserver not found for ", cp.S)
	cp.Rebalance = true
	return -1
}

func (cp *Context) reduceCapacity(s int, n int, c uint32) {
	if n == 0 {
		cp.S[s].MaxVbuckets = 0
		cp.S[s].currentVbuckets = 0
	} else {
		if cp.S[s].NumberOfDisk == 0 {
			logger.Debugf("Disk is Zero for", s)
			return
		}
		cp.S[s].MaxVbuckets -= (uint32(n) * cp.S[s].MaxVbuckets) / uint32(cp.S[s].NumberOfDisk)
		cp.S[s].currentVbuckets -= c
	}
}

func (cp *Context) getServerVbuckets(s int) *DeadVbucketInfo {
	vbaMap := cp.V.Smap.VBucketMap
	priIndex := cp.getPrimaryIndex(s)
	secIndex := cp.getSecondaryIndex(s)
    logger.Debugf("inside getserverbucket", priIndex,secIndex)
	dvi := new(DeadVbucketInfo)
	for i := 0; i < len(vbaMap); i++ {
		for j := 0; j < len(vbaMap[i]); j++ {
			if vbaMap[i][j] == priIndex || vbaMap[i][j] == secIndex {
				if j == 0 {
					dvi.Active = append(dvi.Active, i)
				} else {
					dvi.Replica = append(dvi.Replica, i)
				}
			}
		}
	}
	return dvi
}

func (cp *Context) checkFail(ser string) bool {
	t := cp.T.Second()
	if val, ok := cp.FailedNodes[ser]; ok {
		if t-val > MAX_FAIL_TIME {
			cp.FailedNodes[ser] = t
			return false
		}
		delete(cp.FailedNodes, ser)
		return true
	}
	cp.FailedNodes[ser] = t
	return false
}

func (cp *Context) getRestoreVBuckets(ip string) []int {
    index := cp.getServerIndex(cp.GetPrimaryIp(ip))
    if index == -1 {
        return nil
    }
    return cp.S[index].ReplicaVbuckets
}

func (cp *Context) HandleServerDown(ser []string) (bool, map[string]VbaEntry) {
    logger.Warnf("FAILING NODE:", ser)
	cp.M.Lock()
	dvil := make([]DeadVbucketInfo, len(ser))
	for i := range ser {
		dvil[i] = *cp.getServerVbuckets(cp.getServerIndex(ser[i]))
        restoreVbs := cp.getRestoreVBuckets(ser[i])
        dvil[i].Replica = append(dvil[i].Replica, restoreVbs...)
		dvil[i].Server = ser[i]
	}
	cp.M.Unlock()
	return cp.HandleDeadVbuckets(dvil, ser, true, nil, true)
}

func (cp *Context) RemoveServerInfo(priIps []string, secIps []string) {
	for k := 0; k < len(priIps); k++ {
		cp.S[cp.getServerIndex(priIps[k])].MaxVbuckets = 0
	}
}

func (cp *Context) changeNewCapacity(capacity int) {
    if capacity <= 0 {
        return
    }
	totServers := len(cp.V.Smap.ServerList)
	var cc uint32 = uint32(float32(uint32(cp.C.Replica+1)*uint32(cp.C.Vbuckets))*
		(1+(float32(capacity)/100))) / uint32(totServers)
	for k := 0; k < totServers; k++ {
		cp.S[k].MaxVbuckets = cc
	}
	cp.Maxvbuckets = cc
}

func (cp *Context) HandleReshardDown(ser []string, c int) (bool, map[string]VbaEntry) {
	cp.M.Lock()
	dvil := make([]DeadVbucketInfo, len(ser))
	for i, serv := range ser {
		dvil[i] = *cp.getServerVbuckets(cp.getServerIndex(serv))
		dvil[i].Transfer = dvil[i].Active
		dvil[i].Active = dvil[i].Active[:0]
		dvil[i].Server = serv
	}
    cp.ReInfo.dvi = &dvil
	cp.RemoveServerInfo(ser, nil)
	cp.changeNewCapacity(c)
	cp.M.Unlock()
	return cp.HandleDeadVbuckets(dvil, nil, false, nil, true)
}

func (cp *Context) handleNoReplicaFailure(dvi DeadVbucketInfo, ser int) (bool, map[string]VbaEntry) {
	oldVbaMap := cp.VbaInfo
	serverList := cp.V.Smap.ServerList
	vbucketMa := cp.V.Smap.VBucketMap
	changeVbaMap := make(map[string]VbaEntry)
    lastindex := INDEX_UNASSIGNED

	for i := 0; i < len(dvi.Active); i++ {
		vbucket := vbucketMa[dvi.Active[i]]
		key := serverList[ser]
		oldEntry := oldVbaMap[key]
		//delete the replica vbucket from the vba map
		for r := 0; r < len(oldEntry.VbId); r++ {
			if oldEntry.VbId[r] == dvi.Active[i] {
				oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
				if len(oldEntry.VbId) == 0 {
					delete(oldVbaMap, key)
				} else {
					changeVbaMap[key] = oldEntry
					oldVbaMap[key] = oldEntry
				}
				break
			}
        }
        logger.Debugf("vbucket ", vbucket, "ser is ", ser)
        serverIndex := cp.findFreeServer(vbucket, []int{ser}, nil, &lastindex)
        if serverIndex != -1 {
            logger.Debugf("j is, new server is", 0, serverIndex)
            key := serverList[serverIndex]
            oldEntry := oldVbaMap[key]
            oldEntry.VbId = append(oldEntry.VbId, dvi.Active[i])
            changeVbaMap[key] = oldEntry
            oldVbaMap[key] = oldEntry
            vbucketMa[dvi.Active[i]][0] = serverIndex
        } else {
            logger.Fatalf("CRITICAL:Not enough capacity for active vbuckets")
            break
        }
    }
	cp.VbaInfo = oldVbaMap
	logger.Debugf("new vbucket map was", vbucketMa)
	return true, changeVbaMap
}

func (cp *Context) HandleTransferVbuckets(changeVbaMap map[string]VbaEntry, dvi DeadVbucketInfo,
	allFailedIndex []int, allNewIndex []int) {
	logger.Debugf("Inside HandleTransferVbuckets")
	//transfer should be affected in FFT
	oldVbaMap := cp.VbaInfo
	vbucketMa := cp.V.Smap.VBucketMap
	serverList := cp.V.Smap.ServerList
    lastindex := INDEX_UNASSIGNED
	for i := range dvi.Transfer {
		logger.Debugf("inside transfer for loop")
		vbucket := vbucketMa[dvi.Transfer[i]]
        //need to append for all replica
        index := cp.getRestoreIndex(dvi.Transfer[i])
        failedIndex := allFailedIndex
        func() {
            if index != -1 {
                for j:=0;j<len(failedIndex);j++ {
                    if failedIndex[j] == index {
                        return
                    }
                }
                failedIndex = append(failedIndex, index)
                return
            }
        }()
        ser := cp.findFreeServer(vbucket, failedIndex, allNewIndex, &lastindex)
        if ser == -1 {
            logger.Errorf("Not enough capacity in destination to transfer vbucket", allNewIndex)
            continue
        }
		//add the new transfer entry
		logger.Debugf("server index are", vbucket[0], ser, serverList)
		key := serverList[vbucket[0]] + serverList[ser]
		oldEntry, ok := oldVbaMap[key]
		if ok == false {
			oldEntry.Source = serverList[vbucket[0]]
			oldEntry.Destination = serverList[ser]
		}
		oldEntry.Transfer_VbId = append(oldEntry.Transfer_VbId, dvi.Transfer[i])
		changeVbaMap[key] = oldEntry
		oldVbaMap[key] = oldEntry
        cp.V.Smap.VBucketMapForward[dvi.Transfer[i]][0] = ser
	}
}

func (cp *Context) VerifyCheckPoints(s int, d int, v int) bool {
    s = cp.getPrimaryIndex(s)
    d = cp.getPrimaryIndex(d)
    ckdiff := cp.S[s].ckPointMap[v] - cp.S[d].ckPointMap[v]
	if (cp.S[d].ckPointMap[v] == 0 && ckdiff > 0) || ckdiff > MAX_CKPOINT_DIFF {
		logger.Errorf("check point not matching source destination vbucket checkpoints",
            s, d, v, cp.S[s].ckPointMap[v], cp.S[d].ckPointMap[v])
		return false
	}
	return true
}

func (cp *Context) HandleDeadVbuckets(dvil []DeadVbucketInfo, sl []string, serverDown bool,
	newServerList []string, capHandle bool) (bool, map[string]VbaEntry) {
	cp.M.Lock()
	defer cp.M.Unlock()
	oldVbaMap := cp.VbaInfo
	serverList := cp.V.Smap.ServerList
	vbucketMa := cp.V.Smap.VBucketMap
	changeVbaMap := make(map[string]VbaEntry)
	logger.Debugf("input server list is", sl)
	allFailedIndex := []int{}
	allNewIndex := []int{}
    lastindexActive := INDEX_UNASSIGNED
    lastindexReplica := INDEX_UNASSIGNED
	for _, i := range newServerList {
		allNewIndex = append(allNewIndex, cp.getServerIndex(i))
	}
	if len(newServerList) > 0 {
		for _, i := range dvil {
			allFailedIndex = append(allFailedIndex, cp.getServerIndex(i.Server))
		}
	}
	for i := range dvil {
		dvi := dvil[i]
		s := dvil[i].Server
		ser := cp.getServerIndex(cp.GetPrimaryIp(s))
		if len(newServerList) == 0 {
			allFailedIndex = []int{ser}
		}
		if ser == -1 {
			logger.Warnf("Dead server ",dvil[i].Server, " is not in server list")
			return false, changeVbaMap
		} else if serverDown == false {
			d := cp.getServerVbuckets(ser)
			for i := range dvi.Active {
				for j := range d.Active {
					if dvi.Active[i] != d.Active[j] {
						j++
						if j == len(d.Active) {
							logger.Errorf("Invalid active vbuckets", dvi.Active, d.Active)
							return false, nil
						}
					} else {
						break
					}
				}
				i++
			}
			for i := range dvi.Replica {
				for j := range d.Replica {
					if dvi.Replica[i] != d.Replica[j] {
						j++
						if j == len(d.Replica) {
							logger.Errorf("Invalid replica vbuckets", dvi.Replica, d.Replica)
							return false, nil
						}
					} else {
						break
					}
				}
				i++
			}
		}
		logger.Debugf("Failed vbuckets :", dvi)
		logger.Debugf("old vbucket map was", vbucketMa)
		if capHandle {
			cp.reduceCapacity(ser, dvi.DisksFailed, uint32(len(dvi.Active)+len(dvi.Replica)))
		}

		if cp.V.Smap.NumReplicas == 0 {
			_, changeVbaMap = cp.handleNoReplicaFailure(dvi, ser)
		} else {
			//handle the transfer part
			cp.HandleTransferVbuckets(changeVbaMap, dvi, allFailedIndex, allNewIndex)

			//handle the active vbuckets 
			for i := 0; i < len(dvi.Active); i++ {
				vbucket := vbucketMa[dvi.Active[i]]
				for k := 1; k < len(vbucket); k++ {
					//TODO:Need to verify if this condition is needed
					if vbucket[k] < 0 {
						continue
					}
					key := serverList[vbucket[0]] + serverList[vbucket[k]]
					oldEntry := oldVbaMap[key]
					for r := 0; r < len(oldEntry.VbId); r++ {
						if oldEntry.VbId[r] == dvi.Active[i] {
							oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
							break
						}
					}
					if len(oldEntry.VbId) == 0 && len(oldEntry.Transfer_VbId) == 0 {
						delete(oldVbaMap, key)
					} else {
						oldVbaMap[key] = oldEntry
					}
					changeVbaMap[key] = oldEntry
				}
				k := 1
				for ; k < len(vbucket); k++ {
					if vbucketMa[dvi.Active[i]][k] != -1 {
						break
					}
				}
				if k == len(vbucket) {
					logger.Fatalf("CRITICAL:Not enough capacity for active vbuckets")
					return true, changeVbaMap
				}
				if vbucketMa[dvi.Active[i]][k] == REPLICA_RESTORE ||
					cp.VerifyCheckPoints(vbucket[0], vbucket[k], dvi.Active[i]) == false {
					logger.Errorf("Need manual restore for vbucket", dvi.Active)
					continue
				}
				vbucketMa[dvi.Active[i]][0] = vbucketMa[dvi.Active[i]][k]
				serverIndex := cp.findFreeServer(vbucketMa[dvi.Active[i]], allFailedIndex,
                    allNewIndex, &lastindexActive)
                if serverIndex == -1 {
                    logger.Fatalf("Not enough capacity")
                    continue
                }
				vbucketMa[dvi.Active[i]][k] = REPLICA_RESTORE
				cp.S[cp.getPrimaryIndex(serverIndex)].ReplicaVbuckets =
					append(cp.S[cp.getPrimaryIndex(serverIndex)].ReplicaVbuckets, dvi.Active[i])
				vbucket = vbucketMa[dvi.Active[i]]
				replicaEntry := VbaEntry{Source: serverList[serverIndex]}
				for k := 1; k < len(vbucket); k++ {
					if vbucket[k] == -1 {
						continue
					}
					key := serverList[vbucket[0]]
					oldEntry, ok := oldVbaMap[key]
					if ok == false {
						oldEntry.Source = serverList[vbucket[0]]
					}
					oldEntry.VbId = append(oldEntry.VbId, dvi.Active[i])
					changeVbaMap[key] = oldEntry
					oldVbaMap[key] = oldEntry
				}
				changeVbaMap[serverList[serverIndex]] = replicaEntry
			}

			//handle the replica part
			for i := 0; i < len(dvi.Replica); i++ {
                in := ser
                if cp.UpdateRestoreVbuckets(s, dvi.Replica[i]) {
                    in = -1
                }
				vbucket := vbucketMa[dvi.Replica[i]]
                count := 2
				for l := 0; l < count; l++ {
					found := false
                    key := ""
                    if in == -1 {
					    key = serverList[vbucket[0]]
                        count=1
                    } else {
					    key = serverList[vbucket[0]] + serverList[in]
                    }
					oldEntry := oldVbaMap[key]
					//delete the replica vbucket from the vba map
					for r := 0; r < len(oldEntry.VbId); r++ {
						if oldEntry.VbId[r] == dvi.Replica[i] {
							oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
							if len(oldEntry.VbId) == 0 && len(oldEntry.Transfer_VbId) == 0 {
								delete(oldVbaMap, key)
							} else {
								changeVbaMap[key] = oldEntry
								oldVbaMap[key] = oldEntry
							}
							found = true
							break
						}
					}
					if found {
						break
					}
					if pin := cp.getSecondaryIndex(in); in == pin {
						break
					} else {
						in = pin
					}
				}

				var j int
				for j = 1; j < len(vbucket); j++ {
					if cp.SameServer(vbucket[j], ser) {
						logger.Debugf("vbucket", vbucket, "j is", j, "ser is", ser)
                        index := cp.getTransferIndex(ser)
                        failedIndex := allFailedIndex
                        func() {
                            if index != -1 {
                                for j:=0;j<len(failedIndex);j++ {
                                    if failedIndex[j] == index {
                                        return
                                    }
                                }
                                failedIndex = append(failedIndex, index)
                                return
                            }
                        }()
                        serverIndex := cp.findFreeServer(vbucket, failedIndex, allNewIndex, &lastindexReplica)
                        if serverIndex == -1 {
							continue
						}
						vbucketMa[dvi.Replica[i]][j] = REPLICA_RESTORE
						cp.S[cp.getPrimaryIndex(serverIndex)].ReplicaVbuckets =
							append(cp.S[cp.getPrimaryIndex(serverIndex)].ReplicaVbuckets, dvi.Replica[i])
						replicaEntry := VbaEntry{Source: serverList[serverIndex]}
						changeVbaMap[serverList[serverIndex]] = replicaEntry
						logger.Debugf("j is, new server is", j, serverIndex)
						key := serverList[vbucket[0]]
						oldEntry, ok := oldVbaMap[key]
						if ok == false {
							oldEntry.Source = serverList[vbucket[0]]
						}
						oldEntry.VbId = append(oldEntry.VbId, dvi.Replica[i])
						changeVbaMap[key] = oldEntry
						changeVbaMap[serverList[serverIndex]] = replicaEntry
						oldVbaMap[key] = oldEntry
					}
				}
			}
		}

		if serverDown {
			logger.Debugf("inside serverDown", ser, cp.V.Smap.ServerList)
			for i := range cp.C.Servers {
				if cp.GetPrimaryIp(cp.C.Servers[i]) == cp.GetPrimaryIp(cp.V.Smap.ServerList[ser]) {
					cp.C.Servers[i] = DEAD_NODE_IP
					break
				}
			}
			cp.V.Smap.ServerList[cp.getPrimaryIndex(ser)] = DEAD_NODE_IP
			cp.V.Smap.ServerList[cp.getSecondaryIndex(ser)] = DEAD_NODE_IP
		}
	}
	logger.Debugf("new vbucket map was", vbucketMa)
	logger.Debugf("changed vba map is", changeVbaMap)
	logger.Debugf("Final vbaMap is", oldVbaMap)
	return true, changeVbaMap
}

func (cp *Context) NeedRebalance(index int) (bool, map[string]VbaEntry) {
	cp.M.Lock()
	defer cp.M.Unlock()
	if len(cp.S) <= index {
		return false, nil
	}
	si := cp.S[index]
	if cp.Rebalance == false || si.currentVbuckets >= si.MaxVbuckets {
		return false, nil
	}
	rebalancePending := false
	entries := false
	changeVbaMap := make(map[string]VbaEntry)
	oldVbaMap := cp.VbaInfo
	vbucketMap := cp.V.Smap.VBucketMap
	found := -1
	vbaMap := cp.V.Smap.VBucketMap
	serverList := cp.V.Smap.ServerList

	for i := 0; i < len(vbaMap); i++ {
		indexFound := false
		for j := 0; j < len(vbaMap[i]); j++ {
			if vbaMap[i][j] == index {
				if found != -1 {
					rebalancePending = true
					break
				}
				found = -1
				indexFound = true
			}
			if vbaMap[i][j] == -1 {
				if indexFound {
					rebalancePending = true
					break
				} else {
					found = j
				}
			}
		}
		if found != -1 {
			entries = true
			vbaMap[i][found] = index
			vbucket := vbucketMap[i]
			key := serverList[vbucket[0]] + serverList[index]
			oldEntry, ok := oldVbaMap[key]
			if ok == false {
				oldEntry.Source = serverList[vbucket[0]]
				oldEntry.Destination = serverList[index]
			}
			oldEntry.VbId = append(oldEntry.VbId, i)
			changeVbaMap[key] = oldEntry
			oldVbaMap[key] = oldEntry
		}
	}
	cp.Rebalance = rebalancePending
	return entries, changeVbaMap
}

func (cp *Context) copyVbucketMap() {
	confMap := make([][]int, cp.C.Vbuckets)
	for i := range confMap {
		confMap[i] = make([]int, cp.C.Replica+1)
	}
	for i := range cp.V.Smap.VBucketMap {
		for j := range cp.V.Smap.VBucketMap[i] {
			confMap[i][j] = cp.V.Smap.VBucketMap[i][j]
		}
	}
	cp.V.Smap.VBucketMapForward = confMap
}

func (cp *Context) AddServerInfo(priIps []string, secIps []string) {
	cfg := &Config{
		Servers:      priIps,
		SecondaryIps: secIps,
	}
	for tot := cp.parseIps(cfg) + len(priIps); tot > 0; tot-- {
		cp.S = append(cp.S, *NewServerInfo(cp.Maxvbuckets, 0))
	}
}

//TODO:Another list for having initial config of servers.So if a server connect
//later, it should be allowed
//TODO:need to protect it by lock
//need to remove the condition that only active or replica can be moved. Need to handle
//case when multiple servers are added
//need to use float for vbucketperserver
func (cp *Context) HandleServerAlive(ser []string, secIp []string, toAdd bool) (bool, map[string]VbaEntry) {
	//cp.M.Lock()
	vbuckets := cp.C.Vbuckets
    totServers := len(cp.C.Servers)
    //count for the dead ips in the list and subtract from the total servers
    for _,val := range cp.C.Servers {
        if val == DEAD_NODE_IP {
            totServers--
        }
    }
	servers := totServers + len(ser)
    //number of vbuckets per server
	vbucketsPerServer := len(ser) * ((int(vbuckets) / (servers)) / totServers)
	dvil := make([]DeadVbucketInfo, len(cp.C.Servers))
	activeVbMap := make(map[int]int)
	for i, serv := range cp.C.Servers {
		dvi := cp.getServerVbuckets(cp.getServerIndex(serv))
		dvil[i].Server = serv
		count := 0
		for j := 0; j < len(dvi.Replica); j++ {
			if activeVbMap[dvi.Replica[j]] == 1 {
				continue
			} else {
			    activeVbMap[dvi.Replica[j]] = 1
            }
			dvil[i].Replica = append(dvil[i].Replica, dvi.Replica[j])
			count++
			if count == vbucketsPerServer {
				break
			}
		}
        count = 0
		for j := 0; j < len(dvi.Active); j++ {
            if activeVbMap[dvi.Active[j]] == 1 {
				continue
			} else {
                count++
			    activeVbMap[dvi.Active[j]] = 1
            }
			dvil[i].Transfer = append(dvil[i].Transfer, dvi.Active[j])
			if count == vbucketsPerServer {
				break
			}
		}
		totVbuckets := len(dvil[i].Transfer) + len(dvil[i].Replica)
		logger.Debugf("HandleServerAlive server is", serv)
		si := cp.S[cp.getServerIndex(serv)]
		si.currentVbuckets += uint32(totVbuckets)
	}
    cp.ReInfo.dvi = &dvil
	if toAdd {
		//TODO:Need to check if duplicate ip is getting added
		cp.C.Servers = append(cp.C.Servers, ser...)
		cp.C.SecondaryIps = append(cp.C.SecondaryIps, secIp...)
	}
	cp.AddServerInfo(ser, secIp)
	logger.Debugf("calling HandleDeadVbuket", dvil, ser)
	return cp.HandleDeadVbuckets(dvil, nil, false, ser, false)
	// cp.M.Unlock()
}

func (cp *Context) getServerIndex(si string) int {
	s := cp.V.Smap.ServerList
	for i := range s {
		if strings.Split(s[i], ":")[0] == strings.Split(si, ":")[0] {
			return i
		}
	}
	logger.Debugf("Index not found.server list is", s, si)
	return -1
}

func (cp *Context) HandleCheckPoint(si string, v Vblist, c Vblist) bool {
	if len(v.Active) != len(c.Active) || len(v.Replica) != len(c.Replica) {
		return false
	}
    if index := cp.getServerIndex(si); index != -1 {
        m := cp.S[cp.getServerIndex(si)].ckPointMap
        for i, k := range v.Active {
            m[k] = c.Active[i]
        }
        for i, k := range v.Replica {
            m[k] = c.Replica[i]
        }
    }
	return true
}

func (cp *Context) HandleCapacityUpdate(ci CapacityUpdateInfo) {
	cp.M.Lock()
	defer cp.M.Unlock()
	i := cp.getServerIndex(ci.Server)
	if i == -1 {
		logger.Errorf("Server not found for capaciy update", ci.Server)
		return
	}
	si := cp.S[i]
	if si.NumberOfDisk == 0 {
		logger.Warnf("Capacity is zero for", ci.Server)
		return
	}
	si.MaxVbuckets += (si.MaxVbuckets * uint32(ci.DiskAlive)) / uint32(si.NumberOfDisk)
	cp.S[i] = si
}

func (cls *Cluster) GetContextFromClusterName(clusterName string) *Context {
	cp, ok := cls.ContextMap[clusterName]
	if ok == false {
        logger.Warnf("cluster:", clusterName, "not in cluster name list", cls.ContextMap,
            "Invalid cluster name in client API")
		return nil
	}
	return cp
}

func (cp *Context) getMasterServer(vb int, s int) int {
	vbucket := cp.V.Smap.VBucketMap[vb]
	for i := range vbucket {
		if vbucket[i] == REPLICA_RESTORE {
			//Put the server index in the map
			vbucket[i] = s
			break
		}
	}
	return vbucket[0]
}

func (cp *Context) UpdateRestoreVbuckets(ip string, vb int) bool {
    if index := cp.getServerIndex(cp.GetPrimaryIp(ip));index != -1 {
        for i:=0; i < len(cp.S[index].ReplicaVbuckets); i++ {
            v := cp.S[index].ReplicaVbuckets[i]
            if v == vb {
                cp.S[index].ReplicaVbuckets = append(cp.S[index].ReplicaVbuckets[:i],
                    cp.S[index].ReplicaVbuckets[i+1:]...)
                return true
            }
        }
    }
    return false
}

func (cp *Context) HandleRestoreCheckPoints(vbl Vblist, ck Vblist, ip string) map[string]int {
	cp.M.Lock()
	serverList := cp.V.Smap.ServerList
	serverToInfo := make(map[string]int)
	logger.Debugf("inside HandleRestoreCheckPoints for ip", ip, vbl.Replica)
	for i, vb := range vbl.Replica {
		src := 0
		if i%2 == 0 {
			src = cp.getServerIndex(cp.GetPrimaryIp(ip))
		} else {
			src = cp.getServerIndex(cp.GetSecondaryIp(ip))
		}

        if cp.UpdateRestoreVbuckets(ip, vb) == false {
            logger.Warnf("Restore vbucket: vbucket", vb, "does not belong to ",ip)
            continue
        }

		//this is putting the vbucket in the map
		ms := cp.getMasterServer(vb, src)
		if src == -1 || ms == -1 {
			logger.Warnf("Error in getting HandleRestoreCheckPoints src ms", src, ms)
			return nil
		}
		key := serverList[ms]
		oldVbaMap := cp.VbaInfo
		oldEntry, ok := cp.VbaInfo[key]
		if ok {
			for r := 0; r < len(oldEntry.VbId); r++ {
				if oldEntry.VbId[r] == vb {
					oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
				}
			}
			if len(oldEntry.VbId) == 0 && len(oldEntry.Transfer_VbId) == 0 {
				delete(oldVbaMap, key)
			} else {
				oldVbaMap[key] = oldEntry
			}
		} else {
			logger.Warnf("No replication key for", key)
		}

		key = serverList[ms] + serverList[src]
		oldEntry, ok = cp.VbaInfo[key]
		if ok == false {
			oldEntry.Source = serverList[ms]
			oldEntry.Destination = serverList[src]
		}
		vbId := []int{vb}
		oldEntry.VbId = append(vbId, oldEntry.VbId...)
		ckp := []int{ck.Replica[i]}
		oldEntry.CheckPoints = append(ckp, oldEntry.CheckPoints...)
		cp.VbaInfo[key] = oldEntry
		serverToInfo[serverList[ms]] = 1
	}
    cp.updateReshardStatus(cp.getServerIndex(ip), vbl)
	cp.M.Unlock()
	logger.Debugf("returing the serverinfo", serverToInfo)
	return serverToInfo
	//return nil
}

//t type of the message
//vb contains the list of the vbuckets which failed in replication fail
func (cp *Context) HandleDown(clear bool, verifyFailFunc func(string, *Context)) (bool, map[string]VbaEntry) {
    fi := &cp.NodeFi
    fi.M.Lock()
    NodeFailed := cp.DecideServer(fi.F, NODE_FAIL_WEIGHT)
    if clear {
        fi.F = fi.F[:0]
    }
    fi.M.Unlock()
    fi = &cp.RepFi
    fi.M.Lock()
    RepFailed := cp.DecideServer(fi.F, REP_FAIL_WEIGHT)
    if clear {
        fi.F = fi.F[:0]
    }
    fi.M.Unlock()
    fi = &cp.MoxiFi
    fi.M.Lock()
    MoxiFailed := cp.DecideServer(fi.F, MOXI_FAIL_WEIGHT)
    if clear {
        fi.F = fi.F[:0]
    }
    fi.M.Unlock()

    for node,weight := range RepFailed {
        w := NodeFailed[node]
        NodeFailed[node] = w + weight
    }
    for node,weight := range MoxiFailed {
        w := NodeFailed[node]
        NodeFailed[node] = w + weight
    }
    NewlyFailedNode := []string{}
    for node,weight := range NodeFailed {
        if weight >= FAIL_WEIGHT {
            NewlyFailedNode = append(NewlyFailedNode, node)
        } else if weight >= VBS_VERIFY_WEIGHT {
            go verifyFailFunc(node, cp)
        }
    }
    if len(NewlyFailedNode) > 0 {
        return cp.HandleServerDown(NewlyFailedNode)
    }
    return false,nil
}

//each contains an entry for failure
func (cp *Context) DecideServer(f []FailureEntry, failWeight int) map[string]int {
	failedServer := make(map[string]int)
	failCount := make(map[string]map[string]int)
    for _, en := range f {
        weight := failWeight
        if en.Dst == LOCALHOST {
            weight = 2*weight
            en.Dst = en.Src
        }
        if en.Verified {
            weight = FAIL_WEIGHT
        }
        ip := cp.GetPrimaryIp(en.Dst)
        if ip == "" {
            logger.Debugf("Server to fail:Not found", en.Dst)
            continue
        }
        logger.Infof("primary ip in decide server is",ip)
        if failCount[ip] == nil {
            failCount[ip]= make(map[string]int)
        }
		failCount[ip][en.Src] = weight
	}
	for key, val := range failCount {
        sum := 0
        for _, v := range val {
            sum += v
        }
        failedServer[key] = sum
	}
	return failedServer
}

func addEntry(m map[string]int, e VbaEntry) {
    m[e.Source] = 1
}

func (cls *Cluster) HandleTransferDone(ip string, dst string, vbl Vblist) map[string]int {
	//transfer is complete, so put the change in actual map and send the new config 
	cp := cls.GetContext(ip)
	if cp == nil {
		return nil
	}
	logger.Debugf("In transfer done")
	serverList := cp.V.Smap.ServerList
	changeVbaMap := make(map[string]int)
	vbucketMa := cp.V.Smap.VBucketMap
	vbucketMaFwd := cp.V.Smap.VBucketMapForward
	src := cp.getServerIndex(ip)
	if src == -1 {
		logger.Debugf("in HandleTransferDone src", src)
		return nil
	}
	oldVbaMap := cp.VbaInfo
	for _, vb := range vbl.Active {
		d := vbucketMaFwd[vb][0]
        removeEntry := func (src int) bool {
            logger.Debugf("in HandleTransferDone src", src)
            found := false
            key := serverList[src] + serverList[d]
            oldEntry, _ := oldVbaMap[key]
            for r := 0; r < len(oldEntry.Transfer_VbId); r++ {
                if oldEntry.Transfer_VbId[r] == vb {
                    logger.Debugf("vbucket found",vb)
                    oldEntry.Transfer_VbId = append(oldEntry.Transfer_VbId[:r],
                        oldEntry.Transfer_VbId[r+1:]...)
                    found = true
                    break
                }
            }
            if found == false {
                logger.Warnf("vbucket ", vb, " not found for transfer", oldEntry.Transfer_VbId)
                return false
            }
            if len(oldEntry.VbId) == 0 && len(oldEntry.Transfer_VbId) == 0 {
                delete(oldVbaMap, key)
            } else {
                oldVbaMap[key] = oldEntry
            }
            addEntry(changeVbaMap, oldEntry)

		    vbucket := vbucketMa[vb]
            for k := 1; k < len(vbucket); k++ {
                if vbucket[k] < 0 {
                    logger.Debugf("HandleTransferDone")
                    break
                }
                key = serverList[src] + serverList[vbucket[k]]
                oldEntry, _ = oldVbaMap[key]
                for r := 0; r < len(oldEntry.VbId); r++ {
                    if oldEntry.VbId[r] == vb {
                        oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
                        break
                    }
                }
                if len(oldEntry.VbId) == 0 && len(oldEntry.Transfer_VbId) == 0 {
                    delete(oldVbaMap, key)
                } else {
                    oldVbaMap[key] = oldEntry
                }
                addEntry(changeVbaMap, oldEntry)

                key := serverList[d] + serverList[vbucket[k]]
                oldEntry, ok := oldVbaMap[key]
                if ok == false {
                    oldEntry.Source = serverList[d]
                    oldEntry.Destination = serverList[vbucket[k]]
                }
                oldEntry.VbId = append(oldEntry.VbId, vb)
                addEntry(changeVbaMap, oldEntry)
                oldVbaMap[key] = oldEntry
                vbucketMa[vb][0] = d
            }
            return true
        }
        if removeEntry(src) == false {
            removeEntry(cp.getSecondaryIndex(src))
        }
    }
	cp.updateReshardStatus(src, vbl)
	return changeVbaMap
}

func (cp *Context) GetReshardStatus() string {
	switch cp.ReInfo.status {
	case RESHARD_CONT:
		return RESHARD_CONT_STR
	case RESHARD_DONE:
		return RESHARD_DONE_STR
	}
	return RESHARD_DONE_STR
}

func (cp *Context) updateReshardStatus(index int, list Vblist) {
    if cp.ReInfo.status == RESHARD_DONE {
        return
    }
    repCount := 0
    actCount := 0
    for _,val := range *cp.ReInfo.dvi {
        in := cp.getServerIndex(val.Server)
        if cp.SameServer(index, in) {
            for _,vb := range list.Active {
                for r,v := range val.Active {
                    if v == vb {
                        val.Active = append(val.Active[:r], val.Active[r+1:]...)
                        break
                    }
                }
            }
            for _,vb := range list.Replica {
                for r,v := range val.Replica {
                    if v == vb {
                        val.Replica = append(val.Replica[:r], val.Replica[r+1:]...)
                        break
                    }
                }
            }
        }
        //repCount += len(val.Replica)
        actCount += len(val.Active)
    }
    if repCount + actCount == 0 {
        cp.ReInfo.status = RESHARD_DONE
    }
}

func sameMap(a, b [][]int) bool {
	for i := range a {
		if a[i][0] != b[i][0] {
			return false
		}
	}
	return true
}

func (cp *Context) SetReshard() bool {
	if cp.ReInfo.status == RESHARD_CONT {
		return false
	}
	cp.ReInfo.status = RESHARD_CONT
	return true
}

func (cp *Context) createSecondaryIpMap() {
    vbucketMap := cp.V.Smap.ServerList
    cp.SecondaryIpMap = make(map[int]int)
    cfg := cp.C
    for k,server := range(vbucketMap) {
        for i,priServers := range(cp.C.Servers) {
            if priServers == server {
                if len(cfg.SecondaryIps) > i && cfg.SecondaryIps[i] != "" {
                    getSecIndex := func(ip string) int {
                        for j,l := range(vbucketMap) {
                            if ip == l {
                                return j
                            }
                        }
                        return -1
                    }
                    secIndex := getSecIndex(cfg.SecondaryIps[i])
                    cp.SecondaryIpMap[k] = secIndex
                    cp.SecondaryIpMap[secIndex] = k
                } else {
                    cp.SecondaryIpMap[k] = -1
                }
                break
            }
        }
    }
}

func (cls *Cluster) GetContext(ip string) *Context {
	ip = strings.Split(ip, ":")[0]
	clusterName, ok := cls.IpMap[ip]
	if ok == false {
		logger.Warnf("Ip ", ip, " not in cluster", cls)
		return nil
	}
	cp, ok := cls.ContextMap[clusterName]
	if ok == false {
		logger.Warnf("Invalid cluster", clusterName)
		return nil
	}
	return cp
}

func (cls *Cluster) GenerateIpMap() bool {
	for key, cfg := range cls.ConfigMap {
		cls.AddIpToIpMap(cfg.Servers, cfg.SecondaryIps, key)
	}
	return true
}

func (cls *Cluster) AddIpToIpMap(p []string, s []string, c string) {
	//It will add ip to the ipmap
	for _, s := range p {
		if s == "" {
			continue
		}
		server := strings.Split(s, ":")[0]
		cls.IpMap[server] = c
	}
	for _, s := range s {
		if s == "" {
			continue
		}
		server := strings.Split(s, ":")[0]
		cls.IpMap[server] = c
	}
}

func (cls *Cluster) HandleReplicaConfig(conf map[string]*Context) {
    cls.ContextMap = conf
	for _, cp := range cls.ContextMap {
        logger.Debugf("got config map as", cp.V)
	}
    for _,cp := range conf {
        cp.createSecondaryIpMap()
    }
}

func (cls *Cluster) IsReplicaVbs() bool {
    return cls.State == STATE_REPLICA
}

func (cls *Cluster) IsActiveVbs() bool {
    return cls.State == STATE_ACTIVE
}

func (cls *Cluster) PromoteVbs(state string) {
    if state == STATE_REPLICA {
        cls.State = STATE_ACTIVE
        logger.Debugf("Settting state to active")
    }
    return
}
