package config

import (
	"log"
	"math/rand"
	"strings"
)

const (
	DEAD_NODE_IP = "0.0.0.0:11211"
    REPLICA_RESTORE = -2
)

//return the secondary ip given any ip
//return the same ip, if it is secondary
func (cp *Context) getSecondaryIp(ip string) {
}

//return the primary ip
//return the same ip, if it is primary
func (cp *Context) getPrimaryIp(ip string) {
}


func (cp *Context) getIndex(s int, arr []VbucketCountBoth, isprimary bool) int {
}


func (c Config) generatevBucketMap() (*[][]int, *[]uint32, bool) {
	serv := len(c.Servers)
	capacityMap := make([]uint32, len(c.Servers))
	maxActive := int(c.Vbuckets) / serv
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
					countReplica[lastserver] = cp.getIndex(c, countVbuckets, false)
				}
				confMap[i][j] = lastserver
				capacityMap[lastserver]++
				break
			}
		}
	}
	return &confMap, &capacityMap, false
}

func (cp *Context) generateVBAmap() {
	m := cp.V.Smap.VBucketMap
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
    //log.Println("generateVBAmap : vba info is", cp.VbaInfo)
}

func (cp *Context) parseIps(cfg *Config) {
//populate the map in cp and append cp.V.Smap.ServerList
// with servers and secondary ips 
}

func (cp *Context) GenMap(cname string, cfg *Config) {
    cp.parseIps(cfg)
	if rv, cm, err := cfg.generatevBucketMap(); err == false {
		cp.M.Lock()
		defer cp.M.Unlock()
		cp.V.Smap.VBucketMap = *rv
		cp.V.Smap.HashAlgorithm = cfg.Hash
		cp.V.Port = cfg.Port
		cp.V.Smap.NumReplicas = int(cfg.Replica)
		log.Println("serverlist is", cfg.Servers)
		//cp.V.Smap.ServerList = cfg.Servers
		cp.V.Name = cname
		cp.C = *cfg //update the cfgfig
		cp.generateVBAmap()
        cp.copyVbucketMap() //create the vbucketmap forward
		log.Println("capacity is", cfg.Capacity)
		cp.updateMaxCapacity(cfg.Capacity, len(cfg.Servers), cm)
		//log.Println("updated map ", cp.V)
	} else {
		log.Fatal("failed to generate config map ")
	}
}

func (cp *Context) updateMaxCapacity(capacity int16, totServers int, cm *[]uint32) {
	var cc uint32 = uint32(float32(uint32(cp.C.Replica+1)*uint32(cp.C.Vbuckets))*
        (1+(float32(capacity)/100))) / uint32(totServers)
	for i := 0; i < totServers; i++ {
		c := ServerInfo{
			MaxVbuckets:     cc,
			currentVbuckets: (*cm)[i],
		}
        log.Println("Capacity , server :", cc, i)
		cp.S = append(cp.S, c)
	}
   cp.Maxvbuckets = cc
}

//return the free server
func (cp *Context) findFreeServer(s int, s2 []int, s3 []int) int {
    var arr []int
    if len(s3) == 0 {
        arr = make([]int, len(cp.V.Smap.ServerList))
        for i := 0; i < len(cp.V.Smap.ServerList); i++ {
            arr[i] = i
        }
    } else {
        arr = s3
    }
    s2 = append(s2, s)
    for j := range s2 {
        for k := range arr {
            if arr[k] == j {
                arr = append(arr[:k], arr[k+1:]...)
            }
        }
    }
    lastindex := len(arr) - 1
	count := lastindex
	log.Println("lastindex is", lastindex)
	//donald knuth random shuffle algo ;)
	for k := 0; k <= count; k++ {
		var j int32
		if lastindex == 0 {
			j = 0
		} else {
			j = rand.Int31n(int32(lastindex))
		}
		i := arr[j]
		serInfo := cp.S[i]
		if serInfo.currentVbuckets < serInfo.MaxVbuckets {
			cp.S[i].currentVbuckets++
			return i
		} else {
			log.Println("failed current and max ,index", serInfo.currentVbuckets, serInfo.MaxVbuckets, i)
		}
		arr[j], arr[lastindex] = arr[lastindex], arr[j]
		lastindex--
	}
    //Need to have a list of vbuckets for which server is not available
	log.Println("freeserver not found for ", cp.S)
    cp.Rebalance = true
	return -1
}

func (cp *Context) reduceCapacity(s int, n int, c uint32) {
	if n == 0 {
		cp.S[s].MaxVbuckets = 0
		cp.S[s].currentVbuckets = 0
	} else {
		if cp.S[s].NumberOfDisk == 0 {
			log.Println("Disk is Zero for", s)
			return
		}
		cp.S[s].MaxVbuckets -= (uint32(n)*cp.S[s].MaxVbuckets) / uint32(cp.S[s].NumberOfDisk)
		cp.S[s].currentVbuckets -= c
	}
}

func (cp *Context) getServerVbuckets(s int) *DeadVbucketInfo {
	vbaMap := cp.V.Smap.VBucketMap
	dvi := new(DeadVbucketInfo)
	for i := 0; i < len(vbaMap); i++ {
		for j := 0; j < len(vbaMap[i]); j++ {
			if vbaMap[i][j] == s {
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

func (cp *Context) HandleServerDown(ser string) (bool, map[string]VbaEntry) {
	cp.M.Lock()
	dvi := cp.getServerVbuckets(cp.getServerIndex(ser))
	cp.M.Unlock()
    args := []DeadVbucketInfo{*dvi}
	return cp.HandleDeadVbuckets(args, []string{ser}, true, nil)
}

func (cp *Context) handleNoReplicaFailure(dvi DeadVbucketInfo, ser int) (bool, map[string]VbaEntry) {
	oldVbaMap := cp.VbaInfo
	serverList := cp.V.Smap.ServerList
	vbucketMa := cp.V.Smap.VBucketMap
	changeVbaMap := make(map[string]VbaEntry)

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
		var j int
		if vbucket[0] == ser {
			log.Println("vbucket", vbucket, "j is", j, "ser is", ser)
			serverIndex := cp.findFreeServer(vbucket[0],[]int{ser}, nil)
            if serverIndex != -1 {
                log.Println("j is, new server is", j, serverIndex)
                key := serverList[serverIndex]
                oldEntry := oldVbaMap[key]
                oldEntry.VbId = append(oldEntry.VbId, dvi.Active[i])
                changeVbaMap[key] = oldEntry
			    oldVbaMap[key] = oldEntry
			    vbucketMa[dvi.Active[i]][0] = serverIndex
            } else {
                log.Println("CRITICAL:Not enough capacity for active vbuckets")
                break
            }
		}
	}
	cp.VbaInfo = oldVbaMap
	log.Println("new vbucket map was", vbucketMa)
	return true, changeVbaMap
}

func (cp *Context) HandleTransferVbuckets(changeVbaMap map[string]VbaEntry, dvi DeadVbucketInfo,
    allFailedIndex []int, allNewIndex []int) {
    //transfer should be affected in FFT
    oldVbaMap := cp.VbaInfo
    vbucketMa := cp.V.Smap.VBucketMapForward
    serverList := cp.V.Smap.ServerList
    for i := range dvi.Transfer {
        vbucket := vbucketMa[dvi.Transfer[i]]
        ser := cp.findFreeServer(vbucket[0], allFailedIndex, allNewIndex)
        vbucket[0] = ser
        //add the new transfer entry
        key := serverList[vbucket[0]] + serverList[ser]
        oldEntry, ok := oldVbaMap[key]
        if ok == false {
            oldEntry.Source = serverList[vbucket[0]]
            oldEntry.Destination = serverList[ser]
        }
        oldEntry.Transfer_VbId = append(oldEntry.Transfer_VbId, dvi.Transfer[i])
        changeVbaMap[key] = oldEntry
        oldVbaMap[key] = oldEntry
    }
}

func (cp *Context) HandleDeadVbuckets(dvil []DeadVbucketInfo, sl []string, serverDown bool,
    newServerList []string) (bool, map[string]VbaEntry) {
	cp.M.Lock()
	defer cp.M.Unlock()
	oldVbaMap := cp.VbaInfo
	serverList := cp.V.Smap.ServerList
	vbucketMa := cp.V.Smap.VBucketMap
	changeVbaMap := make(map[string]VbaEntry)
	log.Println("input server list is", sl)
    allFailedIndex := []int{}
    allNewIndex := []int{}
    for _,i := range newServerList {
        allNewIndex = append(allNewIndex, cp.getServerIndex(i))
    }
    if len(newServerList) > 0 {
        for _,i := range sl {
            allFailedIndex = append(allFailedIndex, cp.getServerIndex(i))
        }
    }
    for i,s := range sl {
        dvi := dvil[i]
        ser := cp.getServerIndex(s)
        if len(newServerList) == 0 {
            allFailedIndex = []int{ser}
        }
        if ser == -1 {
            log.Println("Server not in list", s)
            return false, changeVbaMap
        } else if serverDown == false {
            d := cp.getServerVbuckets(ser)
            for i := range(dvi.Active) {
                for j:= range(d.Active) {
                    if dvi.Active[i] != d.Active[j] {
                        j++
                        if j == len(d.Active) {
                            log.Println("Invalid active vbuckets", dvi.Active, d.Active)
                            return false,nil
                        }
                    } else {
                        break
                    }
                }
                i++
            }
            for i := range(dvi.Replica) {
                for j:= range(d.Replica) {
                    if dvi.Replica[i] != d.Replica[j] {
                        j++
                        if j == len(d.Replica) {
                            log.Println("Invalid replica vbuckets")
                            return false,nil
                        }
                    } else {
                        break
                    }
                }
                i++
            }
        }
        log.Println("Failed vbuckets :", dvi)
        log.Println("old vbucket map was", vbucketMa)
        cp.reduceCapacity(ser, dvi.DisksFailed, uint32(len(dvi.Active)+len(dvi.Replica)))

        if cp.V.Smap.NumReplicas == 0 {
            _,changeVbaMap = cp.handleNoReplicaFailure(dvi, ser)
        } else {
            //handle the transfer part
            cp.HandleTransferVbuckets(changeVbaMap, dvi, allFailedIndex, allNewIndex)

            //handle the active vbuckets 
            for i := 0; i < len(dvi.Active); i++ {
                vbucket := vbucketMa[dvi.Active[i]]
                for k := 1; k < len(vbucket); k++ {
                    key := serverList[vbucket[0]] + serverList[vbucket[k]]
                    oldEntry := oldVbaMap[key]
                    for r := 0; r < len(oldEntry.VbId); r++ {
                        if oldEntry.VbId[r] == dvi.Active[i] {
                            oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
                            break
                        }
                    }
                    if len(oldEntry.VbId) == 0 {
                        delete(oldVbaMap, key)
                    } else {
                        changeVbaMap[key] = oldEntry
                        oldVbaMap[key] = oldEntry
                    }
                }
                k := 1
                for ;k<len(vbucket);k++ {
                    if vbucketMa[dvi.Active[i]][k] != -1 {
                        break
                    }
                }
                if k == len(vbucket) {
                    log.Println("CRITICAL:Not enough capacity for active vbuckets")
                    return true,changeVbaMap
                }
                vbucketMa[dvi.Active[i]][0] = vbucketMa[dvi.Active[i]][k]
                serverIndex := cp.findFreeServer(vbucketMa[dvi.Active[i]][0], allFailedIndex, allNewIndex)
                vbucketMa[dvi.Active[i]][k] = REPLICA_RESTORE
                cp.S[serverIndex].ReplicaVbuckets = append(cp.S[serverIndex].ReplicaVbuckets, dvi.Active[i])
                vbucket = vbucketMa[dvi.Active[i]]
                replicaEntry := VbaEntry{Source:serverList[serverIndex],}
                for k:=1; k < len(vbucket); k++ {
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
                vbucket := vbucketMa[dvi.Replica[i]]
                key := serverList[vbucket[0]] + serverList[ser]
                oldEntry := oldVbaMap[key]
                //delete the replica vbucket from the vba map
                for r := 0; r < len(oldEntry.VbId); r++ {
                    if oldEntry.VbId[r] == dvi.Replica[i] {
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
                var j int
                for j = 0; j < len(vbucket); j++ {
                    if vbucket[j] == ser {
                        log.Println("vbucket", vbucket, "j is", j, "ser is", ser)
                        serverIndex := cp.findFreeServer(vbucket[0], allFailedIndex, allNewIndex)
                        if serverIndex == -1 {
                            continue
                        }
                        vbucketMa[dvi.Replica[i]][j] = REPLICA_RESTORE
                        cp.S[serverIndex].ReplicaVbuckets = append(cp.S[serverIndex].ReplicaVbuckets, dvi.Replica[i])
                        replicaEntry := VbaEntry{Source:serverList[serverIndex],}
                        log.Println("j is, new server is", j, serverIndex)
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
            for i := range cp.C.Servers {
                if cp.C.Servers[i] == cp.V.Smap.ServerList[ser] {
                    cp.C.Servers[i] = DEAD_NODE_IP
                    break
                }
            }
            cp.V.Smap.ServerList[ser] = DEAD_NODE_IP
        }
    }
    log.Println("new vbucket map was", vbucketMa)
    log.Println("changed vba map is", changeVbaMap)
    log.Println("Final vbaMap is", oldVbaMap)
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

func (cp *Context) HandleServerAlive(ser []string, toAdd bool) {
	cp.M.Lock()
    if toAdd {
        cp.C.Servers = append(cp.C.Servers, ser...)
    }
    vbuckets := cp.C.Vbuckets
    servers := len(cp.V.Smap.ServerList) + len(ser)
    vbucketsPerServer := int(vbuckets)/(servers*2)
    dvil := []DeadVbucketInfo{}
    for i := range cp.V.Smap.ServerList {
        dvi := cp.getServerVbuckets(i)
        for j:=0; j<vbucketsPerServer; j++ {
            dvil[i].Replica = append(dvil[i].Replica, dvi.Replica[j])
        }
    }
    for _ = range(ser) {
        c := []ServerInfo{}
        cp.S = append(cp.S, c...)
    }
    serverList := cp.V.Smap.ServerList
    cp.V.Smap.ServerList = append(cp.V.Smap.ServerList, ser...)
    dvil = dvil[:0]
    for i := range cp.V.Smap.ServerList {
        dvi := cp.getServerVbuckets(i)
        for j:=0; j<vbucketsPerServer; j++ {
            dvil[i].Transfer = append(dvil[i].Active, dvi.Active[j])
        }
    }
    cp.HandleDeadVbuckets(dvil, serverList, false, ser)
    cp.M.Unlock()
}

func (cp *Context) getServerIndex(si string) int {
	s := cp.V.Smap.ServerList
	for i := range s {
		if s[i] == si {
			return i
		}
	}
	log.Println("server list is", s)
	return -1
}

func (cp *Context) HandleCapacityUpdate(ci CapacityUpdateInfo) {
	cp.M.Lock()
	defer cp.M.Unlock()
	i := cp.getServerIndex(ci.Server)
    if i == -1 {
        log.Println("Server not found for capaciy update", ci.Server)
        return
    }
	si := cp.S[i]
    if si.NumberOfDisk == 0 {
        log.Println("Capacity is zero for", ci.Server)
        return
    }
	si.MaxVbuckets += (si.MaxVbuckets * uint32(ci.DiskAlive)) / uint32(si.NumberOfDisk)
	cp.S[i] = si
}

func (cls *Cluster) GetContextFromClusterName(clusterName string) *Context {
	cp, ok := cls.ContextMap[clusterName]
	if ok == false {
		log.Println("cluster not in cluster name list", clusterName)
		return nil
	}
	return cp
}

func (cp *Context) getMasterServer(vb int) int {
    vbucket := cp.V.Smap.VBucketMap[vb]
    for i:= range vbucket {
        if vbucket[i] == REPLICA_RESTORE {
            //Put the server index in the map
            vbucket[i] = vb
            break
        }
    }
    return vbucket[0]
}

func (cp *Context) HandleRestoreCheckPoints(vb Vblist, ck Vblist, ip string) []string {
	cp.M.Lock()
    serverList := cp.V.Smap.ServerList
    serverToInfo := []string{}
    for i,vb := range vb.Replica {
        src := cp.getServerIndex(ip)
        ms := cp.getMasterServer(vb)
        key := serverList[src] + serverList[ms]
        oldEntry,ok := cp.VbaInfo[key]
        if ok == false {
            oldEntry.Source = serverList[src]
            oldEntry.Destination = serverList[ms]
        }
        vbId := []int{vb}
        oldEntry.VbId = append(vbId, oldEntry.VbId...)
        ckp := []int{ck.Replica[i]}
        oldEntry.CheckPoints = append(ckp, oldEntry.CheckPoints...)
        serverToInfo = append(serverToInfo, serverList[ms])
    }
	cp.M.Unlock()
    return serverToInfo
}

func (cls *Cluster) HandleTransferDone(ip string, dst string, vb Vblist) map[string]VbaEntry {
    //transfer is complete, so put the change in actual map and send the new config 
    cp := cls.GetContext(ip)
    if cp == nil {
        return nil
    }
	serverList := cp.V.Smap.ServerList
    changeVbaMap := make(map[string]VbaEntry)
    vbucketMa := cp.V.Smap.VBucketMap
    src := cp.getServerIndex(ip)
    d := cp.getServerIndex(dst)
    oldVbaMap := cp.VbaInfo
    for _,vb := range vb.Master {
        vbucket := vbucketMa[vb]
        for k := 1; k < len(vbucket); k++ {
            key := serverList[src] + serverList[vbucket[k]]
            oldEntry,_ := oldVbaMap[key]
            for r := 0; r < len(oldEntry.VbId); r++ {
                if oldEntry.VbId[r] == vb {
                    oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
                    break
                }
            }
            if len(oldEntry.VbId) == 0 {
                delete(oldVbaMap, key)
            } else {
                changeVbaMap[key] = oldEntry
                oldVbaMap[key] = oldEntry
            }
            vbucket[0] = d
            for k := 1; k < len(vbucket); k++ {
                key := serverList[vbucket[0]] + serverList[vbucket[k]]
                oldEntry, ok := oldVbaMap[key]
                if ok == false {
                    oldEntry.Source = serverList[vbucket[0]]
                    oldEntry.Destination = serverList[vbucket[k]]
                }
                oldEntry.VbId = append(oldEntry.VbId, vb)
                changeVbaMap[key] = oldEntry
                oldVbaMap[key] = oldEntry
            }
        }
    }
    return changeVbaMap
}

func (cls *Cluster) GetContext(ip string) *Context {
	ip = strings.Split(ip, ":")[0]
	clusterName, ok := cls.IpMap[ip]
	if ok == false {
		log.Println("Ip not in cluster name list", ip, cls)
		return nil
	}
	cp, ok := cls.ContextMap[clusterName]
	if ok == false {
		log.Println("cluster not in cluster name list", clusterName)
		return nil
	}
	return cp
}

func (cls *Cluster) GenerateIpMap() bool {
	for key, cfg := range cls.ConfigMap {
		for i := range cfg.Servers {
			server := strings.Split(cfg.Servers[i], ":")[0]
			if _, ok := cls.IpMap[server]; ok {
				return false
			}
			cls.IpMap[server] = key
		}
	}
	return true
}
