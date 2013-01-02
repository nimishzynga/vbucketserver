package config

import (
	"log"
	"math/rand"
	"strings"
)

const (
	DEAD_NODE_IP = "0.0.0.0:11211"
)

func (c Config) generatevBucketMap() (*[][]int, *[]int16, bool) {
	serv := len(c.Servers)
	capacityMap := make([]int16, len(c.Servers))
	maxActive := int(c.Vbuckets) / serv
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
		confMap[i][0] = s
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
	serverList := cp.C.Servers
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
    log.Println("generateVBAmap : vba info is", cp.VbaInfo)
}

func (cp *Context) GenMap(cname string, cfg *Config) {
	if rv, cm, err := cfg.generatevBucketMap(); err == false {
		cp.M.Lock()
		defer cp.M.Unlock()
		cp.V.Smap.VBucketMap = *rv
		cp.V.Smap.HashAlgorithm = cfg.Hash
		cp.V.Port = cfg.Port
		cp.V.Smap.NumReplicas = int(cfg.Replica)
		log.Println("serverlist is", cfg.Servers)
		cp.V.Smap.ServerList = cfg.Servers
		cp.V.Name = cname
		cp.C = *cfg //update the cfgfig
		cp.generateVBAmap()
		log.Println("capacity is", cfg.Capacity)
		cp.updateMaxCapacity(cfg.Capacity, len(cfg.Servers), cm)
		log.Println("updated map ", cp.V)
	} else {
		log.Fatal("failed to generate config map ")
	}
}

func (cp *Context) updateMaxCapacity(capacity int16, totServers int, cm *[]int16) {
	cc := int16(float32(2*cp.C.Vbuckets)*(1+(float32(capacity)/100))) / int16(totServers)
	for i := 0; i < totServers; i++ {
		c := ServerInfo{
			MaxVbuckets:     cc,
			currentVbuckets: (*cm)[i],
		}
		log.Println("capacity is", cc, i)
		cp.S = append(cp.S, c)
	}
   cp.Maxvbuckets = cc
}

//return the free server
func (cp *Context) findFreeServer(s int, s2 int) int {
	arr := make([]int, len(cp.V.Smap.ServerList))
	for i := 0; i < len(cp.V.Smap.ServerList); i++ {
		arr[i] = i
	}
	//remove the same server
	lastindex := len(cp.V.Smap.ServerList) - 1
	arr[s], arr[lastindex] = arr[lastindex], arr[s]
	lastindex--
	arr[s2], arr[lastindex] = arr[lastindex], arr[s2]
	lastindex--

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
	log.Fatal("freeserver not found for ", cp.S)
	return -1
}

func (cp *Context) reduceCapacity(s int, n int, c int16) {
	if n == 0 {
		cp.S[s].MaxVbuckets = 0
		cp.S[s].currentVbuckets = 0
	} else {
		if cp.S[s].NumberOfDisk == 0 {
			log.Println("Disk is Zero for", s)
			return
		}
		cp.S[s].MaxVbuckets -= (int16(n)*cp.S[s].MaxVbuckets) / cp.S[s].NumberOfDisk
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
	return cp.HandleDeadVbuckets(*dvi, ser, true)
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
			serverIndex := cp.findFreeServer(vbucket[0], ser)
			log.Println("j is, new server is", j, serverIndex)
			key := serverList[serverIndex]
			oldEntry := oldVbaMap[key]
			oldEntry.VbId = append(oldEntry.VbId, dvi.Active[i])
			changeVbaMap[key] = oldEntry
			vbucketMa[dvi.Active[i]][0] = serverIndex
			oldVbaMap[key] = oldEntry
		}
	}
	cp.VbaInfo = oldVbaMap
	log.Println("new vbucket map was", vbucketMa)
	return true, changeVbaMap
}

func (cp *Context) HandleDeadVbuckets(dvi DeadVbucketInfo, s string, serverDown bool) (bool, map[string]VbaEntry) {
	cp.M.Lock()
	defer cp.M.Unlock()
	oldVbaMap := cp.VbaInfo
	serverList := cp.V.Smap.ServerList
	vbucketMa := cp.V.Smap.VBucketMap
	changeVbaMap := make(map[string]VbaEntry)
	log.Println("input server is", s)
	ser := cp.getServerIndex(s)
	if ser == -1 {
		log.Println("Server not in list", s)
		return false, changeVbaMap
	} else if serverDown {
		cp.V.Smap.ServerList[ser] = DEAD_NODE_IP
	} else {
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
	cp.reduceCapacity(ser, dvi.DisksFailed, int16(len(dvi.Active)+len(dvi.Replica)))
	if cp.V.Smap.NumReplicas == 0 {
		return cp.handleNoReplicaFailure(dvi, ser)
	}
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
		vbucketMa[dvi.Active[i]][0] = vbucketMa[dvi.Active[i]][1]
		serverIndex := cp.findFreeServer(vbucketMa[dvi.Active[i]][0], ser)
		vbucketMa[dvi.Active[i]][1] = serverIndex

		vbucket = vbucketMa[dvi.Active[i]]
		for k := 1; k < len(vbucket); k++ {
			key := serverList[vbucket[0]] + serverList[vbucket[k]]
			oldEntry, ok := oldVbaMap[key]
            if ok == false {
                    oldEntry.Source = serverList[vbucket[0]]
                    oldEntry.Destination = serverList[serverIndex]
            }
			oldEntry.VbId = append(oldEntry.VbId, dvi.Active[i])
			changeVbaMap[key] = oldEntry
			oldVbaMap[key] = oldEntry
		}
	}

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
				serverIndex := cp.findFreeServer(vbucket[0], ser)
				log.Println("j is, new server is", j, serverIndex)
				key := serverList[vbucket[0]] + serverList[serverIndex]
				oldEntry, ok := oldVbaMap[key]
                if ok == false {
                    oldEntry.Source = serverList[vbucket[0]]
                    oldEntry.Destination = serverList[serverIndex]
                }
				oldEntry.VbId = append(oldEntry.VbId, dvi.Replica[i])
				changeVbaMap[key] = oldEntry
				vbucketMa[dvi.Replica[i]][j] = serverIndex
				oldVbaMap[key] = oldEntry
			}
		}
	}
	cp.VbaInfo = oldVbaMap
    cp.V.Smap.VBucketMap = vbucketMa
	log.Println("new vbucket map was", vbucketMa)
    log.Println("changed vba map is", changeVbaMap)
	return true, changeVbaMap
}

func (cp *Context) HandleServerAlive(ser string) {
	cp.M.Lock()
    cp.C.Servers = append(cp.C.Servers, ser)
    cp.V.Smap.ServerList = append(cp.V.Smap.ServerList, ser)
    c := ServerInfo{}
    cp.S = append(cp.S, c)
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
	si := cp.S[i]
	si.MaxVbuckets += (si.MaxVbuckets * ci.DiskAlive) / si.NumberOfDisk
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
