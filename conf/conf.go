package conf

import (
	"fmt"
    "math/rand"
)

func (c Conf) generatevBucketMap() (*[][]int, *[]int16, bool) {
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

func (cp *ParsedInfo) generateVBAmap() {
	m := cp.V.VBucketMap
	serverList := cp.C.Servers
	vbaMap := make(map[string]VbaEntry)
	var entry VbaEntry
    var ok bool
	for i := range m {
		server := serverList[m[i][0]]
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
	cp.VbaInfo = vbaMap
}

func (cp *ParsedInfo) GenMap(con *Conf) {
	if rv, cm, err := con.generatevBucketMap(); err == false {
        cp.M.Lock()
        defer cp.M.Unlock()
		cp.V.VBucketMap = *rv
		cp.V.HashAlgorithm = con.Hash
		cp.V.NumReplicas = int(con.Replica)
        fmt.Println("serverlist is", con.Servers)
		cp.V.ServerList = con.Servers
		cp.C = *con //update the config
		cp.generateVBAmap()
        fmt.Println("capacity is", con.Capacity)
		cp.updateMaxCapacity(con.Capacity, len(con.Servers), cm)
		fmt.Println("updated map ", cp.V)
	} else {
		fmt.Println("failed updated map ", err)
	}
}

func (cp *ParsedInfo) updateMaxCapacity(capacity int16, totServers int, cm *[]int16) {
    cc := int16(float32(2*cp.C.Vbuckets)*(1+(float32(capacity)/100)))/int16(totServers)
    for i := 0; i < totServers; i++ {
        c := ServerInfo {
            maxVbuckets: cc,
            currentVbuckets : (*cm)[i],
        }
        fmt.Println("capacity is", cc, i)
        cp.S = append(cp.S, c)
    }
}

//return the free server
func (cp *ParsedInfo) findFreeServer(s int) int {
    arr := make([]int, len(cp.C.Servers))
    for i:=0;i<len(cp.C.Servers);i++ {
        arr[i] = i
    }
    //remove the same server
    lastindex := len(cp.C.Servers)-1
    arr[s],arr[lastindex] = arr[lastindex], arr[s]
    lastindex--

    count := lastindex
    fmt.Println("lastindex is", lastindex)
    //donald knuth random shuffle algo ;)
    for k:=0; k<=count; k++ {
        fmt.Println("test")
        var j int32
        if lastindex == 0 {
            j = 0
        } else {
            j = rand.Int31n(int32(lastindex))
        }
        i := arr[j]
		serInfo := cp.S[i]
		if serInfo.currentVbuckets < serInfo.maxVbuckets {
			cp.S[i].currentVbuckets++
			return i
		} else {
            fmt.Println("failed current and max ,index", serInfo.currentVbuckets, serInfo.maxVbuckets, i)
        }
        arr[j],arr[lastindex] = arr[lastindex], arr[j]
        lastindex--
	}
    fmt.Println("freeserver", cp.S)
    panic("No free server")
	return -1
}

func (cp *ParsedInfo)reduceCapacity(s int, n int, c int16) {
    if  n == 0 {
        cp.S[s].maxVbuckets = 0
        cp.S[s].currentVbuckets = 0
    } else {
        cp.S[s].maxVbuckets -= cp.S[s].maxVbuckets/cp.S[s].NumberOfDisc
        cp.S[s].currentVbuckets -= c
    }
}

func (cp *ParsedInfo)getServerVbuckets(s int) *DeadVbucketInfo {
    cp.M.Lock()
    defer cp.M.Unlock()
    vbaMap := cp.V.VBucketMap
    dvi := new(DeadVbucketInfo)
    for i:=0;i<len(vbaMap);i++ {
        for j:=0;j<len(vbaMap[i]);j++ {
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

func (cp *ParsedInfo) HandleServerDown(ser string) map[string]VbaEntry {
    dvi := cp.getServerVbuckets(cp.getServerIndex(ser))
    return cp.HandleDeadVbuckets(*dvi, ser)
}

func (cp *ParsedInfo) HandleDeadVbuckets(dvi DeadVbucketInfo, s string) map[string]VbaEntry {
    cp.M.Lock()
    defer cp.M.Unlock()
	oldVbaMap := cp.VbaInfo
	serverList := cp.C.Servers
	vbucketMa := cp.V.VBucketMap
	changeVbaMap := make(map[string]VbaEntry)
    ser := cp.getServerIndex(s)
    if ser == -1 {
        panic("server not there")
    }
    fmt.Println("old vbucket map was", vbucketMa)
    cp.reduceCapacity(ser, dvi.DiscsFailed, int16(len(dvi.Active)+len(dvi.Replica)))
	for i := 0; i < len(dvi.Active); i++ {
		vbucket := vbucketMa[dvi.Active[i]]
		for k := 1; k < len(vbucket); k++ {
			key := serverList[vbucket[0]] + serverList[vbucket[k]]
            oldEntry := oldVbaMap[key]
            for r := 0; r < len(oldEntry.VbId); r++ {
                if oldEntry.VbId[r] == dvi.Active[i] {
                    if r == 0  {
                        oldEntry.VbId = oldEntry.VbId[1:]
                    } else {
                        oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
                    }
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
		serverIndex := cp.findFreeServer(vbucketMa[dvi.Active[i]][0])
		vbucketMa[dvi.Active[i]][1] = serverIndex

		vbucket = vbucketMa[dvi.Active[i]]
		for k := 1; k < len(vbucket); k++ {
			key := serverList[vbucket[0]] + serverList[vbucket[k]]
			oldEntry := oldVbaMap[key]
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
                if r == 0 {
                    oldEntry.VbId = oldEntry.VbId[1:]
                } else {
                    oldEntry.VbId = append(oldEntry.VbId[:r], oldEntry.VbId[r+1:]...)
                }
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
				serverIndex := cp.findFreeServer(vbucket[0])
				key := serverList[vbucket[0]] + serverList[serverIndex]
				oldEntry := oldVbaMap[key]
				oldEntry.VbId = append(oldEntry.VbId, dvi.Replica[i])
				changeVbaMap[key] = oldEntry
				vbucketMa[dvi.Replica[i]][j] = serverIndex
				oldVbaMap[key] = oldEntry
			}
		}
	}
    cp.VbaInfo = oldVbaMap
    fmt.Println("new vbucket map was",vbucketMa)
	return changeVbaMap
}

func (cp *ParsedInfo) HandleServerAlive(ser string) {
    cp.M.Lock()
    cp.C.Servers = append(cp.C.Servers, ser)
    cp.M.Unlock()
}

func (cp *ParsedInfo) getServerIndex(si string) int {
    s := cp.V.ServerList
    for i := range s {
        if s[i] == si {
            return i
       }
    }
    return -1
}

func (cp *ParsedInfo) HandleCapacityUpdate(ci CapacityUpdateInfo) {
    cp.M.Lock()
    defer cp.M.Unlock()
    i := cp.getServerIndex(ci.Server)
    si := cp.S[i]
    si.maxVbuckets += (si.maxVbuckets*ci.DiscAlive)/si.NumberOfDisc
    cp.S[i] = si
}
