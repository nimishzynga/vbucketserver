package conf

import (
	"fmt"
)

func (c Conf) generatevBucketMap() (*[][]int, bool) {
	serv := len(c.Servers)
	maxActive := int(c.Vbuckets) / serv
	if int(c.Vbuckets)%serv > 0 {
		maxActive += 1
	}
	maxReplica := maxActive * int(c.Replica)
	countReplica := make([]int, serv)
	if serv <= int(c.Replica) {
		return nil, true
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
				break
			}
		}
	}
	return &confMap, false
}

func activateState(m []stateEntry) {
}

func (cp *ParsedInfo) generateStatemap() []stateEntry {
    m := cp.V.VBucketMap
	stateMap := []stateEntry{}
	for i := range m {
		for j := range m[i] {
			if j == 0 {
                s := NewStateEntry(m[i][j], i, "Active")
				stateMap = append(stateMap, s)
			} else {
                s := NewStateEntry(m[i][j], i, "Replica")
				stateMap = append(stateMap, s)
			}
		}
	}
	return stateMap
}

func (cp *ParsedInfo) generateVBAmap() {
	m := cp.V.VBucketMap
	serverList := cp.C.Servers
	vbaMap := make(map[string]VbaEntry)
	var entry VbaEntry
	for i := range m {
		server := serverList[m[i][0]]
		for j := 1; j < len(m[i]); j++ {
			hashKey := server + serverList[m[i][j]]
			if entry, ok := vbaMap[hashKey]; ok {
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
	if rv, err := con.generatevBucketMap(); err == false {
		cp.M.Lock()
		cp.V.VBucketMap = *rv
		cp.V.HashAlgorithm = con.Hash
		cp.V.NumReplicas = int(con.Replica)
		cp.V.ServerList = con.Servers
		cp.C = *con //update the config
		stateMap := cp.generateStatemap()
		activateState(stateMap)
		cp.generateVBAmap()
		//cl.PushNewConfig(cp.VbaInfo)
		cp.updateMaxCapacity(con.Capacity, len(con.Servers))
		cp.M.Unlock()
		fmt.Println("updated map ", cp.V)
	} else {
		fmt.Println("failed updated map ", err)
	}
}

func (cp *ParsedInfo) updateMaxCapacity(capacity int16, totServers int) {
	c := ServerInfo{
		maxVbuckets: capacity,
	}
	for i := 0; i < totServers; i++ {
		cp.S = append(cp.S, c)
	}
}

//return the free server
func (cp *ParsedInfo) findFreeServer(s int) int {
	for i := 0; i < len(cp.C.Servers); i++ {
		serInfo := cp.S[i]
		if i != s && serInfo.currentVbuckets < serInfo.maxVbuckets {
			cp.S[i].currentVbuckets++
			return i
		}
	}
	return -1
}

func (cp *ParsedInfo) HandleDeadVbuckets(dvi DeadVbucketInfo, ser int) ([]stateEntry, map[string]VbaEntry) {
	oldVbaMap := cp.VbaInfo
	serverList := cp.C.Servers
	vbucketMa := cp.V.VBucketMap
	changeStateMap := []stateEntry{}
	changeVbaMap := make(map[string]VbaEntry)

	for i := 0; i < len(dvi.Active); i++ {
		s := NewStateEntry(vbucketMa[dvi.Active[i]][0], dvi.Active[i], "dead")
		changeStateMap = append(changeStateMap, s)
		vbucket := vbucketMa[dvi.Active[i]]
		for k := 1; k < len(vbucket); k++ {
			key := serverList[vbucket[0]] + serverList[vbucket[k]]
			oldEntry := oldVbaMap[key]
			for r := 0; r < len(oldEntry.VbId); r++ {
				if oldEntry.VbId[r] == dvi.Active[i] {
					oldEntry.VbId = append(oldEntry.VbId[:r-1], oldEntry.VbId[r:]...)
					break
				}
			}
			changeVbaMap[key] = oldEntry
			oldVbaMap[key] = oldEntry
		}
		vbucketMa[dvi.Active[i]][0] = vbucketMa[dvi.Active[i]][1]
		s = NewStateEntry(vbucketMa[dvi.Active[i]][0], dvi.Active[i], "Active")
		changeStateMap = append(changeStateMap, s)
		serverIndex := cp.findFreeServer(vbucketMa[dvi.Active[i]][0])
		s = NewStateEntry(serverIndex, dvi.Active[i], "Replica")
		changeStateMap = append(changeStateMap, s)
		vbucketMa[dvi.Active[i]][1] = serverIndex

		vbucket = vbucketMa[dvi.Active[i]]
		for k := 1; k < len(vbucket); k++ {
			key := serverList[vbucket[0]] + serverList[vbucket[k]]
			oldEntry := oldVbaMap[key]
			oldEntry.VbId = append(oldEntry.VbId, i)
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
				oldEntry.VbId = append(oldEntry.VbId[:r-1], oldEntry.VbId[r:]...)
				changeVbaMap[key] = oldEntry
				oldVbaMap[key] = oldEntry
				break
			}
		}
		var j int
		for j = 0; j < len(vbucket); j++ {
			if vbucket[j] == ser {
				s := NewStateEntry(ser, dvi.Replica[i], "dead")
				changeStateMap = append(changeStateMap, s)
				serverIndex := cp.findFreeServer(vbucket[0])
				key := serverList[vbucket[0]] + serverList[serverIndex]
				oldEntry := oldVbaMap[key]
				oldEntry.VbId = append(oldEntry.VbId, i)
				changeVbaMap[key] = oldEntry
				s = NewStateEntry(serverIndex, dvi.Replica[i], "Replica")
				changeStateMap = append(changeStateMap, s)
				vbucketMa[dvi.Replica[i]][j] = serverIndex
				oldVbaMap[key] = oldEntry
			}
		}
	}
	//need to update old map
	return changeStateMap, changeVbaMap
}
