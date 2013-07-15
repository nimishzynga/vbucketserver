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
	"vbucketserver/config"
    "vbucketserver/net"
    "time"
)

type GenericClient struct {
}

func (gc *GenericClient) ClientType() string {
	return CLIENT_UNKNOWN
}

func (gc *GenericClient) HandleOk(cls *config.Cluster, co *Client, m *RecvMsg) bool {
	if m.Status == MSG_OK_STR {
		return true
	}
	return false
}

func (gc *GenericClient) HandleAlive(cls *config.Cluster, co *Client, m *RecvMsg) bool {
    logger.Debugf("inside handleAlive")
    if m.Cmd == MSG_ALIVE_STR {
		return true
	}
	return false
}

func (gc *GenericClient) HandleInit(ch chan string, cp *config.Cluster, co *Client, c int) bool {
	return false
}

func (gc *GenericClient) HandleFail(m *RecvMsg, cp *config.Cluster, co *Client) bool {
	return false
}

func (gc *GenericClient) HandleUpdateConfig(cp *config.Cluster) bool {
	return false
}

func (gc *GenericClient) HandleCheckPoint(m *RecvMsg, cls *config.Cluster) bool {
    return false
}

func (gc *GenericClient) HandleDeadvBuckets(m *RecvMsg, cls *config.Cluster, co *Client) bool {
    return false
}

type MoxiClient struct {
	conn net.Conn
	ch   chan []byte
	GenericClient
}

func (mc *MoxiClient) ClientType() string {
	return CLIENT_MOXI
}

func (mc *MoxiClient) HandleInit(ch chan string, cls *config.Cluster, co *Client, c int) bool {
	_ = c
	Insert(mc.conn, ch, co, CLIENT_MOXI)
	co.Cond.L.Lock()
	for co.Started == false {
		co.Cond.Wait()
	}
	co.Cond.L.Unlock()
	if m, err := getMsg(MSG_CONFIG, cls, HBTIME, CLIENT_MOXI, getIpAddr(mc.conn)); err == nil {
        logger.Infof("Sending config to moxi ", getIpAddr(mc.conn))
		mc.ch <- m
		return true
	}
	return false
}

func (mc *MoxiClient) HandleUpdateConfig(cls *config.Cluster) bool {
	if m, err := getMsg(MSG_CONFIG, cls, HBTIME, CLIENT_MOXI, getIpAddr(mc.conn)); err == nil {
        logger.Infof("Sending config to moxi ", getIpAddr(mc.conn))
		mc.ch <- m
		return true
	}
	return false
}

func (mc *MoxiClient) HandleFail(m *RecvMsg, cls *config.Cluster, co *Client) bool {
    logger.Infof("In Handle fail: Moxi failure due to ", m.Cmd)
	cp := cls.GetContext(m.Destination)
	if cp == nil {
		logger.Debugf("Not able to find context for", m.Destination)
		return false
	}
    fi := &cp.MoxiFi
    fi.M.Lock()
    entry := config.FailureEntry {
        Src : getIpAddr(mc.conn),
        Dst : m.Destination,
    }
    fi.F = append(fi.F, entry)
    logger.Debugf("Added the failed node entry", entry)
    fi.M.Unlock()
	return true
}

type VbaClient struct {
	conn net.Conn
	ch   chan []byte
	GenericClient
}

func (vc *VbaClient) ClientType() string {
	return CLIENT_VBA
}

func (vc *VbaClient) HandleInit(ch chan string, cls *config.Cluster, co *Client, capacity int) bool {
	Insert(vc.conn, ch, co, CLIENT_VBA)
	co.Cond.L.Lock()
	for co.Started == false {
		co.Cond.Wait()
	}
	co.Cond.L.Unlock()
    ip := getIpAddr(vc.conn)
	cp := cls.GetContext(ip)
	if cp == nil {
		logger.Debugf("Not able to find context for", ip)
		return false
	}
	index := getServerIndex(cp, ip)
	if index == -1 {
        /*
        if ip := getIpFromConfig(cp, ip);ip != "" {
            //TODO:need to fix the secondary ip in the call
            cp.HandleServerAlive([]string{ip}, nil, false)
	        index = getServerIndex(cp, ip)
        } else {
            logger.Debugf("Server not in list", ip)
            return false
        }*/
        logger.Debugf("Server not in list", ip)
        return false
    }
	si := cp.S[index]
	si.NumberOfDisk = int16(capacity)
    si.MaxVbuckets = cp.Maxvbuckets
	cp.S[index] = si

    if ok, mp := cp.NeedRebalance(index); ok {
        PushNewConfig(co, mp, true, cp)
        return true
    }
	if m, err := getMsg(MSG_CONFIG, cls, HBTIME, CLIENT_VBA, ip); err == nil {
        logger.Infof("Sending config to vba ", getIpAddr(vc.conn))
		vc.ch <- m
		return true
	}
	return false
}

func (vc *VbaClient) HandleUpdateConfig(cls *config.Cluster) bool {
	if m, err := getMsg(MSG_CONFIG, cls, HBTIME, CLIENT_VBA, getIpAddr(vc.conn)); err == nil {
        logger.Infof("Sending config to vba ", getIpAddr(vc.conn))
		vc.ch <- m
		return true
	}
	return false
}

func (vc *VbaClient) HandleOk(cls *config.Cluster, co *Client, m *RecvMsg) bool {
    if m.Status == MSG_OK_STR {
        cp := cls.GetContext(getIpAddr(vc.conn))
        logger.Debugf("before contxt got ok from", getIpAddr(vc.conn))
        if cp == nil {
            logger.Debugf("Not able to find context for", getIpAddr(vc.conn))
            return false
        }
        logger.Debugf("got ok from", getIpAddr(vc.conn))
        var ip map[string]int
        if m.Cmd == MSG_CONFIG_STR {
            ip = cp.HandleRestoreCheckPoints(m.Vbuckets, m.CheckPoints, getIpAddr(vc.conn))
        } else if m.Cmd == MSG_TRANSFER_STR {
            ip = cls.HandleTransferDone(getIpAddr(vc.conn), m.Destination, m.Vbuckets)
        }
        logger.Debugf("ip list is", ip)
        if len(ip) > 0 {
            cp.InfoMutex.Lock()
            logger.Debugf("ip list is 2", ip)
            if cp.NotifyServers == nil {
                cp.NotifyServers = make(map[string]int)
                logger.Debugf("notify map is", cp.NotifyServers)
                addMap(cp.NotifyServers, ip, cp)
                logger.Debugf("notify map is after", cp.NotifyServers)
                go func() {
                    time.Sleep(AGGREGATE_TIME * time.Second)
                    PushNewConfigToVBA(co, cp.NotifyServers, cp)
                    logger.Debugf("came up from sleep")
                    cp.NotifyServers = nil
                }()
            } else {
                addMap(cp.NotifyServers, ip, cp)
            }
            cp.InfoMutex.Unlock()
        }
        return true
    }
    if m.Status == MSG_ERROR_STR {
        logger.Debugf("VBA ERROR:", m.Detail)
        //nuke the bastard
        logger.Debugf("Server failure: since vba reported it", getIpAddr(vc.conn),
            m.Detail)
        vc.HandleFail(m, cls, co)
        return true
    }
	return false
}

func (vc *VbaClient) HandleFail(m *RecvMsg, cls *config.Cluster, co *Client) bool {
    logger.Infof("In Handle fail: failure due to ", m.Cmd)
	cp := cls.GetContext(getIpAddr(vc.conn))
	if cp == nil {
		logger.Debugf("Not able to find context for", getIpAddr(vc.conn))
		return false
	}
    fi := &config.FailureInfo{}
    if m.Cmd == MSG_FAIL_STR {
        fi = &cp.NodeFi
    } else {
        fi = &cp.RepFi
    }
    if getServerIndex(cp, getIpAddr(vc.conn)) == -1 {
        logger.Debugf("invalid server reporting failure", getIpAddr(vc.conn))
        return false
    }
    fi.M.Lock()
    entry := config.FailureEntry {
        Src : getIpAddr(vc.conn),
        Dst : m.Destination,
    }
    fi.F = append(fi.F, entry)
    logger.Debugf("Added the failed node entry", entry)
    fi.M.Unlock()
	return true
}

func (vc *VbaClient) HandleDeadvBuckets(m *RecvMsg, cls *config.Cluster, co *Client) bool {
	cp := cls.GetContext(getIpAddr(vc.conn))
	if cp == nil {
		logger.Debugf("Not able to find context for", getIpAddr(vc.conn))
		return false
	}
    var dvi config.DeadVbucketInfo
    dvi.Server = getIpAddr(vc.conn)
    dvi.Active = m.Vbuckets.Active
    dvi.Replica = m.Vbuckets.Replica
    args := []config.DeadVbucketInfo{dvi}
    str := []string{dvi.Server}
    ok, mp := cp.HandleDeadVbuckets(args, str, false, nil, true)
    if ok {
        PushNewConfig(co, mp, true, cp)
    }
	return true
}

func addMap(a,b map[string]int, cp *config.Context) {
    for data,_ := range b {
        a[cp.GetPrimaryIp(data)]=1
    }
}

func (vc *VbaClient) HandleAlive(cls *config.Cluster, co *Client, m *RecvMsg) bool {
    if m.Cmd == MSG_ALIVE_STR {
		return true
	}
	return false
}

func (vc *VbaClient) HandleCheckPoint(m *RecvMsg, cls *config.Cluster) bool {
	logger.Debugf("inside handleCheckPoint for ", m.Vbuckets, m.CheckPoints)
	cp := cls.GetContext(getIpAddr(vc.conn))
	if cp == nil {
		logger.Debugf("Not able to find context for", getIpAddr(vc.conn))
		return false
	}
	cp.HandleCheckPoint(getIpAddr(vc.conn), m.Vbuckets, m.CheckPoints)
    return true
}
