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
//contains the helper functions
package server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"vbucketserver/log"
    "vbucketserver/net"
	"os"
	"strings"
	"time"
	"vbucketserver/config"
    "strconv"
)

var logger *log.SysLog

const (
	MAX_CONFIG_LEN = 4 * 1024
)

func SetLogger(l *log.SysLog) {
    logger = l
}

func getIpAddr(c net.Conn) string {
	ip := strings.Split(c.RemoteAddr().String(), ":")[0]
	//ip := c.RemoteAddr().String()
	return ip
}

func getIpAddrWithPort(c net.Conn) string {
	ip := c.RemoteAddr().String()
	return ip
}

///XXX:Put more validation checks for file inputs
func parseInitialConfig(f string, cls *config.Cluster) bool {
	fi, err := os.Open(f)
	if err != nil {
		//XXX:may be need to change here
		logger.Fatalf("Unable to open initial config file", err)
	}
	defer fi.Close()
	//XXX:Need to increase the buffer size
	buf := make([]byte, MAX_CONFIG_LEN)
	buf, err = ioutil.ReadAll(fi)
	if err != nil {
		logger.Fatalf("Error in reading inital config file", err)
        return false
	}

	if err := json.Unmarshal(buf, cls); err != nil {
		logger.Fatalf("Error in unmarshalling config file contents")
        return false
	}

    if cls.IsReplicaVbs() && cls.ActiveIp == "" {
		logger.Fatalf("Need to specify active VBS ip")
        return false
    }
    logger.Debugf("Initial Cluster config is ", cls)

	cls.M.Lock()
	if cls.GenerateIpMap() == false {
		logger.Fatalf("Same server in multiple pools in Config")
        return false
	}
	if err := validateConfig(cls); err != nil {
		logger.Fatalf(err.Error())
        return false
	}
	cls.M.Unlock()
	return true
}

func validateConfig(cls *config.Cluster) error {
	portMap := make(map[uint16]bool)
	for _, cfg := range cls.ConfigMap {
		if _, ok := portMap[cfg.Port]; ok {
			return errors.New("Duplcate Port Number for pools in Config")
		}
		portMap[cfg.Port] = true
		if cfg.Replica < 0 || cfg.Capacity <= 0 || len(cfg.Servers) == 0 {
			return errors.New("Error in either replica, capacity or servers in Config")
		}
        for _,server := range append(cfg.Servers, cfg.SecondaryIps...) {
            if validateIp(server) == false {
			    return errors.New("Error in server ip/port in Config")
            }
        }
	}
	return nil
}

func validateIp(ip string) bool {
    val := strings.Split(ip, ":")
    if len(val) < 2 {
        return false
    }
    if _,err := strconv.Atoi(val[1]); err != nil {
        return false
    }
    return true
}

//return the message structure for a message type
func getMsg(t int, args ...interface{}) (interface{}, error) {
    logger.Debugf("command type is", t)
	return func(args []interface{}) (interface{}) {
		switch t {
		case MSG_INIT:
			m := InitMsg{}
			m.Cmd = MSG_INIT_STR
			return m
		case MSG_CONFIG:
			cls, _ := args[0].(*config.Cluster)
			t, _ := args[1].(int)
			agent, _ := args[2].(string)
			ip, _ := args[3].(string)
			if agent == CLIENT_MOXI {
				data := ClusterVbucketMap{}
				for _, cp := range cls.ContextMap {
					cp.M.RLock()
					data.Buckets = append(data.Buckets, cp.V)
					cp.M.RUnlock()
				}
				logger.Debugf("agent is", agent)
				m := ConfigMsg{Cmd: MSG_CONFIG_STR, Data: data, HeartBeatTime: t}
				return m
			} else if agent == CLIENT_VBA {
				m := ConfigVbaMsg{Cmd: MSG_CONFIG_STR, HeartBeatTime: t}
				cp := cls.GetContext(ip)
				if cp == nil {
					break
				}

				for _, entry := range cp.VbaInfo {
                    s := getServerIndex(cp, entry.Source)
                    d := getServerIndex(cp, ip)
                    if cp.SameServer(s, d) {
						m.Data = append(m.Data, entry)
                    }
				}
				if index := getServerIndex(cp, ip); index != -1 {
					replicas := cp.S[index]
					if len(replicas.ReplicaVbuckets) > 0 {
						m.RestoreCheckPoints = append(m.RestoreCheckPoints, replicas.ReplicaVbuckets...)
					}
				}
                logger.Debugf("getMsg : config is ",m,ip)
				return m
			} else {
			    cls, _ := args[0].(*config.Cluster)
                logger.Debugf("sending to replica vbs", cls.ContextMap)
                m := ReplicaMsg{Cmd: MSG_REPLICA_CONFIG_STR, Cls : cls.ContextMap}
				return m
            }
        case MSG_REPLICA_VBS:
            m := ReplicaMsg{Agent:CLIENT_REPLICA_VBS}
            return m
        case MSG_HB:
            m := InitMsg{Cmd:MSG_ALIVE_STR}
            return m
		}
		//need to fix this here
		m := InitMsg{}
		m.Cmd = MSG_INIT_STR
		return m
	}(args), nil
}

//wait for VBA's to connect initially
func waitForVBAs(cls *config.Cluster, to int, co *Client, genConf bool) {
	time.Sleep(time.Duration(to) * time.Second)
	logger.Debugf("sleep over for vbas")
	cls.M.Lock()
	for key, cfg := range cls.ConfigMap {
		serverList := cfg.Servers
        if genConf {
		    checkVBAs(&cfg, co.Vba)
            cp := config.NewContext()
            go checkServerDown(cp, co)  //routine to handle the dead servers
            cp.GenMap(key, &cfg)
            cp.C.Servers = serverList
            cls.ContextMap[key] = cp
        } else {
            cp := cls.ContextMap[key]
            go checkServerDown(cp, co)  //routine to handle the dead servers
        }
	}
	cls.M.Unlock()
	co.Started = true
	co.Cond.Broadcast()
}

func vbsVerifyNode(node string, cp *config.Context) {
    _, err := net.DialTimeout("tcp", node, 60*time.Second)
    if err != nil {
        fi := &cp.MoxiFi
        fi.M.Lock()
        entry := config.FailureEntry {
            Src :      node,
            Dst :      node,
            Verified : true,
        }
        fi.F = append(fi.F, entry)
        logger.Debugf("Added the verified failed node entry", entry)
        fi.M.Unlock()
    }
}

func checkServerDown(cp *config.Context, co *Client) {
	//lastNodeFailed := []string{}
    failtime := FAILOVER_TIME
    clear := false
	for {
		select {
		case <-time.After(FAIL_CHECK_TIME * time.Second):
            failtime -= FAIL_CHECK_TIME
            if failtime <= 0 {
                clear = true
                failtime = FAILOVER_TIME
            }
            ok, mp := cp.HandleDown(clear, vbsVerifyNode)
            if ok {
                PushNewConfig(co, mp, true, cp)
            }
        }
    }
}

func getServerIndex(cp *config.Context, sr string) int {
	sr = strings.Split(sr, ":")[0]
	for s := range cp.V.Smap.ServerList {
		if strings.Split(cp.V.Smap.ServerList[s], ":")[0] == sr {
			return s
		}
	}
	logger.Warnf("Server not found.input server is", sr, "all are", cp.C.Servers)
	//panic("server not found")
	return -1
}

func getIpFromConfig(cp *config.Context, sr string) string {
	for _,s := range append(cp.C.Servers, cp.C.SecondaryIps...) {
		if strings.Split(s, ":")[0] == sr {
			logger.Debugf("Found server in config list", sr, cp.C.Servers)
			return s
		}
	}
	return ""
}

func (cl *ClientInfo) WaitForPushConfig(co *Client, cp *config.Context) {
    if cl.W != nil {
        return
    }
    logger.Debugf("in wait for config", getIpAddrWithPort(cl.Conn))
    ch := make(chan string)
    cl.W = ch
    go func() {
        select {
        case _,ok := <-ch:
            if ok == false {
                return
            }
        case <-time.After((2*HBTIME+5) * time.Second):
            logger.Warnf("Unable to push config/timeing out.so failing node", getIpAddrWithPort(cl.Conn))
            ok, mp := cp.HandleServerDown([]string{getIpAddrWithPort(cl.Conn)})
            if ok {
                PushNewConfig(co, mp, true, cp)
            }
        }
    }()
    return
}

//push the config to a VBA
func PushNewConfigAll(co *Client, ipl map[string]int, cp *config.Context) {
    co.Vba.Mu.Lock()
    for ip := range ipl {
        ip = cp.GetPrimaryIp(ip)
        ip = strings.Split(ip, ":")[0]
        if val, ok := co.Vba.Ma[ip]; ok {
            if val.C != nil {
                logger.Infof("Sending config to vba ",ip)
                val.C <- CHN_NOTIFY_STR
                if val.W != nil {
                    close(val.W)
                    val.W = nil
                }
            } else {
                (&val).WaitForPushConfig(co, cp)
            }
        }
    }
    co.Vba.Mu.Unlock()
    co.Moxi.Mu.Lock()
    for ip, val := range co.Moxi.Ma {
        logger.Infof("Sending config to moxi", ip)
        val.C <- CHN_NOTIFY_STR
    }
    co.Moxi.Mu.Unlock()
    co.Rep.Mu.Lock()
    for ip, val := range co.Rep.Ma {
        logger.Infof("Sending config to vbs replica", ip)
        val.C <- CHN_NOTIFY_STR
    }
    co.Rep.Mu.Unlock()
}

//push the config to all the VBA's and Moxi
func PushNewConfig(co *Client, m map[string]config.VbaEntry, toMoxi bool, cp *config.Context) {
    logger.Debugf("In pushnewconfig")
	ma := make(map[string]int)
	for _, en := range m {
		if len(en.VbId) >= 0 {
			co.Vba.Mu.Lock()
            ip := cp.GetPrimaryIp(en.Source)
            if ip == "" {
			    co.Vba.Mu.Unlock()
                continue
            }
			ip = strings.Split(ip, ":")[0]
			if _, o := ma[ip]; o == false {
				if val, ok := co.Vba.Ma[ip]; ok {
                    if val.C != nil {
                        logger.Infof("Sending config to vba ",ip)
					    val.C <- CHN_NOTIFY_STR
                        if val.W != nil {
                            close(val.W)
                            val.W = nil
                        }
                    } else {
                        (&val).WaitForPushConfig(co, cp)
                    }
				}
				ma[ip] = 1
			}
			co.Vba.Mu.Unlock()
            logger.Debugf("notified ips", ma)
            logger.Debugf("conn map is", co.Vba.Ma)
		}
	}
    if toMoxi == true {
        co.Moxi.Mu.Lock()
        for ip, val := range co.Moxi.Ma {
            logger.Infof("Sending config to moxi", ip)
            val.C <- CHN_NOTIFY_STR
        }
        co.Moxi.Mu.Unlock()
    }
    co.Rep.Mu.Lock()
    for ip, val := range co.Rep.Ma {
        logger.Infof("Sending config to vbs replica", ip)
        val.C <- CHN_NOTIFY_STR
    }
    co.Rep.Mu.Unlock()
}

//update the servers list with the connected servers
func checkVBAs(c *config.Config, v ClientInfoMap) {
	v.Mu.RLock()
	defer v.Mu.RUnlock()
	connectedServs := []string{}
	for a := range c.Servers {
		ip := strings.Split(c.Servers[a], ":")[0]
		//Need to uncomment this
		if _, ok := v.Ma[ip]; ok {
			connectedServs = append(connectedServs, c.Servers[a])
        } else if len(c.SecondaryIps) > a {
            sip := c.SecondaryIps[a]
		    sip = strings.Split(sip, ":")[0]
            if _, ok := v.Ma[sip]; ok {
			    connectedServs = append(connectedServs, c.Servers[a])
            }
        }
	}
	//connectedServs = c.Servers
	logger.Debugf("connect servers are", connectedServs)
	capacity := (len(connectedServs) * 100) / len(c.Servers)
	if capacity < CLIENT_PCNT {
		//XXX:May be need to change this panic
     //   logger.Fatalf("Not enough server connected, capacity is", capacity)
      //  os.Exit(0)
	} else {
		c.Servers = connectedServs
		//update the capacity in number of vbuckets
		c.Capacity = (int16(capacity) * c.Capacity) / 100
	}
}

func Insert(c net.Conn, ch chan string, co *Client, a string) {
	/*XXX:close the older connection if exists*/
	ip := getIpAddr(c)
	logger.Infof("client connected ", ip)
	if a == CLIENT_MOXI {
		co.Moxi.Mu.Lock()
		if co.Moxi.Ma[ip] == nil {
			cf := &ClientInfo{C: ch, Conn: c}
			co.Moxi.Ma[ip] = cf
		} else {
			co.Moxi.Ma[ip].C <- CHN_CLOSE_STR
			co.Moxi.Ma[ip].C = ch
			co.Moxi.Ma[ip].Conn = c
		}
		co.Moxi.Mu.Unlock()
	} else if a == CLIENT_VBA {
		co.Vba.Mu.Lock()
		if co.Vba.Ma[ip] == nil {
			cf := &ClientInfo{C: ch, Conn: c}
			co.Vba.Ma[ip] = cf
		} else {
            val := co.Vba.Ma[ip]
            if val.C != nil {
			    val.C <- CHN_CLOSE_STR
            }
			val.C = ch
			val.Conn = c
            if val.W != nil {
                close(val.W)
                val.W = nil
            }
		}
		co.Vba.Mu.Unlock()
	} else {
        co.Rep.Mu.Lock()
		if co.Rep.Ma[ip] == nil {
			cf := &ClientInfo{C: ch, Conn: c}
			co.Rep.Ma[ip] = cf
		} else {
			co.Rep.Ma[ip].C <- CHN_CLOSE_STR
			co.Rep.Ma[ip].C = ch
			co.Rep.Ma[ip].Conn = c
		}
		co.Rep.Mu.Unlock()
    }
}

func RemoveConn(c net.Conn, co *Client, ct string, cp *config.Context) {
	/*XXX:close the older connection if exists*/
	ip := getIpAddr(c)
	if ct == CLIENT_MOXI {
		co.Moxi.Mu.Lock()
		if val, ok := co.Moxi.Ma[ip]; ok && val.Conn == c {
			delete(co.Moxi.Ma, ip)
		}
		co.Moxi.Mu.Unlock()
	} else if ct == CLIENT_VBA {
		co.Vba.Mu.Lock()
		if val, ok := co.Vba.Ma[ip]; ok && val.Conn == c {
            val.C = nil
            (&val).WaitForPushConfig(co, cp)
		}
		co.Vba.Mu.Unlock()
	}
}

