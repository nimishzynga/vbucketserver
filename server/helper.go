//contains the helper functions
package server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"time"
	"vbucketserver/config"
)

const (
    MAX_CONFIG_LEN = 4*1024
)

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
        log.Fatal("Unable to open initial config file", err)
	}
	defer fi.Close()
	//XXX:Need to increase the buffer size
	buf := make([]byte, MAX_CONFIG_LEN)
	buf, err = ioutil.ReadAll(fi)
	if err != nil {
        log.Fatal("Error in reading inital config file", err)
	}

	if err := json.Unmarshal(buf, &cls.ConfigMap); err != nil {
		log.Fatal("Error in unmarshalling config file contents")
	}

    log.Println("Initial Cluster config is ", cls)
	cls.M.Lock()
	if cls.GenerateIpMap() == false {
		log.Fatal("Same server in multiple pools in Config")
	}
	if err := validateConfig(cls); err != nil {
		log.Fatal(err)
	}
	cls.M.Unlock()
	return true
}

func validateConfig(cls *config.Cluster) error {
	portMap := make(map[int16]bool)
	for _, cfg := range cls.ConfigMap {
		if _, ok := portMap[cfg.Port]; ok {
			return errors.New("Duplcate Port Number for pools in Config")
		}
		portMap[cfg.Port] = true
		if cfg.Replica < 0 || cfg.Capacity <= 0 || len(cfg.Servers) == 0 {
			return errors.New("Error in either replica, capacity or servers in Config")
		}
	}
	return nil
}

//return the message structure for a message type
func getMsg(t int, args ...interface{}) ([]byte, error) {
	return func(args []interface{}) ([]byte, error) {
		switch t {
		case MSG_INIT:
			m := InitMsg{}
			m.Cmd = MSG_INIT_STR
			return json.Marshal(m)
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
				log.Println("agent is", agent)
				m := ConfigMsg{Cmd: MSG_CONFIG_STR, Data: data, HeartBeatTime: t}
				return json.Marshal(m)
			} else {
				m := ConfigVbaMsg{Cmd: MSG_CONFIG_STR, HeartBeatTime: t}
				cp := cls.GetContext(ip)
				if cp == nil {
					break
				}
				for _, entry := range cp.VbaInfo {
					if strings.Split(entry.Source, ":")[0] == ip {
						if index := getServerIndex(cp, ip); index != -1 {
							if len(cp.C.SecondaryIps) > index {
								if secondIp := cp.C.SecondaryIps[index]; secondIp != "" {
									entry.Source = cp.C.SecondaryIps[index]
								}
							}
						}
						dest := strings.Split(entry.Destination, ":")[0]
						if dest != "" {
							if index := getServerIndex(cp, dest); index != -1 {
								if len(cp.C.SecondaryIps) > index {
									if secondIp := cp.C.SecondaryIps[index]; secondIp != "" {
										entry.Destination = cp.C.SecondaryIps[index]
									}
								}
							}
						}
						m.Data = append(m.Data, entry)
					}
				}
				return json.Marshal(m)
			}
		}
		//need to fix this here
		m := InitMsg{}
		m.Cmd = MSG_INIT_STR
		return json.Marshal(m)
	}(args)
}

//wait for VBA's to connect initially
func waitForVBAs(cls *config.Cluster, to int, co *Client) {
	time.Sleep(time.Duration(to) * time.Second)
	log.Println("sleep over for vbas")
	cls.M.Lock()
	for key, cfg := range cls.ConfigMap {
        serverList := cfg.Servers
		checkVBAs(&cfg, co.Vba)
		cp := &config.Context{}
		cp.GenMap(key, &cfg)
        cp.C.Servers = serverList
		cls.ContextMap[key] = cp
	}
	cls.M.Unlock()
	co.Started = true
	co.Cond.Broadcast()
}

func getServerIndex(cp *config.Context, sr string) int {
    sr = strings.Split(sr, ":")[0]
	for s := range cp.V.Smap.ServerList {
		if strings.Split(cp.V.Smap.ServerList[s], ":")[0] == sr {
			return s
		}
	}
	log.Println("Server not found.input server is", sr, "all are", cp.C.Servers)
	//panic("server not found")
	return -1
}

func getIpFromConfig(cp *config.Context, sr string) string {
	for s := range cp.C.Servers {
		if strings.Split(cp.C.Servers[s], ":")[0] == sr {
            log.Println("Found server in config list", sr, cp.C.Servers)
			return cp.C.Servers[s]
		}
	}
	return ""
}

//push the config to all the VBA's
func PushNewConfig(co *Client, m map[string]config.VbaEntry) {
	ma := make(map[string]int)
	for _, en := range m {
		if len(en.VbId) > 0 {
			co.Vba.Mu.Lock()
            ip := strings.Split(en.Source, ":")[0]
			if _, o := ma[ip]; o == false {
				if val, ok := co.Vba.Ma[ip]; ok {
					val.C <- CHN_NOTIFY_STR
				}
				ma[ip] = 1
			}
			co.Vba.Mu.Unlock()
		}
	}

	co.Moxi.Mu.Lock()
	for _, val := range co.Moxi.Ma {
		val.C <- CHN_NOTIFY_STR
	}
	co.Moxi.Mu.Unlock()
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
		}
	}
    //connectedServs = c.Servers
	log.Println("connect servers are", connectedServs)
	capacity := (len(connectedServs) * 100) / len(c.Servers)
	if capacity < CLIENT_PCNT {
		//XXX:May be need to change this panic
        log.Fatal("Not enough server connected, capacity is", capacity)
	} else {
        c.Servers = connectedServs
		//update the capacity in number of vbuckets
		c.Capacity = (int16(capacity) * c.Capacity) / 100
	}
}

func Insert(c net.Conn, ch chan string, co *Client, a string) {
	/*XXX:close the older connection if exists*/
	ip := getIpAddr(c)
	log.Println("insert ip is", ip)
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
	} else {
		co.Vba.Mu.Lock()
		if co.Vba.Ma[ip] == nil {
			cf := &ClientInfo{C: ch, Conn: c}
			co.Vba.Ma[ip] = cf
		} else {
			co.Vba.Ma[ip].C <- CHN_CLOSE_STR
			co.Vba.Ma[ip].C = ch
			co.Vba.Ma[ip].Conn = c
		}
		co.Vba.Mu.Unlock()
	}
}

func RemoveConn(c net.Conn, co *Client, ct string) {
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
			delete(co.Vba.Ma, ip)
		}
		co.Vba.Mu.Unlock()
	}
}
