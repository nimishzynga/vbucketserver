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
		panic(err)
	}
	defer fi.Close()
	//XXX:Need to increase the buffer size
	buf := make([]byte, RECV_BUF_LEN)
	buf, err = ioutil.ReadAll(fi)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(buf, &cls.ConfigMap); err != nil {
		log.Println("error in unmarshalling")
		return false
	}
	log.Println("Cluster config is ", cls)
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
			return errors.New("Duplcate Port Number in Config")
		}
		portMap[cfg.Port] = true
		if cfg.Replica < 0 || cfg.Capacity <= 0 || len(cfg.Servers) == 0 {
			return errors.New("Error in either replica, capacity or servers in Config")
		}
	}
	return nil
}

//return the client type
func getClientType(c net.Conn, co *Client) string {
	ip := getIpAddr(c)
	if cl := co.Moxi.Ma[ip]; cl != nil {
		return CLIENT_MOXI
	}
	if cl := co.Vba.Ma[ip]; cl != nil {
		return CLIENT_VBA
	}
	if cl := co.Cli.Ma[ip]; cl != nil {
		return CLIENT_CLI
	}
	log.Println("client type is unknown", ip, co.Vba.Ma)
	return CLIENT_UNKNOWN
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
                    log.Println("get msg vbamap is", cp.VbaInfo)
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
		checkVBAs(&cfg, co.Vba)
		cp := &config.Context{}
		cp.GenMap(key, &cfg)
		cls.ContextMap[key] = cp
	}
	cls.M.Unlock()
	co.Started = true
	co.Cond.Broadcast()
}

func getServerIndex(cp *config.Context, sr string) int {
	log.Println("input server is", sr, "all are", cp.C.Servers)
	for s := range cp.C.Servers {
		if strings.Split(cp.C.Servers[s], ":")[0] == sr {
			return s
		}
	}
	panic("server not found")
	return -1
}

//push the config to all the VBA's
func PushNewConfig(co *Client, m map[string]config.VbaEntry) {
	ma := make(map[string]int)
	for _, en := range m {
		if len(en.VbId) > 0 {
			co.Vba.Mu.Lock()
			if _, o := ma[en.Source]; o == false {
				if val, ok := co.Vba.Ma[en.Source]; ok {
					val.C <- CHN_NOTIFY_STR
				}
				ma[en.Source] = 1
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
func checkVBAs(c *config.Config, v ClientInfoMap) *config.Config {
	v.Mu.RLock()
	defer v.Mu.RUnlock()
	connectedServs := []string{}
    /*
	for a := range c.Servers {
		//Need to uncomment this
		if _, ok := v.Ma[c.Servers[a]]; ok {
			connectedServs = append(connectedServs, c.Servers[a])
		}
	}*/
    connectedServs = c.Servers
	log.Println("connect servers are", connectedServs)
	capacity := len(connectedServs) * 100 / len(c.Servers)
	if capacity < CLIENT_PCNT {
		//XXX:May be need to change this panic
		log.Fatal("Not enough server connected")
	} else {
		c.Servers = connectedServs
		//update the capacity in number of vbuckets
		c.Capacity = (int16(capacity) * c.Capacity) / 100
	}
	return c
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
