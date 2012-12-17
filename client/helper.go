//contains the helper functions 
package clientHandler1

import (
	l4g "code.google.com/p/log4go"
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"
	"vbucketserver/conf"
)

var Log l4g.Logger

func getIpAddr(c net.Conn) string {
	ip := strings.Split(c.RemoteAddr().String(), ":")[0]
	//ip := c.RemoteAddr().String()
	return ip
}

func getIpAddrWithPort(c net.Conn) string {
	ip := c.RemoteAddr().String()
	return ip
}

func SetLogFile(file string, ll string) {
	Log = make(l4g.Logger)
	level := l4g.WARNING
	if ll == "info" {
		level = l4g.INFO
	} else if ll == "debug" {
		level = l4g.DEBUG
	}
	Log.AddFilter("file", level, l4g.NewFileLogWriter(file, false))
}

///XXX:Put more validation checks for file inputs
func parseInitialConfig(f string, cp *conf.ParsedInfo) *conf.Conf {
	con := &conf.Conf{}
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
	if err := json.Unmarshal(buf, con); err != nil {
		Log.Debug("error in unmarshalling")
		return nil
	}
	if con.Replica <= 0 || con.Capacity <= 0 || len(con.Servers) == 0 {
		Log.Debug("Invalid configuration data")
		return nil
	}
	return con
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
	Log.Debug("client type is unknown", ip, co.Vba.Ma)
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
			cp, _ := args[0].(*conf.ParsedInfo)
			t, _ := args[1].(int)
			agent, _ := args[2].(string)
			ip, _ := args[3].(string)
			cp.M.RLock()
			defer cp.M.RUnlock()
			Log.Debug("agent is", agent)
			if agent == CLIENT_MOXI {
				m := ConfigMsg{Cmd: MSG_CONFIG_STR, Data: cp.V, HeartBeatTime: t}
				return json.Marshal(m)
			} else {
				m := ConfigVbaMsg{Cmd: MSG_CONFIG_STR, HeartBeatTime: t}
				for _, entry := range cp.VbaInfo {
					if strings.Split(entry.Source, ":")[0] == ip {
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
func waitForVBAs(c *conf.Conf, cp *conf.ParsedInfo, to int, co *Client) {
	time.Sleep(time.Duration(to) * time.Second)
	Log.Debug("sleep over for vbas")
	checkVBAs(c, co.Vba)
	cp.GenMap(c)
	co.Started = true
	co.Con.Broadcast()
}

func getServerIndex(cp *conf.ParsedInfo, sr string) int {
	Log.Debug("input server is", sr, "all are", cp.C.Servers)
	for s := range cp.C.Servers {
		Log.Debug(strings.Split(cp.C.Servers[s], ":")[0])
		if strings.Split(cp.C.Servers[s], ":")[0] == sr {
			return s
		}
	}
	panic("server not found")
	return -1
}

//push the config to all the VBA's
func PushNewConfig(co *Client, m map[string]conf.VbaEntry) {
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
func checkVBAs(c *conf.Conf, v ClientInfoMap) *conf.Conf {
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
	Log.Debug("connect servers are", connectedServs)
	capacity := len(connectedServs) * 100 / len(c.Servers)

	if capacity < CLIENT_PCNT {
		//XXX:May be need to change this panic
		//panic("Not enough server connected")
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
	Log.Debug("insert ip is", ip)
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
