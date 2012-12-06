//contains the helper functions 
package clientHandler

import (
	"encoding/json"
    "io/ioutil"
	"net"
	"os"
	"strings"
	"time"
	"vbucketserver/conf"
    "fmt"
)

func getIpAddr(c net.Conn) string {
    ip := strings.Split(c.RemoteAddr().String(), ":")[0]
    return ip
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
	buf,err = ioutil.ReadAll(fi)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(buf, con); err != nil {
		println("error in unmarshalling")
		return nil
	}
    if con.Replica <= 0 || con.Capacity <= 0 || len(con.Servers) == 0 {
        println("Invalid configuration data")
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
            if agent == CLIENT_MOXI {
                m := ConfigMsg{Config: cp.V, HeartBeatTime: t}
                return json.Marshal(m)
            } else {
                m := ConfigVbaMsg{HeartBeatTime: t}
                for _,entry := range cp.VbaInfo {
                    if entry.Source == ip {
                        m.Config = append(m.Config, entry)
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
    println("sleep over for vbas")
	checkVBAs(c, co.Vba)
	cp.GenMap(c)
	co.Started = true
	co.Con.Broadcast()
}

func getServerIndex(cp *conf.ParsedInfo, sr string) int {
    for s := range cp.C.Servers {
        if cp.C.Servers[s] == sr {
            return s
        }
    }
    return -1
}

//push the config to all the VBA's
func PushNewConfig(co *Client, m map[string]conf.VbaEntry) {
    for _,en := range m {
        co.Vba.Mu.Lock()
        if val,ok := co.Vba.Ma[en.Source];ok {
            val.C<-'a'
        }
        co.Vba.Mu.Unlock()
    }
}

//update the servers list with the connected servers
func checkVBAs(c *conf.Conf, v ClientInfoMap) *conf.Conf {
	v.Mu.RLock()
	defer v.Mu.RUnlock()
	connectedServs := []string{}
	for a := range c.Servers {
		if _, ok := v.Ma[c.Servers[a]]; ok {
			connectedServs = append(connectedServs, c.Servers[a])
		}
	}
    fmt.Println("connect servers are", connectedServs)
	capacity := len(connectedServs) * 100 / len(c.Servers)
    _= capacity
    /*
	if capacity < CLIENT_PCNT {
		//XXX:May be need to change this panic
		panic("Not enough server connected")
	} else {
		c.Servers = connectedServs
		//update the capacity in number of vbuckets
		//    c.Capacity = capacity * 100/(int16)c.Capacity
	}
    */
	return c
}

func Insert(c net.Conn, ch chan byte, co *Client, a string) {
	/*XXX:close the older connection if exists*/
	ip := getIpAddr(c)
	if a == CLIENT_MOXI {
		co.Moxi.Mu.Lock()
		if co.Moxi.Ma[ip] != nil {
			cf := &ClientInfo{C: ch}
			co.Moxi.Ma[ip] = cf
		} else {
			co.Moxi.Ma[ip].C = ch
		}
		co.Moxi.Mu.Unlock()
	} else {
        println("dude, inserting here")
		co.Vba.Mu.Lock()
		if co.Vba.Ma[ip] == nil {
			cf := &ClientInfo{C: ch}
			co.Vba.Ma[ip] = cf
		} else {
			co.Vba.Ma[ip].C = ch
		}
		co.Vba.Mu.Unlock()
	}
}
