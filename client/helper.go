//contains the helper functions 
package clientHandler

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"time"
	"vbucketserver/conf"
)

///XXX:Put more validation checks for file inputs
func parseInitialConfig(f string, cp *conf.ParsedInfo) *conf.Conf {
	con := &conf.Conf{}
	fi, err := os.Open(f)
	if err != nil {
		//XXX:may be need to change here
		panic(err)
	}
	defer fi.Close()
	buf := make([]byte, RECV_BUF_LEN)
	_, err = fi.Read(buf)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if err := json.Unmarshal(buf, con); err != nil {
		println("error in unmarshalling")
		return nil
	}
	return con
}

//return the client type
func getClientType(c net.Conn, co *Client) string {
	ip := strings.Split(c.LocalAddr().String(), ":")[0]

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
		var mar interface{}
		switch t {
		case MSG_INIT:
			m := &InitMsg{}
			m.Cmd = MSG_INIT_STR
			mar = t
		case MSG_CONFIG:
			cp, _ := args[0].(*conf.ParsedInfo)
			t, _ := args[1].(int)
			cp.M.RLock()
			defer cp.M.RUnlock()
			m := &ConfigMsg{Config: cp.V, HeartBeatTime: t}
			mar = m
		}
		return json.Marshal(mar)
	}(args)
}

//wait for VBA's to connect initially
func waitForVBAs(c *conf.Conf, cp *conf.ParsedInfo, to int, co *Client) {
	time.Sleep(time.Duration(to) * time.Second)
	checkVBAs(c, co.Vba)
	cp.GenMap(c)
	co.Started = true
	co.Con.Signal()
}

//push the config to all the VBA's
func PushNewConfig(m map[string]conf.VbaEntry) {
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
	capacity := len(connectedServs) * 100 / len(c.Servers)
	if capacity < CLIENT_PCNT {
		//XXX:May be need to change this panic
		panic("Not enough server connected")
	} else {
		c.Servers = connectedServs
		//update the capacity in number of vbuckets
		//    c.Capacity = capacity * 100/(int16)c.Capacity
	}
	return c
}

func Insert(c net.Conn, ch chan []byte, co *Client, a string) {
	/*XXX:close the older connection if exists*/
	ip := strings.Split(c.RemoteAddr().String(), ":")[0]
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
		co.Vba.Mu.Lock()
		if co.Vba.Ma[ip] != nil {
			cf := &ClientInfo{C: ch}
			co.Vba.Ma[ip] = cf
		} else {
			co.Vba.Ma[ip].C = ch
		}
		co.Vba.Mu.Unlock()
	}
}
