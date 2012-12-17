package clientHandler1

import (
	"net"
    "log"
	"vbucketserver/conf"
)

type GenericClient struct {
}

func (gc *GenericClient) ClientType() string {
	return CLIENT_UNKNOWN
}

func (gc *GenericClient) HandleOk(m *RecvMsg) bool {
	if m.Status == MSG_OK_STR {
		return true
	}
	return false
}

func (gc *GenericClient) HandleAlive(m *RecvMsg) bool {
	if m.Cmd == MSG_ALIVE_STR {
		return true
	}
	return false
}

func (gc *GenericClient) HandleInit(ch chan string, cp *conf.ParsedInfo, co *Client, c int) bool {
	_ = ch
	_ = cp
	_ = co
	_ = c
	return false
}

func (gc *GenericClient) HandleFail(m *RecvMsg, cp *conf.ParsedInfo, co *Client) bool {
	_ = cp
	_ = m
	_ = co
	log.Println("invalid client")
	return false
}

func (gc *GenericClient) HandleUpdateConfig(cp *conf.ParsedInfo) bool {
	_ = gc
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

//it shoudl be for mcs client
func (mc *MoxiClient) HandleFail(m *RecvMsg, cp *conf.ParsedInfo, co *Client) bool {
	log.Println("inside handleFail")
	ok, mp := cp.HandleServerDown(m.Server)
	if ok {
		//need to call it on client info
		go PushNewConfig(co, mp)
	}
	return true
}

func (mc *MoxiClient) HandleInit(ch chan string, cp *conf.ParsedInfo, co *Client, c int) bool {
	_ = c
	Insert(mc.conn, ch, co, CLIENT_MOXI)
	co.Con.L.Lock()
	for co.Started == false {
		co.Con.Wait()
	}
	co.Con.L.Unlock()
	if m, err := getMsg(MSG_CONFIG, cp, HBTIME, CLIENT_MOXI, getIpAddr(mc.conn)); err == nil {
		mc.ch <- m
		return true
	}
	return false
}

func (mc *MoxiClient) HandleUpdateConfig(cp *conf.ParsedInfo) bool {
	if m, err := getMsg(MSG_CONFIG, cp, HBTIME, CLIENT_MOXI, getIpAddr(mc.conn)); err == nil {
		mc.ch <- m
		return true
	}
	return false
}

type VbaClient struct {
	conn net.Conn
	ch   chan []byte
	GenericClient
}

func (vc *VbaClient) ClientType() string {
	return CLIENT_VBA
}

func (vc *VbaClient) HandleInit(ch chan string, cp *conf.ParsedInfo, co *Client, capacity int) bool {
	Insert(vc.conn, ch, co, CLIENT_VBA)
	co.Con.L.Lock()
	for co.Started == false {
		co.Con.Wait()
	}
	co.Con.L.Unlock()
	index := getServerIndex(cp, getIpAddr(vc.conn))
	if index == -1 {
		log.Println("Server not in list", getIpAddr(vc.conn))
		return false
	}
	si := cp.S[index]
	si.NumberOfDisc = int16(capacity)
	cp.S[index] = si

	if m, err := getMsg(MSG_CONFIG, cp, HBTIME, CLIENT_VBA, getIpAddr(vc.conn)); err == nil {
		vc.ch <- m
		return true
	}
	return false
}

func (vc *VbaClient) HandleUpdateConfig(cp *conf.ParsedInfo) bool {
	if m, err := getMsg(MSG_CONFIG, cp, HBTIME, CLIENT_VBA, getIpAddr(vc.conn)); err == nil {
		vc.ch <- m
		return true
	}
	return false
}
