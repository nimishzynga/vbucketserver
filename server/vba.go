package server

import (
	"log"
	"net"
	"vbucketserver/config"
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
    log.Println("inside handleAlive")
    if m.Cmd == MSG_ALIVE_STR {
		return true
	}
	return false
}

func (gc *GenericClient) HandleInit(ch chan string, cp *config.Cluster, co *Client, c int) bool {
	return false
}

func (gc *GenericClient) HandleFail(m *RecvMsg, cp *config.Cluster, co *Client) bool {
	log.Println("invalid client")
	return false
}

func (gc *GenericClient) HandleUpdateConfig(cp *config.Cluster) bool {
	return false
}

func (gc *GenericClient) HandleCheckPoint(m *RecvMsg, cls *config.Cluster) bool {
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
		mc.ch <- m
		return true
	}
	return false
}

func (mc *MoxiClient) HandleUpdateConfig(cls *config.Cluster) bool {
	if m, err := getMsg(MSG_CONFIG, cls, HBTIME, CLIENT_MOXI, getIpAddr(mc.conn)); err == nil {
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
		log.Println("Not able to find context for", ip)
		return false
	}
	index := getServerIndex(cp, ip)
	if index == -1 {
        if ip := getIpFromConfig(cp, ip);ip != "" {
            //TODO:need to fix the secondary ip in the call
            cp.HandleServerAlive([]string{ip}, nil, false)
	        index = getServerIndex(cp, ip)
        } else {
            log.Println("Server not in list", ip)
            return false
        }
	}
	si := cp.S[index]
	si.NumberOfDisk = int16(capacity)
    si.MaxVbuckets = cp.Maxvbuckets
	cp.S[index] = si

    if ok, mp := cp.NeedRebalance(index); ok {
        PushNewConfig(co, mp, true)
        return true
    }
	if m, err := getMsg(MSG_CONFIG, cls, HBTIME, CLIENT_VBA, ip); err == nil {
		vc.ch <- m
		return true
	}
	return false
}

func (vc *VbaClient) HandleUpdateConfig(cls *config.Cluster) bool {
	if m, err := getMsg(MSG_CONFIG, cls, HBTIME, CLIENT_VBA, getIpAddr(vc.conn)); err == nil {
		vc.ch <- m
		return true
	}
	return false
}

func (vc *VbaClient) HandleOk(cls *config.Cluster, co *Client, m *RecvMsg) bool {
	if m.Status == MSG_OK_STR {
        cp := cls.GetContext(getIpAddr(vc.conn))
        log.Println("before contxt got ok from", getIpAddr(vc.conn))
	    if cp == nil {
		    log.Println("Not able to find context for", getIpAddr(vc.conn))
		    return false
	    }
        log.Println("got ok from", getIpAddr(vc.conn))
        ip := cp.HandleRestoreCheckPoints(m.Vbuckets, m.CheckPoints, getIpAddr(vc.conn))
        go PushNewConfigToVBA(co, ip)
		return true
	}
    if m.Status == MSG_ERROR_STR {
        log.Println("VBA ERROR:", m.Detail)
        /*dont disconnect VBA if error comes*/
        return true
    }
	return false
}

func (vc *VbaClient) HandleFail(m *RecvMsg, cls *config.Cluster, co *Client) bool {
	log.Println("inside handleFail")
	cp := cls.GetContext(getIpAddr(vc.conn))
	if cp == nil {
		log.Println("Not able to find context for", getIpAddr(vc.conn))
		return false
	}
	ok, mp := cp.HandleServerDown([]string{m.Server})
	if ok {
		//need to call it on client info
		go PushNewConfig(co, mp, true)
	}
	return true
}

func (vc *VbaClient) HandleAlive(cls *config.Cluster, co *Client, m *RecvMsg) bool {
    log.Println("inside handleAlive")
    if m.Cmd == MSG_ALIVE_STR {
		return true
	}
    if m.Cmd == MSG_TRANSFER_STR {
        log.Println("calling HandleTransferDone")
        changeMap := cls.HandleTransferDone(getIpAddr(vc.conn), m.Destination, m.Vbuckets)
        //push config only to VBA
        go PushNewConfig(co, changeMap, false)
        return true
    }
    if m.Status == MSG_ERROR_STR {
        log.Println("VBA ERROR:", m.Detail)
        /*dont disconnect VBA if error comes*/
        return true
    }
	return false
}

func (vc *VbaClient) HandleCheckPoint(m *RecvMsg, cls *config.Cluster) bool {
	log.Println("inside handleCheckPoint")
	cp := cls.GetContext(getIpAddr(vc.conn))
	if cp == nil {
		log.Println("Not able to find context for", getIpAddr(vc.conn))
		return false
	}
	cp.HandleCheckPoint(getIpAddr(vc.conn), m.Vbuckets, m.CheckPoints)
    return true
}
