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
	"bytes"
	"encoding/binary"
	"os"
net "vbucketserver/net"
	"time"
	"vbucketserver/config"
)

//client states
const (
	STATE_INVALID = iota
	STATE_INIT
	STATE_INIT_RES
	STATE_CONFIG
	STATE_CONFIG_RES
	STATE_UPDATE_CONFIG
	STATE_CONFIG_RES_NXT
	STATE_ALIVE
    STATE_HB
)

//msg types and strings
const (
	MSG_INVALID = iota
	MSG_INIT
	MSG_ALIVE
	MSG_CAPACITY
	MSG_CONFIG
    MSG_REPLICA_VBS
    MSG_HB
	MSG_INIT_STR   = "INIT"
	MSG_OK_STR     = "OK"
	MSG_ALIVE_STR  = "ALIVE"
	MSG_CONFIG_STR = "CONFIG"
	MSG_FAIL_STR   = "FAIL"
	MSG_DEAD_VB_STR  = "DEAD_VBUCKETS"
    MSG_ERROR_STR  = "ERROR"
    MSG_TRANSFER_STR = "TRANSFER_DONE"
    MSG_CKPOINT_STR = "CKPOINT"
    MSG_REP_FAIL_STR = "REPLICATION_FAIL"
    MSG_REPLICA_CONFIG_STR = "REPLICA_CONFIG"
)

//other constants
const (
	RECV_BUF_LEN   = 1024*10
	HEADER_SIZE    = 4
	HBTIME         = 30
	MAX_TIMEOUT    = 3
	VBA_WAIT_TIME  = 30
	CHN_NOTIFY_STR = "NOTIFY"
	CHN_CLOSE_STR  = "CLOSE"
	CLIENT_VBA     = "VBA"
	CLIENT_MOXI    = "MOXI"
	CLIENT_REPLICA_VBS = "REPLICA_VBS"
	CLIENT_CLI     = "Cli"
	CLIENT_UNKNOWN = "Unknown"
	CLIENT_PCNT    = 10
    AGGREGATE_TIME  = 30
	FAIL_CHECK_TIME = 30
	FAILOVER_TIME   = 120
)

//return status
const (
	STATUS_INVALID = iota
	STATUS_CONT
	STATUS_ERR
    STATUS_CLOSE_CONN
	STATUS_SUCCESS
)

type VbsClient interface {
	ClientType() string
	HandleInit(chan string, *config.Cluster, *Client, int) bool
	HandleFail(*RecvMsg, *config.Cluster, *Client) bool
	HandleOk(*config.Cluster, *Client, *RecvMsg) bool
	HandleAlive(*config.Cluster, *Client, *RecvMsg) bool
	HandleUpdateConfig(*config.Cluster) bool
	HandleHBSend(*config.Cluster) bool
    HandleCheckPoint(*RecvMsg, *config.Cluster) bool
    HandleDeadvBuckets(m *RecvMsg, cls *config.Cluster, co *Client) bool
}

func HandleTcp(c *Client, cls *config.Cluster, s string, confFile string) {
    //parse the conf file
    genConfig := true
    if ok := parseInitialConfig(confFile, cls); ok == false {
		logger.Fatalf("Unable to parse the config")
		return
	}
    if cls.IsReplicaVbs() {
        genConfig = false
        HandleReplicaConfig(cls, c)
	}
    listener, err := net.Listen("tcp", s)
    if err != nil {
        logger.Fatalf("error listening:", err.Error())
        os.Exit(1)
    }
    //wait for VBA's to connect
    go waitForVBAs(cls, VBA_WAIT_TIME, c, genConfig)
    for {
        conn, err := listener.Accept()
        if err != nil {
            logger.Fatalf("Error accept:", err.Error())
            return
        }
        go handleConn(conn, c, cls)
    }
}

func HandleTcpDebug(c *Client, cls *config.Cluster, s string, confFile string) {
    //parse the conf file
	if ok := parseInitialConfig(confFile, cls); ok == false {
		logger.Fatalf("Unable to parse the config")
		return
	}
	listener, err := net.ListenDebug("tcp", s)
    if err != nil {
		logger.Fatalf("error listening:", err.Error())
		os.Exit(1)
	}
	//wait for VBA's to connect
	go waitForVBAs(cls, VBA_WAIT_TIME, c, true)
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Fatalf("Error accept:", err.Error())
			return
		}
		go handleConn(conn, c, cls)
	}
}

func handleConn(conn net.Conn, co *Client, cls *config.Cluster) {
	ch := make(chan interface{})
	go handleWrite(conn, ch)
	handleRead(conn, ch, co, cls)
}

func handleWrite(conn net.Conn, ch chan interface{}) {
	for data := range ch {
        logger.Debugf("doing write", data)
        m ,err := conn.Marshal(data)
        if err != nil {
            logger.Warnf("Error in marshalling", err)
            return
        }
		l := new(bytes.Buffer)
		var ln int32 = int32(len(m))
		binary.Write(l, binary.BigEndian, ln)
		conn.Write(l.Bytes())
		_, err = conn.Write(m)
		if err != nil {
            logger.Warnf("Error in write:", err.Error(), " connection ",getIpAddr(conn))
			return
		}
	}
}

func handleRead(conn net.Conn, c chan interface{}, co *Client, cls *config.Cluster) {
	var state int = STATE_INIT_RES
	data := []byte{}
	fullData := []byte{}
	var length int32
	currTimeouts := 0
	var vc VbsClient
	var err error

	defer func() {
		logger.Infof("disconnecting client", getIpAddr(conn))
        if vc != nil {
            var cp *config.Context = nil
            if vc.ClientType() == CLIENT_VBA {
                cp = cls.GetContext(getIpAddr(conn))
                if cp == nil {
                    logger.Warnf("Not able to find context for", getIpAddr(conn))
                }
            }
            RemoveConn(conn, co, vc.ClientType(), cp)
        }
        close(c)
		conn.Close()
	}()

    m := &RecvMsg{}
    hasData := false
    c1 := make(chan error)
    c2 := make(chan []byte)
    c3 := make(chan string, 10)

	go func() {
		d := make([]byte, RECV_BUF_LEN)
		for {
			n, err := conn.Read(d)
			if err != nil {
				c1 <- err
			} else {
				c2 <- d[:n]
			}
		}
	}()

    if cls.IsActiveVbs() {
	    SendInitMsg(c)
    }

	for {
		select {
        case err := <-c1:
            logger.Warnf("error on socket:", err, getIpAddr(conn))
			return
		case data = <-c2:
			currTimeouts = 0
			fullData = append(fullData, data...)
		case info := <-c3:
			if info == CHN_NOTIFY_STR {
				state = STATE_UPDATE_CONFIG
				hasData = true
			} else if info == CHN_CLOSE_STR {
				return
			}
		case <-time.After((HBTIME) * time.Second):
            if cls.IsReplicaVbs() {
				hasData = true
                state = STATE_HB
                break
            }
			currTimeouts++
			logger.Warnf("timeout on socket", getIpAddr(conn))
			if currTimeouts > MAX_TIMEOUT {
				return
			}
			logger.Warnf("timeout on socket", getIpAddr(conn))
			length = 0
			fullData = fullData[:0]
			continue
		}

		for {
			if hasData == false {
				n := len(fullData)
				if length == 0 {
					if n > HEADER_SIZE {
						buf := bytes.NewBuffer(fullData)
						binary.Read(buf, binary.BigEndian, &length)
						if length > RECV_BUF_LEN {
							logger.Warnf("Data size is more", length, fullData)
							return
						}
                        if length <= 0 {
							logger.Warnf("Data size is negative", length, fullData)
							return
                        }
						fullData = fullData[HEADER_SIZE:]
						n -= HEADER_SIZE
					} else {
						break
					}
				}

				if n < int(length) {
					break
				}

				input := fullData[:length]
				fullData = fullData[length:]
				length = 0
				m, err = parseMsg(conn, input)
				if err != nil {
					return
				}
			}

			if state == STATE_INIT_RES {
                if cls.IsReplicaVbs() {
                    m.Agent = CLIENT_REPLICA_VBS
                }
                logger.Debugf("got response for init", *m)
				vc = getClient(m.Agent, conn, c, cls)
			}

			switch ret := handleMsg(m, conn, &state, c, co, cls, c3, vc); ret {
			case STATUS_ERR:
                logger.Infof("handleMsg returned error")
				hasData = false
				m = nil
			case STATUS_SUCCESS:
				hasData = false
				m = nil
            case STATUS_CLOSE_CONN:
                return
			}
		}
	}
}

func parseMsg(conn net.Conn ,b []byte) (*RecvMsg, error) {
	m := &RecvMsg{}
	var err error
    if err = conn.Unmarshal(b, m); err != nil {
        logger.Warnf("error in unmarshalling", err)
    }
	return m, err
}

func getClient(ct string, conn net.Conn, ch chan interface{}, cls *config.Cluster) VbsClient {
	if ct == CLIENT_MOXI {
		g := &MoxiClient{conn: conn, ch: ch}
		return g
	} else if ct == CLIENT_VBA {
		g := &VbaClient{conn: conn, ch: ch}
		return g
	} else if ct == CLIENT_REPLICA_VBS {
        g := &ReplicaClient{conn: conn, ch: ch, cls: cls}
        return g
    }
    logger.Warnf("Invalid client type ", ct)
	g := &GenericClient{}
	return g
}

func handleMsg(m *RecvMsg, c net.Conn, s *int, ch chan interface{}, co *Client,
cls *config.Cluster, i chan string, vc VbsClient) int {
    if m != nil {
        logger.Debugf("Received msg", m)
        if vc != nil {
            if m.Cmd == MSG_FAIL_STR || m.Cmd == MSG_REP_FAIL_STR {
                vc.HandleFail(m, cls, co)
                return STATUS_SUCCESS
            } else if m.Cmd == MSG_CKPOINT_STR {
                vc.HandleCheckPoint(m, cls)
                return STATUS_SUCCESS
            } else if m.Cmd == MSG_DEAD_VB_STR {
                vc.HandleDeadvBuckets(m, cls, co)
                return STATUS_SUCCESS
            } else if m.Cmd == MSG_ALIVE_STR {
               if vc.HandleAlive(cls, co, m) == false {
                    return STATUS_ERR
                }
                return STATUS_SUCCESS
            } else if m.Cmd == MSG_REPLICA_CONFIG_STR {
                if cls.IsActiveVbs() {
                    return STATUS_CLOSE_CONN
                }
                handleConfig(cls, m)
                return STATUS_SUCCESS
            }
        }
    }
    switch *s {
    case STATE_INIT_RES:
        vc.HandleInit(i, cls, co, m.Capacity)
        *s = STATE_CONFIG_RES

    case STATE_CONFIG_RES:
        if vc.HandleOk(cls, co, m) == false {
            return STATUS_ERR
        }

    case STATE_UPDATE_CONFIG:
        if vc.HandleUpdateConfig(cls) == false {
            return STATUS_ERR
        }
        *s = STATE_CONFIG_RES

    case STATE_HB:
        if vc.HandleHBSend(cls) == false {
            return STATUS_ERR
        }
        *s = STATE_CONFIG_RES
    }
    return STATUS_SUCCESS
}

func SendInitMsg(ch chan interface{}) {
	if m, err := getMsg(MSG_INIT); err == nil {
		ch <- m
	}
}

func HandleReplicaConfig(cls *config.Cluster, c *Client) {
    for {
        conn, err := net.DialTimeout("tcp", cls.ActiveIp, 120*time.Second)
        if err != nil {
            logger.Debugf("Error in connecting to active VBS")
            time.Sleep(60*time.Second)
        } else {
            handleConn(conn, c, cls)
        }
        if cls.IsActiveVbs() {
            break
        }
    }
}

func handleConfig(cls *config.Cluster, m *RecvMsg) {
    logger.Debugf("Replica got config before", cls.ContextMap)
    cls.HandleReplicaConfig(m.Cls)
    for a,_ := range cls.ContextMap {
        logger.Debugf("replica config", a)
    }
    logger.Debugf("Replica got config after", cls.ContextMap)
}
