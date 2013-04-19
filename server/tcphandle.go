package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"net"
	"os"
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
)

//msg types and strings
const (
	MSG_INVALID = iota
	MSG_INIT
	MSG_ALIVE
	MSG_CAPACITY
	MSG_CONFIG
	MSG_INIT_STR   = "INIT"
	MSG_OK_STR     = "OK"
	MSG_ALIVE_STR  = "ALIVE"
	MSG_CONFIG_STR = "CONFIG"
	MSG_FAIL_STR   = "FAIL"
	MSG_DEAD_VB_STR  = "DEAD_VBUCKETS"
    MSG_ERROR_STR  = "ERROR"
    MSG_TRANSFER_STR = "TRANSFER_DONE"
    MSG_CKPOINT_STR = "CKPOINT"
)

//other constants
const (
	RECV_BUF_LEN   = 1024
	HEADER_SIZE    = 4
	HBTIME         = 30
	MAX_TIMEOUT    = 3
	VBA_WAIT_TIME  = 30
	CHN_NOTIFY_STR = "NOTIFY"
	CHN_CLOSE_STR  = "CLOSE"
	CLIENT_VBA     = "VBA"
	CLIENT_MOXI    = "MOXI"
	CLIENT_CLI     = "Cli"
	CLIENT_UNKNOWN = "Unknown"
	CLIENT_PCNT    = 10
)

//return status
const (
	STATUS_INVALID = iota
	STATUS_CONT
	STATUS_ERR
	STATUS_SUCCESS
)

type VbsClient interface {
	ClientType() string
	HandleInit(chan string, *config.Cluster, *Client, int) bool
	HandleFail(*RecvMsg, *config.Cluster, *Client) bool
	HandleOk(*config.Cluster, *Client, *RecvMsg) bool
	HandleAlive(*config.Cluster, *Client, *RecvMsg) bool
	HandleUpdateConfig(*config.Cluster) bool
    HandleCheckPoint(*RecvMsg, *config.Cluster) bool
    HandleDeadvBuckets(m *RecvMsg, cls *config.Cluster, co *Client) bool
}

func HandleTcp(c *Client, cls *config.Cluster, s string, confFile string) {
	listener, err := net.Listen("tcp", s)
	if err != nil {
		log.Println("error listening:", err.Error())
		os.Exit(1)
	}
	//parse the conf file
	if ok := parseInitialConfig(confFile, cls); ok == false {
		log.Println("Unable to parse the config")
		return
	}
	//wait for VBA's to connect
	go waitForVBAs(cls, VBA_WAIT_TIME, c)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accept:", err.Error())
			return
		}
		go handleConn(conn, c, cls)
	}
}

func handleConn(conn net.Conn, co *Client, cls *config.Cluster) {
	ch := make(chan []byte)
	go handleWrite(conn, ch)
	handleRead(conn, ch, co, cls)
}

func handleWrite(conn net.Conn, ch chan []byte) {
	for m := range ch {
		l := new(bytes.Buffer)
		var ln int32 = int32(len(m))
		binary.Write(l, binary.BigEndian, ln)
		conn.Write(l.Bytes())
		_, err := conn.Write(m)
		if err != nil {
			log.Println("Error write", err.Error())
			return
		}
	}
}

func handleRead(conn net.Conn, c chan []byte, co *Client, cls *config.Cluster) {
	var state int = STATE_INIT_RES
	data := []byte{}
	fullData := []byte{}
	var length int32
	currTimeouts := 0
	var vc VbsClient
	var err error

	defer func() {
		conn.Close()
		log.Println("disconnecting client", getIpAddr(conn))
        if vc != nil {
            cp := cls.GetContext(getIpAddr(conn))
            if cp == nil {
                log.Println("Not able to find context for", getIpAddr(conn))
            } else if si := getServerIndex(cp, getIpAddr(conn)); si == -1 {
                log.Println("handleRead server not found", getIpAddr(conn))
            } else {
                m := &RecvMsg{Server:cp.V.Smap.ServerList[si],}
                vc.HandleFail(m, cls, co)
            }
            RemoveConn(conn, co, vc.ClientType())
        }
        close(c)
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

	SendInitMsg(c)

	for {
		select {
        case err := <-c1:
			log.Println("error on socket", err)
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
		case <-time.After(HBTIME * time.Second):
			currTimeouts++
			if state != STATE_ALIVE || currTimeouts > MAX_TIMEOUT {
				return
			}
			log.Println("timeout on socket", getIpAddr(conn))
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
							log.Println("Data size is more", length)
							return
						} else {
                            log.Println("got length", length)
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

				m, err = parseMsg(input)
				if err != nil {
					return
				}
			}

			if state == STATE_INIT_RES {
                log.Println("got response for init", *m)
				vc = getClient(m.Agent, conn, c)
			}

			switch ret := handleMsg(m, conn, &state, c, co, cls, c3, vc); ret {
			case STATUS_ERR:
				return
			case STATUS_SUCCESS:
				hasData = false
				m = nil
			}
		}
	}
}

func parseMsg(b []byte) (*RecvMsg, error) {
	m := &RecvMsg{}
	var err error
	if err = json.Unmarshal(b, m); err != nil {
		log.Println("error in unmarshalling")
	}
	return m, err
}

func getClient(ct string, conn net.Conn, ch chan []byte) VbsClient {
	if ct == CLIENT_MOXI {
		g := &MoxiClient{conn: conn, ch: ch}
		return g
	} else if ct == CLIENT_VBA {
		g := &VbaClient{conn: conn, ch: ch}
		return g
	}
	g := &GenericClient{}
	return g
}

func handleMsg(m *RecvMsg, c net.Conn, s *int, ch chan []byte, co *Client,
cls *config.Cluster, i chan string, vc VbsClient) int {
    if m != nil {
        log.Println("in handleMsg msg state",*m,*s)
        if vc != nil {
            if m.Cmd == MSG_FAIL_STR {
                vc.HandleFail(m, cls, co)
                return STATUS_SUCCESS
            } else if m.Cmd == MSG_CKPOINT_STR {
                vc.HandleCheckPoint(m, cls)
                return STATUS_SUCCESS
            } else if m.Cmd == MSG_DEAD_VB_STR {
                vc.HandleDeadvBuckets(m, cls, co)
                return STATUS_SUCCESS
            }
        }
    }
    switch *s {
    case STATE_INIT_RES:
        vc.HandleInit(i, cls, co, m.Capacity)
        *s = STATE_CONFIG_RES
        break;

    case STATE_CONFIG_RES:
        if vc.HandleOk(cls, co, m) {
            *s = STATE_ALIVE
        }
        break;

    case STATE_ALIVE:
        if vc.HandleAlive(cls, co, m) == false {
            return STATUS_ERR
        }
        break;

    case STATE_UPDATE_CONFIG:
        if vc.HandleUpdateConfig(cls) == false {
            return STATUS_ERR
        }
        *s = STATE_CONFIG_RES
        break;
    }
    return STATUS_SUCCESS
}

func SendInitMsg(ch chan []byte) {
	if m, err := getMsg(MSG_INIT); err == nil {
		ch <- m
	}
}
