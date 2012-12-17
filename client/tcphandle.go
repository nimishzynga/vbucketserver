package clientHandler1

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"time"
	"vbucketserver/conf"
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
	CLIENT_PCNT    = 1
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
	HandleInit(chan string, *conf.ParsedInfo, *Client, int) bool
	HandleFail(*RecvMsg, *conf.ParsedInfo, *Client) bool
	HandleOk(*RecvMsg) bool
	HandleAlive(*RecvMsg) bool
	HandleUpdateConfig(*conf.ParsedInfo) bool
}

func HandleTcp(c *Client, cp *conf.ParsedInfo, s string, confFile string) {
	listener, err := net.Listen("tcp", s)
	if err != nil {
		Log.Debug("error listening:", err.Error())
		os.Exit(1)
	}
	//parse the conf file
	con := parseInitialConfig(confFile, cp)
	if con == nil {
		return
	}
	//wait for VBA's to connect
	go waitForVBAs(con, cp, VBA_WAIT_TIME, c)
	for {
		conn, err := listener.Accept()
		if err != nil {
			Log.Debug("Error accept:", err.Error())
			return
		}
		go handleConn(conn, c, cp)
	}
}

func handleConn(conn net.Conn, co *Client, cp *conf.ParsedInfo) {
	ch := make(chan []byte)
	go handleWrite(conn, ch)
	handleRead(conn, ch, co, cp)
}

func handleWrite(conn net.Conn, ch chan []byte) {
	for m := range ch {
		l := new(bytes.Buffer)
		var ln int32 = int32(len(m))
		binary.Write(l, binary.BigEndian, ln)
		conn.Write(l.Bytes())
		_, err := conn.Write(m)
		if err != nil {
			Log.Debug("Error write", err.Error())
			return
		}
	}
}

func handleRead(conn net.Conn, c chan []byte, co *Client, cp *conf.ParsedInfo) {
	var state int = STATE_INIT_RES
	data := []byte{}
	fullData := []byte{}
	var length int32
	currTimeouts := 0
	var vc VbsClient
	var err error

	defer func() {
		conn.Close()
		Log.Debug("disconnecting client", getIpAddr(conn))
		if vc != nil {
			RemoveConn(conn, co, vc.ClientType())
		}
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
		case <-c1:
			Log.Debug("error on socket")
			return
		case data = <-c2:
			currTimeouts = 0
			Log.Debug("got data")
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
			Log.Debug("timeout on socket", conn)
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
							Log.Debug("Data size is more", length)
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

				m, err = parseMsg(input)
				if err != nil {
					return
				}
			}

			if state == STATE_INIT_RES {
				vc = getClient(m.Agent, conn, c)
			}

			switch ret := handleMsg(m, conn, &state, c, co, cp, c3, vc); ret {
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
		Log.Debug("error in unmarshalling")
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
	cp *conf.ParsedInfo, i chan string, vc VbsClient) int {
	if m != nil && m.Cmd == MSG_FAIL_STR {
		if vc != nil {
			vc.HandleFail(m, cp, co)
		}
	} else {
		switch *s {
		case STATE_INIT_RES:
			vc.HandleInit(i, cp, co, m.Capacity)
			*s = STATE_CONFIG_RES

		case STATE_CONFIG_RES:
			if vc.HandleOk(m) {
				*s = STATE_ALIVE
			}
			Log.Debug("setting alive state")

		case STATE_ALIVE:
			if vc.HandleAlive(m) == false {
				return STATUS_ERR
			}

		case STATE_UPDATE_CONFIG:
			if vc.HandleUpdateConfig(cp) == false {
				return STATUS_ERR
			}
			*s = STATE_CONFIG_RES
		}
	}
	return STATUS_SUCCESS
}

func SendInitMsg(ch chan []byte) {
	if m, err := getMsg(MSG_INIT); err == nil {
		ch <- m
	}
}
