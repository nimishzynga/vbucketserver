package clientHandler

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
	VBA_WAIT_TIME  = 10
	CLIENT_VBA     = "VBA"
	CLIENT_MOXI    = "MOXI"
	CLIENT_CLI     = "Cli"
	CLIENT_UNKNOWN = "Unknown"
	CLIENT_PCNT    = 1
	CONF_FILE_PATH = "/tmp/file"
)

//return status
const (
	STATUS_INVALID = iota
	STATUS_CONT
	STATUS_ERR
	STATUS_SUCCESS
)

func HandleTcp(c *Client, cp *conf.ParsedInfo, s string) {
	listener, err := net.Listen("tcp", s)
	if err != nil {
		println("error listening:", err.Error())
		os.Exit(1)
	}
	//parse the conf file
	con := parseInitialConfig(CONF_FILE_PATH, cp)
	if con == nil {
		return
	}
	//wait for VBA's to connect
	go waitForVBAs(con, cp, VBA_WAIT_TIME, c)
	for {
		conn, err := listener.Accept()
		if err != nil {
			println("Error accept:", err.Error())
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
			println("Error write", err.Error())
			return
		}
	}
}

func handleRead(conn net.Conn, c chan []byte, co *Client, cp *conf.ParsedInfo) {
    var state int = STATE_INIT
    data := []byte{}
    fullData := []byte{}
    var length int32
    currTimeouts := 0
    defer func() {
        conn.Close()
        fmt.Println("disconnecting client", getIpAddr(conn))
        RemoveConn(conn, co)
    }()
    m := &RecvMsg{}
    hasData := false
    c1 := make(chan error)
    c2 := make(chan []byte)
    c3 := make(chan byte)

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

    if s := getClientType(conn, co); s == CLIENT_UNKNOWN {
        handleMsg(nil, conn, &state, c, co, cp, c3)
    }

    for {
        select {
        case <-c1:
            fmt.Println("error on socket")
            return
        case data = <-c2:
            currTimeouts = 0
            fmt.Println("got data")
            fullData = append(fullData, data...)
        case <-c3:
            if state >= STATE_CONFIG_RES {
                state = STATE_UPDATE_CONFIG
                hasData = true
            }
        case <-time.After(HBTIME * time.Second):
            currTimeouts++
            if state != STATE_ALIVE || currTimeouts > MAX_TIMEOUT {
                return
            }
            fmt.Println("timeout on socket", conn)
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
                            fmt.Println("Data size is more", length)
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

                m, _ = parseMsg(input)
            }
            switch ret := handleMsg(m, conn, &state, c, co, cp, c3); ret {
                case STATUS_ERR:
                    return
            }
            hasData = false
        }
    }
}

func parseMsg(b []byte) (*RecvMsg, error) {
	m := &RecvMsg{}
	var err error
	fmt.Println("in pasrse msg input length is", len(b))
	if err = json.Unmarshal(b, m); err != nil {
		println("error in unmarshalling")
	}
	fmt.Println("inside parseMsg", m)
	return m, err
}

func handleMsg(m *RecvMsg, c net.Conn, s *int, ch chan []byte, co *Client,
	cp *conf.ParsedInfo, i chan byte) int {
	fmt.Println("state is", *s)
    //hack to work with MCS
    if m != nil && m.Cmd == MSG_FAIL_STR {
        mp := cp.HandleServerDown(m.Server)
        //need to call it on client info
        PushNewConfig(co, mp)
        return STATUS_SUCCESS
    }
    switch *s {
	case STATE_INIT:
		if m, err := getMsg(MSG_INIT); err == nil {
			ch <- m
		}
		*s = STATE_INIT_RES
	case STATE_INIT_RES:
		if m.Agent == CLIENT_MOXI || m.Agent == CLIENT_VBA {
			Insert(c, i, co, m.Agent)
			co.Con.L.Lock()
			for co.Started == false {
				co.Con.Wait()
			}
			co.Con.L.Unlock()
            if m.Agent == CLIENT_VBA {
                index := getServerIndex(cp, getIpAddr(c))
                si := cp.S[index]
                si.NumberOfDisc = int16(m.Capacity)
                cp.S[index] = si
            }
			//see if i can create an interface and handle it separately
			if m, err := getMsg(MSG_CONFIG, cp, HBTIME, m.Agent, getIpAddr(c)); err == nil {
				ch <- m
			}
		} else {
			fmt.Println("CLI connected", m)
		}
		*s = STATE_CONFIG_RES
	case STATE_CONFIG_RES:
		//this timeout is for alive messages
		//need to handle timeout here
		if m.Cmd == MSG_ALIVE_STR {
			*s = STATE_CONFIG_RES_NXT
			return STATUS_SUCCESS
		}
		if m.Status != MSG_OK_STR {
			return STATUS_ERR
		}
		*s = STATE_ALIVE
		fmt.Println("setting alive state")
	case STATE_ALIVE:
		//ignore the ok response here
        if m.Cmd != MSG_ALIVE_STR && m.Cmd != MSG_OK_STR {
			return STATUS_ERR
		} else {
			fmt.Println("got alive message")
		}
	case STATE_UPDATE_CONFIG:
        if m, err := getMsg(MSG_CONFIG, cp, HBTIME, getClientType(c, co),
	        getIpAddr(c));err == nil {
			ch <- m
		}
		*s = STATE_CONFIG_RES
	case STATE_CONFIG_RES_NXT:
		if m.Status != MSG_OK_STR {
			return STATUS_ERR
		}
		*s = STATE_ALIVE
	}
	return STATUS_SUCCESS
}
