package clientHandler

import (
	"vbucketserver/conf"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
    "fmt"
    "time"
    "bytes"
)

//client states
const (
    STATE_INVALID = iota
    STATE_INIT
    STATE_INIT_RES
    STATE_CONFIG
    STATE_CONFIG_RES
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
)

//other constants
const (
	RECV_BUF_LEN   = 1024
	HEADER_SIZE    = 4
	HBTIME         = 30
	MAX_TIMEOUT    = 3
    VBA_WAIT_TIME  = 60
	CLIENT_VBA     = "Vba"
	CLIENT_MOXI    = "Moxi"
	CLIENT_CLI     = "Cli"
	CLIENT_UNKNOWN = "Unknown"
    CLIENT_PCNT    = 95
    CONF_FILE_PATH = "/tmp/file"
)

func HandleTcp(c *Client, cp *conf.ParsedInfo, s string) {
	listener, err := net.Listen("tcp", s)
	if err != nil {
		println("error listening:", err.Error())
		os.Exit(1)
	}
    //parse the conf file
    con := parseInitialConfig(CONF_FILE_PATH, cp)
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
		_, err := conn.Write(m)
		if err != nil {
			println("Error write", err.Error())
			return
		}
	}
}

func handleRead(conn net.Conn, c chan []byte, co *Client, cp *conf.ParsedInfo) {
	var state int = STATE_INIT
	data := make([]byte, RECV_BUF_LEN)
	fullData := []byte{}
	var length int
	currTimeouts := 0
	defer conn.Close()
	m := &RecvMsg{}
	c1 := make(chan error)
	c2 := make(chan []byte)

	go func() {
		d := make([]byte, RECV_BUF_LEN)
		for {
			_, err := conn.Read(d)
			if err != nil {
				c1 <- err
			} else {
				c2 <- d
			}
		}
	}()

	if s := getClientType(conn, co); s == CLIENT_UNKNOWN {
		handleMsg(nil, conn, state, c, co, cp)
	}

	for {
		select {
		case <-c1:
			fmt.Println("error on socket")
			return
		case data = <-c2:
			currTimeouts = 0
			fmt.Println("got data")
		case <-time.After(HBTIME * time.Second):
			currTimeouts++
			if currTimeouts <= MAX_TIMEOUT {
				fmt.Println("timeout on socket", conn)
				handleMsg(m, conn, state, c, co, cp)
				length = 0
				fullData = fullData[:0]
				continue
			} else {
				return
			}
		}

        n := len(data)
		if length == 0 && n > HEADER_SIZE {
			buf := bytes.NewBuffer(data)
			binary.Read(buf, binary.LittleEndian, &length)
			if length > RECV_BUF_LEN {
				fmt.Println("Data size is more")
				return
			}
            data = data[HEADER_SIZE:]
		}

		fullData = append(fullData, data...)
		if len(fullData) < length {
			continue
		}

        input := fullData[:length]
        fullData = fullData[length:]
        length = len(fullData)

        if m,err := parseMsg(input); err != nil {
            _ = m
            return
        }
		if handleMsg(m, conn, state, c, co, cp) == false {
			return
		}
		changeState(&state)
	}
}

//set the state to next
func changeState(s *int) {
	switch *s {
	case STATE_INIT:
		*s = STATE_INIT_RES
	case STATE_INIT_RES:
		*s = STATE_CONFIG_RES
	case STATE_CONFIG_RES:
		*s = STATE_ALIVE
	}
}

func parseMsg(b []byte) (*RecvMsg,error) {
	m := &RecvMsg{}
    var err error
	if err = json.Unmarshal(b, m); err != nil {
		println("error in unmarshalling")
	}
	return m, err
}

func handleMsg(m *RecvMsg, c net.Conn, s int, ch chan []byte, co *Client,
	cp *conf.ParsedInfo) bool {
	switch s {
	case STATE_INIT:
        if m, err := getMsg(MSG_INIT); err == nil {
            ch<-m
        }
	case STATE_INIT_RES:
        if m.Agent == CLIENT_MOXI || m.Agent == CLIENT_VBA {
            Insert(c, ch, co, m.Agent)
            if co.Started == false {
                co.Con.Wait()
            }
            if m, err := getMsg(MSG_CONFIG, cp, HBTIME); err == nil {
                ch<-m
            }
        }
	case STATE_CONFIG_RES:
		//this timeout is for alive messages
        //need to handle timeout here
		if m.Cmd != "Ok" {
			return false
		}
	}
	return true
}
