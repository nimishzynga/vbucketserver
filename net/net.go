package net
//connection wrapper
import (
    "time"
	"bytes"
    "log"
	"encoding/json"
	"encoding/binary"
    "sync"
    "errors"
    "vbucketserver/config"
Net "net"
)

const (
    INIT = -1
)

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
    MSG_REP_FAIL_STR = "REPLICATION_FAIL"
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
    AGGREGATE_TIME  = 30
	FAILOVER_TIME    = 30
	STATE_ALIVE   = iota
    STATE_CLOSE
)

type clientI struct {
    ip string
    index int
    c string
    p func()
}

const (
    CLIENT1 = "127.0.0.1:11211"
    CLIENT2 = "127.0.0.2:11211"
    CLIENT3 = "127.0.0.3:11211"
    CLIENT4 = "127.0.0.4:11211"
    CLIENT5 = "127.0.0.5:11211"
    CLIENT6 = "127.0.0.11:11211"
)

var debug bool
var Ch chan string
var myMap map[string]*MyConn
var clientMap map[int]*clientI
var totc int
var M sync.RWMutex

type NewListener struct {
    T Net.Listener
}

func createClient(ip string) {
    Ch<-ip
    cl := &clientI { ip,totc, "", nil}
    clientMap[totc] = cl
    time.Sleep(2*time.Second)
}

func RpFail(ip string) *RecvMsg {
    m := &RecvMsg{Cmd:MSG_REP_FAIL_STR , Destination:ip}
    return m
}

func Dead(a []int, r []int) *RecvMsg {
    m := &RecvMsg{Cmd:"DEAD_VBUCKETS" , Vbuckets:config.Vblist{Active:a,},}
    return m
}

func getIp(i int) string {
    return clientMap[i].ip
}

func register(i int, c string, v func()) {
    clientMap[i].c = c
    clientMap[i].p = v
}

func getConn(i int) (*MyConn) {
    return myMap[clientMap[i].ip]
}

func settimer(t time.duration, f func()) {
    go func() {
        time.sleep(t *time.Second)
        f()
    }()
}

func ReplicationFail() {
        register(1, "CONFIG", func() {
            time.Sleep(4*time.Second)
            SendToClient(RpFail(getIp(4)), 1)
            c := getConn(1)
            c.handleMyClose()
            time.Sleep(10*time.Second)
            createClient(CLIENT2)
        })
        register(2, "CONFIG", func() {
            time.Sleep(5*time.Second)
            SendToClient(RpFail(getIp(4)), 2)
        })
        register(3, "CONFIG", func() {
            time.Sleep(6*time.Second)
            SendToClient(RpFail(getIp(4)), 3)
        })
}

func TestDiskFailure() {
    register(2, "CONFIG", func() {
        time.Sleep(5*time.Second)
        c := getConn(1)
        SendToClient(Dead(c.m.active[3:], nil), 1)
    })
}

func TestAliveFail() {
        register(1, "CONFIG", func() {
            time.Sleep(4*time.Second)
            c := getConn(1)
            c.handleMyClose()
            time.Sleep(20*time.Second)
            createClient(CLIENT2)
        })
}

func TestVbaDown() {
}

func SetDebug() {
    debug = true
}

func TestReshardDown() {


}

func HandleDebug() {
    SetDebug()
    log.Println("inside handleDebug")
    Ch = make(chan string)
    debug = true
    time.Sleep(3 *time.Second)
    myMap = make(map[string]*MyConn)
    clientMap= make(map[int]*clientI)
    createClient(CLIENT1)
    createClient(CLIENT2)
    createClient(CLIENT3)
    createClient(CLIENT4)
    createClient(CLIENT5)
    createClient(CLIENT6)
    time.Sleep(3 *time.Second)
    ReplicationFail()
   //TestDiskFailure()
   //TestAliveFail()
}

func SendToClient(data *RecvMsg, i int) {
    ip := clientMap[i].ip
    m,err := json.Marshal(data)
    if err != nil {
        log.Println("wtf")
    }
    l := new(bytes.Buffer)
    var ln int32 = int32(len(m))
    binary.Write(l, binary.BigEndian, ln)
    d := append(l.Bytes(), m...)
    myMap[ip].r<-d
}

func RecvClient(ip string) *RecvMsg {
         val := <-myMap[ip].w
         val = <-myMap[ip].w
         r := RecvMsg{}
         err := json.Unmarshal(val, &r)
         if err != nil {
             log.Println("TEST:unmarshal",err)
         }
     return &r
}

func ListenDebug(protocol string, server string) (NewListener, error) {
    SetDebug()
    newListener := NewListener{}
    return newListener,nil
}

func Listen(protocol string, server string) (Net.Listener, error) {
    v, err := Net.Listen(protocol, server)
    //newListener := NewListener{v}
    return v,err
}

func (c NewListener) Accept() (Net.Conn, error) {
    v1 := MyConn{}
    var err1 error
    if debug {
        val := <-Ch
        w := make(chan []byte, 10)
        r := make(chan []byte)
        v1 = NewConn(w,r,val, totc)
        totc++
        go v1.handleMyRead()
        go v1.doAlive()
        myMap[val]=&v1
    } else {
        v, err := c.T.Accept()
        return v, err
    }
    return v1,err1
}


func (c MyConn) doAlive() {
    for {
        time.Sleep(c.t *time.Second)
        if c.m.state == STATE_CLOSE {
            log.Println("returning due to close state")
            return
        }
        m:=&RecvMsg{Cmd:MSG_ALIVE_STR}
        c.handleMyWrite(m)
    }
}

func (c MyConn) handleMyWrite(data *RecvMsg) {
    log.Println("writing data", data)
    m,err := json.Marshal(data)
    if err != nil {
        log.Println("wtf")
    }
    l := new(bytes.Buffer)
    var ln int32 = int32(len(m))
    binary.Write(l, binary.BigEndian, ln)
    v := append(l.Bytes(), m...)
    c.r<-v
}

func (c MyConn) handleMyClose() {
    log.Println("setting close state")
    c.m.state = STATE_CLOSE
    close(c.r)
}

func (c MyConn) handleMyRead() {
	for {
         val := <-c.w
         val = <-c.w
         r := &ConfigVbaMsg{}
         err := json.Unmarshal(val, &r)
         if err != nil {
             log.Println("TEST:unmarshal",err)
         }
         (&c).handleRead(r)
    }
}

func (c *MyConn)handleRead(m *ConfigVbaMsg) {
    log.Println("got the message :TEST:",m)
    msg := &RecvMsg{}
    if m.Cmd == "INIT" {
        msg = &RecvMsg{Agent:"VBA",Capacity:3}
    } else if m.Cmd == MSG_CONFIG_STR {
        for _,g := range m.Data {
            c.m.active = append(c.m.active, g.VbId...)
        }
        c.t = time.Duration(m.HeartBeatTime)
        msg = &RecvMsg{Status: MSG_OK_STR, Cmd: MSG_CONFIG_STR}
        if m.RestoreCheckPoints != nil {
            for _,v := range m.RestoreCheckPoints {
                msg.Vbuckets.Replica = append(msg.Vbuckets.Replica, v)
                msg.CheckPoints.Replica = append(msg.CheckPoints.Replica, 0)
            }
        }
    }
    ff := clientMap[c.index]
    if ff.c != ""{
        log.Println("calling callback")
        go ff.p()
        ff.c = ""
    }
    c.handleMyWrite(msg)
    c.handleMyWrite(msg)
}

type MetaData struct {
    state int
    active []int
    replica []int
}

type Conn Net.Conn

type MyConn struct {
    w,r chan []byte
    ip string
    m *MetaData
    index int
    t time.Duration
    Net.Conn
}

func NewConn(w,r chan []byte, val string, i int) MyConn {
    v := MyConn{w:w,r:r,ip:val, m:&MetaData{state:STATE_ALIVE}, index:i, t:HBTIME}
    return v
}

func (c MyConn) Write(b []byte)(int,error) {
    c.w<-b
    return len(b),nil
}

func (c MyConn) Read(b []byte)(int, error) {
    data,ok := <-c.r
    var err error = nil
    if ok {
        copy(b, data)
    } else {
        err = errors.New("Connection closed by client")
    }
    return len(data),err
}

func (c MyConn) Close() (error) {
    log.Println("close called")
    return nil
}

type test struct {
    c MyConn
    Net.Addr
}

func (c MyConn) RemoteAddr() Net.Addr {
    return test{c:c}
}

func ( t test) String() string {
    return t.c.ip
}
