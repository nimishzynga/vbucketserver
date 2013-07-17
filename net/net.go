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
package net
//connection wrapper
import (
    "time"
	"bytes"
    "vbucketserver/log"
	"encoding/json"
	"encoding/binary"
    "sync"
    "errors"
    "vbucketserver/config"
    "sort"
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

var debug bool
var moxiCh,vbaCh chan string
var myMap map[string]*MyConn
var clientMoxiMap map[int]*moxiClient
var clientVbaMap map[int]*vbaClient
var totc int
var M sync.RWMutex
var logger *log.SysLog
var addVbucket addVbuc

func SetLogger(l *log.SysLog) {
    logger = l
}

type NewListener struct {
    T Net.Listener
}

func createMoxiClient(ip string) {
    moxiCh<-ip
    cl := &moxiClient{ip, totc, "", nil}
    clientMoxiMap[totc] = cl
    time.Sleep(2*time.Second)
}

func createVbaClient(ip string) {
    vbaCh<-ip
    cl := &vbaClient{ ip,totc, "", nil}
    clientVbaMap[totc] = cl
    time.Sleep(2*time.Second)
}

func RpFail(ip string) *RecvMsg {
    m := &RecvMsg{Cmd:MSG_REP_FAIL_STR , Destination:ip}
    return m
}

func Fail(ip string) *RecvMsg {
    m := &RecvMsg{Cmd:MSG_FAIL_STR , Destination:ip}
    return m
}

func Dead(a []int, r []int) *RecvMsg {
    m := &RecvMsg{Cmd:"DEAD_VBUCKETS" , Vbuckets:config.Vblist{Active:a,},}
    return m
}

func getMoxiIp(i int) string {
    return clientMoxiMap[i].ip
}

/*returns the vba up*/
func getIp(i int) string {
    return clientVbaMap[i].ip
}

func register(i int, c string, v func()) {
    clientVbaMap[i].c = c
    clientVbaMap[i].p = v
}

func getConn(i int) (*MyConn) {
    return myMap[clientVbaMap[i].ip]
}

func ReplicationFail() {
        register(4, "CONFIG", func() {
            time.Sleep(4*time.Second)
            SendToClient(Fail("127.0.0.1:11211"), 4)
        })
        register(2, "CONFIG", func() {
            time.Sleep(5*time.Second)
            SendToClient(Fail(getIp(4)), 2)
        })
        register(1, "CONFIG", func() {
            time.Sleep(5*time.Second)
            SendToClient(Fail(getIp(4)), 1)
        })
        register(3, "CONFIG", func() {
            time.Sleep(6*time.Second)
            SendToClient(Fail(getIp(4)), 3)
            time.Sleep(2*time.Second)
            SendToClient(RpFail(getIp(4)), 3)
        })
}

func AllDc() {
        register(1, "CONFIG", func() {
            time.Sleep(4*time.Second)
            //logger.Debugf(addVbucket.vbList)
            c := getConn(1)
            c.handleMyClose()
        })
        register(2, "CONFIG", func() {
            time.Sleep(4*time.Second)
            c := getConn(2)
            c.handleMyClose()
        })
        register(3, "CONFIG", func() {
            time.Sleep(4*time.Second)
            c := getConn(3)
            c.handleMyClose()
        })
        register(4, "CONFIG", func() {
            time.Sleep(4*time.Second)
            c := getConn(4)
            c.handleMyClose()
        })
        register(0, "CONFIG", func() {
            time.Sleep(4*time.Second)
            c := getConn(0)
            c.handleMyClose()
        })
}

func TestDiskFailure() {
    register(2, "CONFIG", func() {
        time.Sleep(5*time.Second)
        c := getConn(1)
        SendToClient(Dead(c.m.active[3:], nil), 1)
    })
}

func TestConfig() {
        register(1, "CONFIG", func() {
            time.Sleep(4*time.Second)
            sort.Ints(addVbucket.vbList)
            for i:=0; i<2048;i++ {
                if addVbucket.vbList[i] != i {
                    logger.Errorf("not received", i)
                }
            }
            logger.Errorf(addVbucket.vbList)
        })
}

func TestAliveFail() {
        register(1, "CONFIG", func() {
            time.Sleep(4*time.Second)
            c := getConn(1)
            c.handleMyClose()
            time.Sleep(20*time.Second)
            createVbaClient(CLIENT2)
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
    logger.Debugf("%s", "inside handleDebug")
    moxiCh = make(chan string)
    vbaCh = make(chan string)
    time.Sleep(3 *time.Second)
    myMap = make(map[string]*MyConn)
    clientVbaMap= make(map[int]*vbaClient)
    clientMoxiMap= make(map[int]*moxiClient)
    createVbaClient(CLIENT1)
    createVbaClient(CLIENT2)
    createVbaClient(CLIENT3)
    createVbaClient(CLIENT4)
    createVbaClient(CLIENT5)
    createMoxiClient(CLIENT1)
    //createVbaClient(CLIENT6)
    time.Sleep(3 *time.Second)
    ReplicationFail()
   //TestDiskFailure()
   //TestAliveFail()
    //AllDc()
   // TestConfig()
}

func SendToClient(data *RecvMsg, i int) {
    ip := clientVbaMap[i].ip
    m,err := json.Marshal(data)
    if err != nil {
        logger.Debugf("wtf")
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
             logger.Debugf("TEST:unmarshal",err)
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
    var val string
    if debug {
        w := make(chan []byte, 10)
        r := make(chan []byte)
        select {
        case val = <-moxiCh:
            v1 = NewConn(w,r,val, totc)
            v1.moxi = true
        case val = <-vbaCh:
            v1 = NewConn(w,r,val, totc)
        }
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
            logger.Debugf("returning due to close state")
            return
        }
        m:=&RecvMsg{Cmd:MSG_ALIVE_STR}
        c.handleMyWrite(m)
    }
}

func (c MyConn) handleMyWrite(data *RecvMsg) {
    logger.Debugf("writing data", data)
    m,err := json.Marshal(data)
    if err != nil {
        logger.Debugf("wtf")
    }
    l := new(bytes.Buffer)
    var ln int32 = int32(len(m))
    binary.Write(l, binary.BigEndian, ln)
    v := append(l.Bytes(), m...)
    c.r<-v
}

func (c MyConn) handleMyClose() {
    logger.Debugf("setting close state")
    c.m.state = STATE_CLOSE
    close(c.r)
}

func (c MyConn) handleMyRead() {
	for {
         val := <-c.w
         val = <-c.w
         r := &RecvMsg{}
         err := json.Unmarshal(val, &r)
         if err != nil {
             logger.Debugf("TEST:unmarshal",err)
         }
         (&c).handleRead(r)
    }
}

func (c *MyConn)handleRead(m *RecvMsg) {
    logger.Debugf("got te message :TEST:",m)
    if c.moxi {
        cInfo := clientMoxiMap[c.index]
        if m.Cmd == "INIT" {
            cInfo.handleInit(c)
            return
        } else if m.Cmd == MSG_CONFIG_STR {
            cInfo.handleConfig(m, c)
            return
        }
    } else {
        cInfo := clientVbaMap[c.index]
        if m.Cmd == "INIT" {
            cInfo.handleInit(c)
            return
        } else if m.Cmd == MSG_CONFIG_STR {
            cInfo.handleConfig(m, c)
            return
        }
    }
}

func NewConn(w,r chan []byte, val string, i int) MyConn {
    v := MyConn{w:w,r:r,ip:val, m:&MetaData{state:STATE_ALIVE}, index:i, moxi:false, t:HBTIME}
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
    logger.Debugf("close called")
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
