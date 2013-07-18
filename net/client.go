package net
import "time"

func (m *moxiClient) handleInit(c *MyConn) {
    msg := &RecvMsg{Agent:"MOXI"}
    c.handleMyWrite(msg)
}

func (m *vbaClient) handleInit(c *MyConn) {
    msg := &RecvMsg{Agent:"VBA",Capacity:3}
    c.handleMyWrite(msg)
}

func (mc *moxiClient) handleConfig(m *RecvMsg, c *MyConn) {
    logger.Debugf("got moxi config", m)
    msg := Fail("127.0.0.5:11211")
    c.handleMyWrite(msg)
    return
}

func (vc *vbaClient) handleConfig(m *RecvMsg, c *MyConn) {
    for _,g := range m.Data {
        c.m.active = append(c.m.active, g.VbId...)
    }
    addVbucket.l.Lock()
    addVbucket.vbList = append(addVbucket.vbList, c.m.active...)
    addVbucket.l.Unlock()

    c.t = time.Duration(m.HeartBeatTime)
    msg := &RecvMsg{Status: MSG_OK_STR, Cmd: MSG_CONFIG_STR}
    if m.RestoreCheckPoints != nil {
        for _,v := range m.RestoreCheckPoints {
            msg.Vbuckets.Replica = append(msg.Vbuckets.Replica, v)
            msg.CheckPoints.Replica = append(msg.CheckPoints.Replica, 0)
        }
    }
    ff := clientVbaMap[c.index]
    if ff.c != ""{
        logger.Debugf("calling callback")
        go ff.p()
        ff.c = ""
    }
    c.handleMyWrite(msg)
}
