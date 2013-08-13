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
