package main

import (
	"code.google.com/p/goweb/goweb"
	"fmt"
 cl "vbucketserver/client"
	"vbucketserver/conf"
)

func HandleUpLoadConfig(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var con conf.Conf
		if err := c.Fill(&con); err != nil {
			fmt.Println("got error", err)
			return
		}
        fmt.Println("data is",con)
		cp.GenMap(&con)
	}
}

func HandleVbucketMap(c *goweb.Context, cp *conf.ParsedInfo) {
	cp.M.RLock()
	defer cp.M.RUnlock()
	fmt.Println("map ", cp.V)
	c.WriteResponse(cp.V, 200)
}

func HandleDeadvBuckets(c *goweb.Context, cp *conf.ParsedInfo, co *cl.Client) {
	if c.IsPost() || c.IsPut() {
		var dvi conf.DeadVbucketInfo
		if err := c.Fill(&dvi); err != nil {
			fmt.Println("got error", err)
			return
		}
        mp := cp.HandleDeadVbuckets(dvi, dvi.Server)
        //need to call it on client info
		cl.PushNewConfig(co, mp)
	}
}

func HandleServerDown(c *goweb.Context, cp *conf.ParsedInfo, co *cl.Client) {
	if c.IsPost() || c.IsPut() {
		var si conf.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			fmt.Println("got error", err)
			return
		}
        fmt.Println("si is",si)
        if si.Server == "" {
            fmt.Println("server is null")
            return
        }
        fmt.Println("downserver is", si.Server)
        mp := cp.HandleServerDown(si.Server)
        //need to call it on client info
		cl.PushNewConfig(co, mp)
	}
}

func HandleServerAlive(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var si conf.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			fmt.Println("got error", err)
			return
		}
		cp.HandleServerAlive(si.Server)
	}
}

func HandleCapacityUpdate(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var si conf.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			fmt.Println("got error", err)
			return
		}
		cp.HandleCapacityUpdate(si)
	}
}
