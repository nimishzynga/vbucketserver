package main

import (
	"code.google.com/p/goweb/goweb"
	"fmt"
	"vbucketserver/client"
	"vbucketserver/conf"
)

func HandleUpLoadConfig(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var con conf.Conf
		if err := c.Fill(&con); err != nil {
			fmt.Println("got error", err)
			return
		}
		cp.GenMap(&con)
	}
}

func HandleVbucketMap(c *goweb.Context, cp *conf.ParsedInfo) {
	cp.M.RLock()
	defer cp.M.RUnlock()
	fmt.Println("map ", cp.V)
	c.WriteResponse(cp.V, 200)
}

func HandleDeadvBuckets(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var dvi conf.DeadVbucketInfo
		if err := c.Fill(&dvi); err != nil {
			fmt.Println("got error", err)
			return
		}
		_, mp := cp.HandleDeadVbuckets(dvi, 1)
		clientHandler.PushNewConfig(mp)
	}
}
