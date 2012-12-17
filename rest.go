package main

import (
	"code.google.com/p/goweb/goweb"
	cl "vbucketserver/client"
	"vbucketserver/conf"
)

func HandleUpLoadConfig(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var con conf.Conf
		if err := c.Fill(&con); err != nil {
			Log.Debug("got error", err)
			return
		}
		Log.Debug("data is", con)
		cp.GenMap(&con)
	}
}

func HandleVbucketMap(c *goweb.Context, cp *conf.ParsedInfo) {
	cp.M.RLock()
	defer cp.M.RUnlock()
	c.WriteResponse(cp.V, 200)
}

func HandleDeadvBuckets(c *goweb.Context, cp *conf.ParsedInfo, co *cl.Client) {
	if c.IsPost() || c.IsPut() {
		var dvi conf.DeadVbucketInfo
		if err := c.Fill(&dvi); err != nil {
			Log.Debug("got error", err)
			return
		}
		Log.Debug("server is", dvi.Server)
		ok, mp := cp.HandleDeadVbuckets(dvi, dvi.Server, false)
		if ok {
			//need to call it on client info
			cl.PushNewConfig(co, mp)
		}
	}
}

func HandleServerDown(c *goweb.Context, cp *conf.ParsedInfo, co *cl.Client) {
	defer func() {
		recover()
	}()
	if c.IsPost() || c.IsPut() {
		var si conf.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			Log.Debug("got error", err)
			return
		}
		Log.Debug("si is", si)
		if si.Server == "" {
			Log.Debug("server is null")
			return
		}
		Log.Debug("downserver is", si.Server)
		ok, mp := cp.HandleServerDown(si.Server)
		if ok {
			//need to call it on client info
			cl.PushNewConfig(co, mp)
		}
	}
}

func HandleServerAlive(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var si conf.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			Log.Debug("got error", err)
			return
		}
		cp.HandleServerAlive(si.Server)
	}
}

func HandleCapacityUpdate(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var si conf.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			Log.Debug("got error", err)
			return
		}
		cp.HandleCapacityUpdate(si)
	}
}
