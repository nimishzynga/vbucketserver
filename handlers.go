package main

import (
	"code.google.com/p/goweb/goweb"
    "log"
	"vbucketserver/client"
	"vbucketserver/conf"
)

func HandleUpLoadConfig(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var con conf.Conf
		if err := c.Fill(&con); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("data is", con)
		cp.GenMap(&con)
	}
}

func HandleVbucketMap(c *goweb.Context, cp *conf.ParsedInfo) {
	cp.M.RLock()
	defer cp.M.RUnlock()
	c.WriteResponse(cp.V, 200)
}

func HandleDeadvBuckets(c *goweb.Context, cp *conf.ParsedInfo, co *client.Client) {
	if c.IsPost() || c.IsPut() {
		var dvi conf.DeadVbucketInfo
		if err := c.Fill(&dvi); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("server is", dvi.Server)
		ok, mp := cp.HandleDeadVbuckets(dvi, dvi.Server, false)
		if ok {
			//need to call it on client info
			client.PushNewConfig(co, mp)
		}
	}
}

func HandleServerDown(c *goweb.Context, cp *conf.ParsedInfo, co *client.Client) {
	defer func() {
		recover()
	}()
	if c.IsPost() || c.IsPut() {
		var si conf.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("si is", si)
		if si.Server == "" {
			log.Println("server is null")
			return
		}
		log.Println("downserver is", si.Server)
		ok, mp := cp.HandleServerDown(si.Server)
		if ok {
			//need to call it on client info
			client.PushNewConfig(co, mp)
		}
	}
}

func HandleServerAlive(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var si conf.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cp.HandleServerAlive(si.Server)
	}
}

func HandleCapacityUpdate(c *goweb.Context, cp *conf.ParsedInfo) {
	if c.IsPost() || c.IsPut() {
		var si conf.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cp.HandleCapacityUpdate(si)
	}
}

func SetupHandlers(cp *conf.ParsedInfo, co *client.Client) {
	goweb.MapFunc("/{version}/uploadConfig", func(c *goweb.Context) {
		HandleUpLoadConfig(c, cp)
	})

	goweb.MapFunc("/{version}/vbucketMap", func(c *goweb.Context) {
		HandleVbucketMap(c, cp)
	})

	goweb.MapFunc("/{version}/deadvBuckets", func(c *goweb.Context) {
		HandleDeadvBuckets(c, cp, co)
	})

	goweb.MapFunc("/{version}/serverDown", func(c *goweb.Context) {
		HandleServerDown(c, cp, co)
	})

	goweb.MapFunc("/{version}/serverAlive", func(c *goweb.Context) {
		HandleServerAlive(c, cp)
	})

	goweb.MapFunc("/{version}/capacityUpdate", func(c *goweb.Context) {
		HandleCapacityUpdate(c, cp)
	})
}

