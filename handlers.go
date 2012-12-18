package main

import (
	"code.google.com/p/goweb/goweb"
	"log"
	"vbucketserver/client"
	"vbucketserver/conf"
)

func HandleUpLoadConfig(c *goweb.Context, cfgctx *conf.Context) {
	if c.IsPost() || c.IsPut() {
		var con conf.Config
		if err := c.Fill(&con); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("data is", con)
		cfgctx.GenMap(&con)
	}
}

func HandleVbucketMap(c *goweb.Context, cfgctx *conf.Context) {
	cfgctx.M.RLock()
	defer cfgctx.M.RUnlock()
	c.WriteResponse(cfgctx.V, 200)
}

func HandleDeadvBuckets(c *goweb.Context, cfgctx *conf.Context, co *client.Client) {
	if c.IsPost() || c.IsPut() {
		var dvi conf.DeadVbucketInfo
		if err := c.Fill(&dvi); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("server is", dvi.Server)
		ok, mp := cfgctx.HandleDeadVbuckets(dvi, dvi.Server, false)
		if ok {
			client.PushNewConfig(co, mp)
		}
	}
}

func HandleServerDown(c *goweb.Context, cfgctx *conf.Context, co *client.Client) {
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
		ok, mp := cfgctx.HandleServerDown(si.Server)
		if ok {
			client.PushNewConfig(co, mp)
		}
	}
}

func HandleServerAlive(c *goweb.Context, cfgctx *conf.Context) {
	if c.IsPost() || c.IsPut() {
		var si conf.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cfgctx.HandleServerAlive(si.Server)
	}
}

func HandleCapacityUpdate(c *goweb.Context, cfgctx *conf.Context) {
	if c.IsPost() || c.IsPut() {
		var si conf.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cfgctx.HandleCapacityUpdate(si)
	}
}

func SetupHandlers(cfgctx *conf.Context, co *client.Client) {
	goweb.MapFunc("/{version}/uploadConfig", func(c *goweb.Context) {
		HandleUpLoadConfig(c, cfgctx)
	})

	goweb.MapFunc("/{version}/vbucketMap", func(c *goweb.Context) {
		HandleVbucketMap(c, cfgctx)
	})

	goweb.MapFunc("/{version}/deadvBuckets", func(c *goweb.Context) {
		HandleDeadvBuckets(c, cfgctx, co)
	})

	goweb.MapFunc("/{version}/serverDown", func(c *goweb.Context) {
		HandleServerDown(c, cfgctx, co)
	})

	goweb.MapFunc("/{version}/serverAlive", func(c *goweb.Context) {
		HandleServerAlive(c, cfgctx)
	})

	goweb.MapFunc("/{version}/capacityUpdate", func(c *goweb.Context) {
		HandleCapacityUpdate(c, cfgctx)
	})
}
