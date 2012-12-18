package main

import (
	"code.google.com/p/goweb/goweb"
	"log"
	"vbucketserver/server"
	"vbucketserver/config"
)

func HandleUpLoadConfig(c *goweb.Context, cfgctx *config.Context) {
	if c.IsPost() || c.IsPut() {
		var con config.Config
		if err := c.Fill(&con); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("data is", con)
		cfgctx.GenMap(&con)
	}
}

func HandleVbucketMap(c *goweb.Context, cfgctx *config.Context) {
	cfgctx.M.RLock()
	defer cfgctx.M.RUnlock()
	c.WriteResponse(cfgctx.V, 200)
}

func HandleDeadvBuckets(c *goweb.Context, cfgctx *config.Context, co *server.Client) {
	if c.IsPost() || c.IsPut() {
		var dvi config.DeadVbucketInfo
		if err := c.Fill(&dvi); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("server is", dvi.Server)
		ok, mp := cfgctx.HandleDeadVbuckets(dvi, dvi.Server, false)
		if ok {
			server.PushNewConfig(co, mp)
		}
	}
}

func HandleServerDown(c *goweb.Context, cfgctx *config.Context, co *server.Client) {
	defer func() {
		recover()
	}()
	if c.IsPost() || c.IsPut() {
		var si config.ServerUpDownInfo
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
			server.PushNewConfig(co, mp)
		}
	}
}

func HandleServerAlive(c *goweb.Context, cfgctx *config.Context) {
	if c.IsPost() || c.IsPut() {
		var si config.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cfgctx.HandleServerAlive(si.Server)
	}
}

func HandleCapacityUpdate(c *goweb.Context, cfgctx *config.Context) {
	if c.IsPost() || c.IsPut() {
		var si config.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cfgctx.HandleCapacityUpdate(si)
	}
}

func SetupHandlers(cfgctx *config.Context, co *server.Client) {
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
