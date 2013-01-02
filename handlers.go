package main

import (
	"code.google.com/p/goweb/goweb"
	"log"
	"vbucketserver/config"
	"vbucketserver/server"
    "strings"
)

func HandleUpLoadConfig(c *goweb.Context, cls *config.Cluster) {
	if c.IsPost() || c.IsPut() {
		clsNew := config.NewCluster()
		if err := c.Fill(&clsNew); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("data is", clsNew)
		for key, cfg := range clsNew.ConfigMap {
			cp := &config.Context{}
			cp.GenMap(key, &cfg)
			cls.ContextMap[key] = cp
		}
		*cls = *clsNew
	}
}

func HandleVbucketMap(c *goweb.Context, cls *config.Cluster) {
	cls.M.RLock()
	defer cls.M.RUnlock()
	data := server.ClusterVbucketMap{}
	for _, cp := range cls.ContextMap {
		cp.M.RLock()
        data.Buckets = append(data.Buckets, cp.V)
		cp.M.RUnlock()
	}
	c.WriteResponse(data, 200)
}

func HandleDeadvBuckets(c *goweb.Context, cls *config.Cluster, co *server.Client) {
	if c.IsPost() || c.IsPut() {
		var dvi config.DeadVbucketInfo
		if err := c.Fill(&dvi); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("server is", dvi.Server)
		cp := cls.GetContext(dvi.Server)
		if cp == nil {
			log.Println("Context not found for", dvi.Server)
			return
		}
		ok, mp := cp.HandleDeadVbuckets(dvi, dvi.Server, false)
		if ok {
			server.PushNewConfig(co, mp)
		}
	}
}

func HandleServerDown(c *goweb.Context, cls *config.Cluster, co *server.Client) {
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
		cfgctx := cls.GetContext(si.Server)
		if cfgctx == nil {
			log.Println("Context not found for", si.Server)
			return
		}
		log.Println("downserver is", si.Server)
		ok, mp := cfgctx.HandleServerDown(si.Server)
		if ok {
			server.PushNewConfig(co, mp)
		}
	}
}

func HandleServerAlive(c *goweb.Context, cls *config.Cluster) {
    log.Println("Adding new server")
	if c.IsPost() || c.IsPut() {
		var si config.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cfgctx := cls.GetContextFromClusterName(c.PathParams["cluster"])
		if cfgctx == nil {
			log.Println("Context not found for", si.Server)
			return
		}
        log.Println("got cluster name as", c.PathParams["cluster"])
        server := strings.Split(si.Server, ":")[0]
        cls.IpMap[server] = c.PathParams["cluster"]
		cfgctx.HandleServerAlive(si.Server)
	}
}

func HandleCapacityUpdate(c *goweb.Context, cls *config.Cluster) {
	if c.IsPost() || c.IsPut() {
		var si config.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		cfgctx := cls.GetContext(si.Server)
		if cfgctx == nil {
			log.Println("Context not found for", si.Server)
			return
		}
		cfgctx.HandleCapacityUpdate(si)
	}
}

func SetupHandlers(cls *config.Cluster, co *server.Client) {
	goweb.MapFunc("/{cluster}/uploadConfig", func(c *goweb.Context) {
		HandleUpLoadConfig(c, cls)
	})

	goweb.MapFunc("/{cluster}/vbucketMap", func(c *goweb.Context) {
		HandleVbucketMap(c, cls)
	})

	goweb.MapFunc("/{cluster}/deadvBuckets", func(c *goweb.Context) {
		HandleDeadvBuckets(c, cls, co)
	})

	goweb.MapFunc("/{cluster}/serverDown", func(c *goweb.Context) {
		HandleServerDown(c, cls, co)
	})

	goweb.MapFunc("/{cluster}/serverAlive", func(c *goweb.Context) {
		HandleServerAlive(c, cls)
	})

	goweb.MapFunc("/{cluster}/capacityUpdate", func(c *goweb.Context) {
		HandleCapacityUpdate(c, cls)
	})
}
