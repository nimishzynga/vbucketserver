package main

import (
	"code.google.com/p/goweb/goweb"
	"log"
	"vbucketserver/config"
	"vbucketserver/server"
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
        args := []config.DeadVbucketInfo{dvi}
        str := []string{dvi.Server}
		ok, mp := cp.HandleDeadVbuckets(args, str, false, nil, true)
		if ok {
			server.PushNewConfig(co, mp, true)
		}
	}
}

func HandleServerDown(c *goweb.Context, cls *config.Cluster, co *server.Client) {
	if c.IsPost() || c.IsPut() {
		var si config.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("si is", si)
		if len(si.Server) == 0 {
			log.Println("server is null")
			return
		}
        //TODO:Need to fix here.Assuming all server belongs to same cluster
		cfgctx := cls.GetContext(si.Server[0])
		if cfgctx == nil {
			log.Println("Context not found for", si.Server)
			return
		}
		log.Println("downserver is", si.Server)
        //TODO:Need to fix here
		ok, mp := cfgctx.HandleServerDown(si.Server)
		if ok {
			server.PushNewConfig(co, mp, true)
		}
	}
}

func HandleReshardDown(c *goweb.Context, cls *config.Cluster, co *server.Client) {
	if c.IsPost() || c.IsPut() {
		var si config.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			log.Println("got error", err)
			return
		}
		log.Println("si is", si)
		if len(si.Server) == 0 {
			log.Println("server is null")
			return
		}
        if cls.SetReshard() == false {
            data := "Reshard is already going on.Please try later"
	        c.WriteResponse(data, 200)
            return
        }
        //TODO:Need to fix here.Assuming all server belongs to same cluster
		cfgctx := cls.GetContext(si.Server[0])
		if cfgctx == nil {
			log.Println("Context not found for", si.Server)
			return
		}
		log.Println("downserver is", si.Server)
        //TODO:Need to fix here
		ok, mp := cfgctx.HandleReshardDown(si.Server, si.Capacity)
		if ok {
			server.PushNewConfig(co, mp, true)
		}
	}
}

func HandleServerAlive(c *goweb.Context, cls *config.Cluster, co *server.Client) {
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
        for _, serv := range si.Server {
            if serv == "" {
                log.Println("Invalid server in server alive")
                return
            }
            for _,s := range cfgctx.C.Servers {
                if s == serv {
                    log.Println("Server already in server alive")
                    return
                }
            }
        }
        cls.AddIpToIpMap(si.Server, si.SecIp, c.PathParams["cluster"])
		ok, mp := cfgctx.HandleServerAlive(si.Server, si.SecIp, true)
		if ok {
			server.PushNewConfig(co, mp, true)
		}
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
		HandleServerAlive(c, cls, co)
	})

	goweb.MapFunc("/{cluster}/reshardDown", func(c *goweb.Context) {
		HandleReshardDown(c, cls, co)
	})

	goweb.MapFunc("/{cluster}/capacityUpdate", func(c *goweb.Context) {
		HandleCapacityUpdate(c, cls)
	})
}
