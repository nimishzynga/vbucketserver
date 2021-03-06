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
package main

import (
	"os"
	"vbucketserver/config"
	"vbucketserver/goweb"
	"vbucketserver/log"
	"vbucketserver/net"
	"vbucketserver/server"
)

var logger *log.SysLog

func HandleUpLoadConfig(c *goweb.Context, cls *config.Cluster) {
	if c.IsPost() || c.IsPut() {
		clsNew := config.NewCluster()
		if err := c.Fill(&clsNew); err != nil {
			logger.Warnf("got error", err)
			return
		}
		logger.Debugf("data is", clsNew)
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
			logger.Warnf("got error", err)
			return
		}
		logger.Debugf("server is", dvi.Server)
		cp := cls.GetContext(dvi.Server)
		if cp == nil {
			logger.Warnf("Context not found for", dvi.Server)
			return
		}
		args := []config.DeadVbucketInfo{dvi}
		str := []string{dvi.Server}
		ok, mp := cp.HandleDeadVbuckets(args, str, false, nil, true)
		if ok {
			server.PushNewConfig(co, mp, true, cp)
		}
	}
}

func HandleServerDown(c *goweb.Context, cls *config.Cluster, co *server.Client) {
	if c.IsPost() || c.IsPut() {
		logger.Infof("ServerDown api called")
		var si config.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			logger.Warnf("got error", err)
			return
		}
		logger.Debugf("si is", si)
		if len(si.Server) == 0 {
			logger.Debugf("server is null")
			return
		}
		//TODO:Need to fix here.Assuming all server belongs to same cluster
		cfgctx := cls.GetContext(si.Server[0])
		if cfgctx == nil {
			logger.Debugf("Context not found for", si.Server)
			return
		}
		logger.Infof("Downserver is", si.Server)
		//TODO:Need to fix here
		ok, mp := cfgctx.HandleServerDown(si.Server)
		if ok {
			server.PushNewConfig(co, mp, true, cfgctx)
		}
	}
}

func HandleReshardDown(c *goweb.Context, cls *config.Cluster, co *server.Client) {
	if c.IsPost() || c.IsPut() {
		logger.Infof("ReshardDown api called")
		var si config.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			logger.Warnf("got error", err)
			return
		}
		logger.Debugf("si is", si)
		if len(si.Server) == 0 {
			logger.Debugf("server is null")
			return
		}
		//TODO:Need to fix here.Assuming all server belongs to same cluster
		cfgctx := cls.GetContext(si.Server[0])
		if cfgctx == nil {
			logger.Debugf("Context not found for", si.Server)
			return
		}
		if cfgctx.SetReshard() == false {
			data := "Reshard is already going on.Please try later"
			c.WriteResponse(data, 200)
			return
		}
		logger.Infof("reshard server is", si.Server)
		//TODO:Need to fix here
		ok, mp := cfgctx.HandleReshardDown(si.Server, si.Capacity)
		if ok {
			server.PushNewConfig(co, mp, true, cfgctx)
		}
		c.WriteResponse("SUCCESS", 200)
	}
}

func HandleServerAlive(c *goweb.Context, cls *config.Cluster, co *server.Client) {
	if c.IsPost() || c.IsPut() {
		logger.Infof("ServerALive api called")
		var si config.ServerUpDownInfo
		if err := c.Fill(&si); err != nil {
			logger.Warnf("got error", err)
			return
		}
		logger.Debugf("Server:", si.Server, "SecIp:", si.SecIp)
		cfgctx := cls.GetContextFromClusterName(c.PathParams["cluster"])
		if cfgctx == nil {
			logger.Debugf("Context not found for", si.Server)
			return
		}
		logger.Debugf("got cluster name as", c.PathParams["cluster"])
		if len(si.Server) == 0 {
			logger.Warnf("server list is empty in server alive")
			return
		}
		removeDuplicateIps(&si.Server)
		removeDuplicateIps(&si.SecIp)
		for _, serv := range si.Server {
			if serv == "" {
				logger.Warnf("Invalid server in server alive")
				return
			}
			for _, s := range cfgctx.C.Servers {
				if s == serv {
					logger.Warnf("Server already in server alive")
					return
				}
			}
		}
		logger.Infof("added server is", si.Server)
		if cfgctx.SetReshard() == false {
			data := "Reshard is going on.Please try later"
			c.WriteResponse(data, 200)
			return
		}

		cls.AddIpToIpMap(si.Server, si.SecIp, c.PathParams["cluster"])
		ok, mp := cfgctx.HandleServerAlive(si.Server, si.SecIp, true)
		if ok {
			server.PushNewConfig(co, mp, true, cfgctx)
		}
		c.WriteResponse("SUCCESS", 200)
	}
}

func HandleCapacityUpdate(c *goweb.Context, cls *config.Cluster) {
	if c.IsPost() || c.IsPut() {
		var si config.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			logger.Warnf("got error", err)
			return
		}
		cfgctx := cls.GetContext(si.Server)
		if cfgctx == nil {
			logger.Warnf("Context not found for", si.Server)
			return
		}
		cfgctx.HandleCapacityUpdate(si)
	}
}

func HandleReshardStatus(c *goweb.Context, cls *config.Cluster) {
	cfgctx := cls.GetContextFromClusterName(c.PathParams["cluster"])
	if cfgctx == nil {
		logger.Warnf("HandleReshardStatus :Context not found")
		return
	}
	status := cfgctx.GetReshardStatus()
	c.WriteResponse(status, 200)
}

/*
func HandleCapacityInfo(c *goweb.Context, cls *config.Cluster) {
	if c.IsPost() || c.IsPut() {
    var si config.CapacityUpdateInfo
		if err := c.Fill(&si); err != nil {
			logger.Debugf("got error", err)
			return
		}
		cfgctx := cls.GetContext(si.Server)
		if cfgctx == nil {
			logger.Debugf("Context not found for", si.Server)
			return
		}
		cfgctx.HandleCapacityInfo(si)
    }
}*/

func createLogger(level int) {
	if level < 0 {
		return
	}
	logger = log.NewSysLog(os.Stdout, "[VBS]", level)
	config.SetLogger(logger)
	server.SetLogger(logger)
	net.SetLogger(logger)
}

func removeDuplicateIps(ip *[]string) {
	found := make(map[string]bool)
	j := 0
	for i, serv := range *ip {
		if !found[serv] {
			found[serv] = true
			(*ip)[j] = (*ip)[i]
			j++
		}
	}
	(*ip) = (*ip)[:j]
}

func HandleVbsState(cls *config.Cluster, state string) {
	if state == "" {
		return
	}
	cls.PromoteVbs(state)
}

func HandleParams(c *goweb.Context, cls *config.Cluster) {
	if c.IsPost() || c.IsPut() {
		si := config.Params{LogLevel: -1}
		if err := c.Fill(&si); err != nil {
			logger.Warnf("got error", err)
			return
		}
		logger.Infof("Changing log level to ", si.LogLevel)
		createLogger(si.LogLevel)
		HandleVbsState(cls, si.State)
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

	goweb.MapFunc("/{cluster}/reshardStatus", func(c *goweb.Context) {
		HandleReshardStatus(c, cls)
	})

	goweb.MapFunc("/{cluster}/setParams", func(c *goweb.Context) {
		HandleParams(c, cls)
	})

	/*
	   goweb.MapFunc("/{cluster}/capacityInfo", func(c *goweb.Context) {
	       HandleCapacityInfo(c, cls)
	   }))*/
}
