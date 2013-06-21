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
	"flag"
	"net/http"
	"vbucketserver/goweb"
	"vbucketserver/config"
	"vbucketserver/net"
	"vbucketserver/server"
)

import _ "net/http/pprof"

func main() {
	var addr = flag.String("addr", "0:14000", "Socket Listen Address - ip:port")
	var cfg = flag.String("config", "/etc/sysconfig/vbucketserver", "Configuration file")
    var debug = flag.String("debug", "false", "VBS unit testing mode")
    var logLevel = flag.Int("log-level", 2, "Log level for VBS")
	flag.Parse()

	goweb.ConfigureDefaultFormatters()

	cls := config.NewCluster()
	h := server.NewClient()
    createLogger(*logLevel)
	SetupHandlers(cls, h)

    if *debug == "true" {
        go net.HandleDebug()
	    go server.HandleTcpDebug(h, cls, *addr, *cfg)
    } else {
	    go server.HandleTcp(h, cls, *addr, *cfg)
    }

    logger.Infof("=======STARTING VBS========")
	go func() {
        http.ListenAndServe(":8080", nil)
	}()

	http.ListenAndServe(":6060", goweb.DefaultHttpHandler)
}
