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
