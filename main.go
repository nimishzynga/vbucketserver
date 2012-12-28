package main

import (
	"code.google.com/p/goweb/goweb"
	"flag"
	"log"
	"net/http"
	"vbucketserver/config"
	"vbucketserver/server"
)
import _ "net/http/pprof"

func main() {
	var addr = flag.String("addr", "0:14000", "Socket Listen Address - ip:port")
	var cfg = flag.String("config", "/etc/sysconfig/vbucketserver", "Configuration file")
	flag.Parse()

	goweb.ConfigureDefaultFormatters()

    cls := config.NewCluster()
	h := server.NewClient()
	SetupHandlers(cls, h)
	go server.HandleTcp(h, cls, *addr, *cfg)

	go func() {
		log.Println(http.ListenAndServe(":8080", nil))
	}()

	http.ListenAndServe(":6060", goweb.DefaultHttpHandler)
}
