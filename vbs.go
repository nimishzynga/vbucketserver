package main

import (
	"code.google.com/p/goweb/goweb"
	"flag"
	"log"
	"net/http"
	cl "vbucketserver/client"
	"vbucketserver/conf"
)
import _ "net/http/pprof"

func Init(cp *conf.ParsedInfo, co *cl.Client) {
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

func main() {
	var cp conf.ParsedInfo
	h := cl.NewClient()
	var port = flag.String("port", ":14000", "Port number for VBS")
	var file = flag.String("confFile", "/tmp/file", "Config file for VBS")
	flag.Parse()

	goweb.ConfigureDefaultFormatters()
	Init(&cp, h)
	go cl.HandleTcp(h, &cp, *port, *file)
	go func() {
		log.Println(http.ListenAndServe(":8080", nil))
	}()
	http.ListenAndServe(":6060", goweb.DefaultHttpHandler)
}
