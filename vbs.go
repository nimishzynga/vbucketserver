package main

import (
	"code.google.com/p/goweb/goweb"
	"net/http"
    "vbucketserver/conf"
 cl "vbucketserver/client"
    "flag"
)

func Init(cp conf.ParsedInfo) {
	goweb.MapFunc("/{version}/uploadConfig", func(c *goweb.Context) {
        HandleUpLoadConfig(c, &cp)
    })

	goweb.MapFunc("/{version}/vbucketMap", func(c *goweb.Context) {
        HandleVbucketMap(c, &cp)
    })

	goweb.MapFunc("/{version}/deadvBuckets", func(c *goweb.Context) {
        HandleDeadvBuckets(c,&cp)
    })

/*
	goweb.MapFunc("/{version}/serverDown", func(c *goweb.Context) {
        handleServerDown(c, cp)
    })

	goweb.MapFunc("/{version}/serverAlive", func(c *goweb.Context) {
        handleServerAlive(c, cp)
    })

	goweb.MapFunc("/{version}/capacityUpdate", func(c *goweb.Context) {
        handleCapacityUpdate(c, cp)
    })

    goweb.MapFunc("/{version}/vmaConfig", func(c *goweb.Context) {
        handleVmaConfig(c, cp)
    })
    */
}

func main() {
    var cp conf.ParsedInfo
    var h cl.Client
    var port = flag.String("port", ":14000", "Port number for VBS")
	goweb.ConfigureDefaultFormatters()
    Init(cp)
    go cl.HandleTcp(&h, &cp, *port)
    http.ListenAndServe(":8080",goweb.DefaultHttpHandler)
}
