package main

import (
	"code.google.com/p/goweb/goweb"
	"flag"
	"log"
	"net/http"
	"vbucketserver/client"
	"vbucketserver/conf"
)
import _ "net/http/pprof"

func main() {
	var port = flag.String("port", ":14000", "Port number for VBS")
	var file = flag.String("confFile", "/tmp/file", "Config file for VBS")
	flag.Parse()

	goweb.ConfigureDefaultFormatters()

	var cp conf.Context
	h := client.NewClient()
	SetupHandlers(&cp, h)
	go client.HandleTcp(h, &cp, *port, *file)

	go func() {
		log.Println(http.ListenAndServe(":8080", nil))
	}()

	http.ListenAndServe(":6060", goweb.DefaultHttpHandler)
}
