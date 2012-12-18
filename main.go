package main

import (
	"code.google.com/p/goweb/goweb"
	"flag"
	"log"
	"net/http"
	"vbucketserver/server"
	"vbucketserver/config"
)
import _ "net/http/pprof"

func main() {
	var port = flag.String("port", ":14000", "Port number for VBS")
	var file = flag.String("confFile", "/tmp/file", "Config file for VBS")
	flag.Parse()

	goweb.ConfigureDefaultFormatters()

	var cp config.Context
	h := server.NewClient()
	SetupHandlers(&cp, h)
	go server.HandleTcp(h, &cp, *port, *file)

	go func() {
		log.Println(http.ListenAndServe(":8080", nil))
	}()

	http.ListenAndServe(":6060", goweb.DefaultHttpHandler)
}
