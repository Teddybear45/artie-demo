// queued is the frankenstein queue HTTP server.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/teddybear45/artie-demo/internal/api"
	"github.com/teddybear45/artie-demo/internal/queue"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data", "data", "directory for the persistence log")
	flag.Parse()

	reg, err := queue.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	log.Printf("queued listening on %s (log: %s/queue.log)", *addr, *dataDir)
	log.Fatal(http.ListenAndServe(*addr, api.New(reg)))
}
