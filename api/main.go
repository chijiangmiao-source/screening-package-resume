package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	healthcheck := flag.Bool("healthcheck", false, "perform an HTTP healthcheck against ADDR and exit")
	flag.Parse()

	addr := envOr("ADDR", ":8080")

	if *healthcheck {
		port := addr
		if len(port) > 0 && port[0] == ':' {
			port = "127.0.0.1" + port
		}
		resp, err := http.Get("http://" + port + "/api/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			fmt.Fprintln(os.Stderr, "healthcheck failed:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	dataDir := envOr("DATA_DIR", "./data")
	db, err := openDB(dataDir)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	srv, err := NewServer(db, dataDir)
	if err != nil {
		log.Fatalf("init server: %v", err)
	}

	log.Printf("delivery-api listening on %s, data dir %s", addr, dataDir)
	log.Fatal(http.ListenAndServe(addr, srv.Handler()))
}
