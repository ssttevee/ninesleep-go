package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"eightsleep-esphome/internal/controller"
	"eightsleep-esphome/internal/gost"
	"eightsleep-esphome/internal/httpui"
)

const (
	httpListenAddr  = "0.0.0.0:8080"
	shutdownTimeout = 5 * time.Second
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	httpAddr := flag.String("http", httpListenAddr, "HTTP listen address (host:port)")
	gostPort := flag.Int("gost-port", 6053, "Gosthome API port")
	gostName := flag.String("gost-name", "pod3", "Gosthome node name")
	gostPoll := flag.Duration("gost-poll", 15*time.Second, "Polling interval for variables")
	gostMDNS := flag.Bool("gost-mdns", true, "Enable mDNS advertisement")
	webUI := flag.Bool("webui", false, "Enable web UI / HTTP server")
	mitm := flag.Bool("mitm", false, "Enable MITM mode (ephemeral firmware <-> dac proxy)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var opts []controller.Option
	if *mitm {
		opts = append(opts, controller.WithMITM(true))
	}
	pod := controller.New(opts...)

	gm := gost.NewManager(
		pod,
		gost.WithAPIPort(uint16(*gostPort)),
		gost.WithName(*gostName),
		gost.WithPollingInterval(*gostPoll),
		gost.WithMDNS(*gostMDNS),
	)
	gm.Start(ctx)
	if err := gm.InitError(); err != nil {
		log.Printf("[gosthome] init error: %v", err)
	} else {
		log.Printf("[gosthome] started")
	}

	go func() {
		if err := pod.RunUnixSocketLoop(ctx); err != nil {
			log.Printf("unix socket loop exited: %v", err)
			cancel()
		}
	}()

	var httpSrv *http.Server
	if *webUI {
		srv := httpui.NewServer(pod)
		httpSrv = &http.Server{
			Addr:              *httpAddr,
			Handler:           srv.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Printf("[http] listening on %s (version=%s commit=%s date=%s)", httpSrv.Addr, version, commit, date)
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("http server error: %v", err)
				cancel()
			}
		}()
	} else {
		log.Printf("[http] web UI disabled (enable with -webui)")
	}

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(shutdownCtx)
	}
	gm.Stop()
	log.Println("exit.")
}
