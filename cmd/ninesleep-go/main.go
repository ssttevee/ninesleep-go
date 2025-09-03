package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"eightsleep-esphome/internal/controller"
	"eightsleep-esphome/internal/gost"
	"eightsleep-esphome/internal/httpui"
)

const (
	socketPath      = "/deviceinfo/dac.sock"
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

	// CLI flags
	httpAddr := flag.String("http", httpListenAddr, "HTTP listen address (host:port)")
	gostPort := flag.Int("gost-port", 6053, "Gosthome API port")
	gostName := flag.String("gost-name", "pod3", "Gosthome node name")
	gostPoll := flag.Duration("gost-poll", 15*time.Second, "Polling interval for variables")
	gostMDNS := flag.Bool("gost-mdns", true, "Enable mDNS advertisement")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pod := controller.New()

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

	podCallback := func(pv *controller.PodVariables) {
		if pv == nil {
			return
		}
	}
	_ = podCallback

	go func() {
		if err := runUnixListener(ctx, pod); err != nil {
			log.Printf("unix listener exited: %v", err)
			cancel()
		}
	}()

	srv := httpui.NewServer(pod)
	httpSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           logRequestMiddleware(srv.Handler()),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("[http] listening on %s (version=%s commit=%s date=%s)", httpSrv.Addr, version, commit, date)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	gm.Stop()
	log.Println("exit.")
}

func runUnixListener(ctx context.Context, pod *controller.PodController) error {
	dir := filepath.Dir(socketPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	if st, err := os.Stat(socketPath); err == nil && (st.Mode()&os.ModeSocket) != 0 {
		_ = os.Remove(socketPath)
	}

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen unix: %w", err)
	}
	log.Printf("[unix] listening on %s", socketPath)

	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("[unix] accept error: %v", err)
			}
			return err
		}
		log.Printf("[unix] accepted connection")
		pod.SetConnection(c)
	}
}

func logRequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &respWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		log.Printf("[http] %s %s %d %s", r.Method, r.URL.Path, rw.status, time.Since(start))
	})
}

type respWriter struct {
	http.ResponseWriter
	status int
}

func (rw *respWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
