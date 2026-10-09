// Command patroni is a Go rewrite of the Patroni PostgreSQL HA daemon.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/patroni/patroni-go/internal/api"
	"github.com/patroni/patroni-go/internal/config"
	"github.com/patroni/patroni-go/internal/dcs"
	"github.com/patroni/patroni-go/internal/dcs/etcd3"
	"github.com/patroni/patroni-go/internal/ha"
	"github.com/patroni/patroni-go/internal/postgres"
	"github.com/patroni/patroni-go/internal/watchdog"
)

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: patroni [options] <config.yml>

Options:
  --config-file PATH   path to the configuration file
  --validate-config    validate the configuration file and exit
  -h, --help           show this help
`)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	var configPath string
	validateOnly := false
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-h" || arg == "--help":
			usage()
			os.Exit(0)
		case arg == "--config-file":
			i++
			if i < len(args) {
				configPath = args[i]
			}
		case strings.HasPrefix(arg, "--config-file="):
			configPath = strings.TrimPrefix(arg, "--config-file=")
		case arg == "--validate-config":
			validateOnly = true
		case !strings.HasPrefix(arg, "-") && configPath == "":
			configPath = arg
		}
	}
	if configPath == "" {
		usage()
		os.Exit(2)
	}
	cfg, err := config.NewFromFile(configPath)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	if cfg.Name() == "" {
		log.Fatalf("FATAL: `name` is required in the configuration")
	}
	dcsName := cfg.DetectDCS()
	if dcsName == "" {
		log.Fatalf("FATAL: no DCS configuration found; supported: etcd3")
	}
	if dcsName != "etcd3" {
		log.Fatalf("FATAL: DCS backend %q is not implemented in the Go rewrite; use etcd3", dcsName)
	}
	if validateOnly {
		log.Printf("configuration file %s is valid (dcs=%s scope=%s name=%s)",
			configPath, dcsName, cfg.Scope(), cfg.Name())
		return
	}
	run(cfg, dcsName)
}

func run(cfg *config.Config, dcsName string) {
	d, err := etcd3.New(cfg.DCSSection(dcsName))
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	cfg.LoadCache()
	d.ReloadConfig(cfg.DCSSection(dcsName))

	pg, err := postgres.New(cfg.Name(), cfg.PostgresqlSection())
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	wd := watchdog.New(map[string]any{})
	h := ha.New(cfg, d, pg, wd)

	restSrv, err := api.New(cfg.RestAPISection(), h)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	if err := restSrv.Listen(); err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- restSrv.Serve() }()
	restSrv.SetShutdownFunc(func() {
		log.Printf("shutdown requested via REST API")
		gracefulShutdown(h, restSrv, d)
		os.Exit(0)
	})

	ensureDCSAccess(d)
	ensureUniqueName(h, cfg)

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for sig := range sigs {
			switch sig {
			case syscall.SIGHUP:
				log.Printf("received SIGHUP, reloading local configuration")
				if err := cfg.ReloadLocal(); err != nil {
					log.Printf("failed to reload configuration: %v", err)
					continue
				}
				d.ReloadConfig(cfg.DCSSection(dcsName))
			default:
				log.Printf("received %s, shutting down", sig)
				gracefulShutdown(h, restSrv, d)
				os.Exit(0)
			}
		}
	}()

	log.Printf("patroni-go started: name=%s scope=%s dcs=%s", cfg.Name(), cfg.Scope(), dcsName)
	loopWait := time.Duration(cfg.LoopWait()) * time.Second
	for {
		started := time.Now()
		msg := h.RunCycle()
		log.Printf("HA cycle: %s", msg)
		if strings.HasPrefix(msg, "FATAL:") {
			log.Fatalf("%s", msg)
		}
		if strings.HasPrefix(msg, "bootstrap") || strings.Contains(msg, "not accessible") {
			time.Sleep(5 * time.Second)
			continue
		}
		elapsed := time.Since(started)
		nap := loopWait - elapsed
		if nap > 0 {
			h.Watch(nap)
		}
	}
}

func ensureDCSAccess(d dcs.DCS) {
	for {
		if _, err := d.GetCluster(); err == nil {
			return
		} else {
			log.Printf("can not get cluster from dcs: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func ensureUniqueName(h *ha.Ha, cfg *config.Config) {
	cluster := h.Cluster()
	if cluster == nil {
		return
	}
	member := cluster.GetMember(cfg.Name(), false)
	if member == nil || member.Name != cfg.Name() {
		return
	}
	if member.APIURL() == "" {
		return
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(strings.TrimSuffix(member.APIURL(), "/") + "/liveness")
	if err == nil {
		resp.Body.Close()
		log.Fatalf("FATAL: can't start; there is already a node named '%s' running", cfg.Name())
	}
}

func gracefulShutdown(h *ha.Ha, srv *api.Server, d dcs.DCS) {
	_ = srv.Shutdown()
	h.Shutdown()
	if c, ok := d.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
