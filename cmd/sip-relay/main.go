package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"sip-relay/internal/config"
	relaysip "sip-relay/internal/sip"
)

func main() {
	configPath := flag.String("config", "", "path to YAML config")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	log.Info("config loaded", "config", *configPath, "sip_addr", cfg.SIP.ListenIP, "sip_port", cfg.SIP.ListenPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("starting SIP relay")
	server := relaysip.NewServer(cfg, log)
	if err := server.Start(ctx); err != nil {
		log.Error("failed to start SIP relay", "error", err)
		os.Exit(1)
	}

	<-ctx.Done()
	server.Close()
	log.Info("SIP relay stopped")
}
