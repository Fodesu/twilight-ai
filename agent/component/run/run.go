// Package run is the shared main of the reference agent's binaries: parse
// the -config flag, load the component's document, compose it and serve it
// until a termination signal. Each cmd/* is this function applied to one
// component package.
package run

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/serve"
)

// Main runs one component: name is the binary's name for messages, compose
// builds the component from its document, listen extracts the address. The
// config path is required for service binaries.
func Main[C any](name string, compose func(context.Context, C) (serve.Component, error), listen func(C) string) {
	mainWithConfig(name, compose, listen, nil)
}

// MainWithDefaultConfig is like Main, but discovers a config when -config is
// omitted. It is intended for the human-facing local agent, not service
// binaries whose deployment always mounts an explicit document.
func MainWithDefaultConfig[C any](name string, compose func(context.Context, C) (serve.Component, error), listen func(C) string, discover func() (string, error)) {
	mainWithConfig(name, compose, listen, discover)
}

func mainWithConfig[C any](name string, compose func(context.Context, C) (serve.Component, error), listen func(C) string, discover func() (string, error)) {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("role", name,
		"pod", os.Getenv("TWILIGHT_POD_NAME"), "namespace", os.Getenv("TWILIGHT_POD_NAMESPACE"), "node", os.Getenv("TWILIGHT_NODE_NAME"))
	slog.SetDefault(logger)
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	path := fs.String("config", "", "path of the component's configuration document")
	_ = fs.Parse(os.Args[1:])
	configPath := *path
	if configPath == "" && discover != nil {
		var err error
		configPath, err = discover()
		if err != nil {
			logger.Error("Configuration discovery failed", "event", "configuration_failed", "error", err)
			os.Exit(2)
		}
	}
	if configPath == "" {
		logger.Error("-config is required", "event", "configuration_failed")
		os.Exit(2)
	}
	cfg, err := config.LoadFile[C](configPath)
	if err != nil {
		logger.Error("Configuration load failed", "event", "configuration_failed", "error", err)
		os.Exit(2)
	}
	addr := listen(cfg)
	if addr == "" {
		logger.Error("Listen address is required", "event", "configuration_failed")
		os.Exit(2)
	}
	ctx := context.Background()
	component, err := compose(ctx, cfg)
	if err != nil {
		logger.Error("Composition failed", "event", "composition_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("Starting HTTP server", "event", "server_starting", "listen", addr)
	if err := serve.Run(ctx, addr, component, serve.Options{}); err != nil {
		logger.Error("Server stopped with an error", "event", "server_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("Server stopped", "event", "server_stopped")
}
