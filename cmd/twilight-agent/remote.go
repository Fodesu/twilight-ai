package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/felinics/twilight/agent/executor"
	executorhttp "github.com/felinics/twilight/agent/executor/http"
	executionstore "github.com/felinics/twilight/agent/executor/store"
	"github.com/felinics/twilight/agent/host"
	"github.com/felinics/twilight/agent/run"
	"github.com/felinics/twilight/agent/run/loop"
	"github.com/felinics/twilight/agent/session/filestore"
	runmod "github.com/felinics/twilight/agent/session/run"
)

func newExecutor(ctx context.Context, root string, models map[run.ModelRef]loop.ModelInvoker, tools []loop.ExecutableTool) (*executor.Worker, error) {
	content, err := filestore.NewContentStore(root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return nil, err
	}
	catalog, err := host.NewCatalog(models, tools...)
	if err != nil {
		return nil, err
	}
	backend, err := host.NewLocalExecutor(catalog, content, nil, false)
	if err != nil {
		return nil, err
	}
	records, err := executionstore.NewFileStore(filepath.Join(root, "executions"))
	if err != nil {
		return nil, err
	}
	return executor.NewWorker(ctx, records, backend)
}

func listenLoopback(address string) (net.Listener, error) {
	hostname, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if hostname != "localhost" && !net.ParseIP(hostname).IsLoopback() {
		return nil, errors.New("HTTP services require a loopback listen address; use SSH port forwarding for remote access")
	}
	return net.Listen("tcp", address)
}

func serveExecutor(root, address string, models map[run.ModelRef]loop.ModelInvoker, tools []loop.ExecutableTool) error {
	listener, err := listenLoopback(address)
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	worker, err := newExecutor(ctx, root, models, tools)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.Handle("/", (&executorhttp.Server{Worker: worker}).Handler())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: time.Minute}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Printf("executor http://%s (pid %d, records %s)\n", listener.Addr(), os.Getpid(), filepath.Join(root, "executions"))
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
		<-done
		return nil
	}
}
