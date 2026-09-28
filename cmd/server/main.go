// Command server starts the NoSQL MCP server.
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/FreePeak/cortex/pkg/server"

	"github.com/no-sql-mcp/server/internal/config"
	"github.com/no-sql-mcp/server/internal/delivery/mcp"
	"github.com/no-sql-mcp/server/internal/repository"
	"github.com/no-sql-mcp/server/internal/usecase"
	"github.com/no-sql-mcp/server/pkg/clients"
)

func main() {
	configPath := flag.String("config", "config.json", "path to the data source configuration file")
	transport := flag.String("transport", "stdio", "transport mode: stdio | sse")
	address := flag.String("address", ":9091", "listen address for the sse transport")
	lazyLoading := flag.Bool("lazy-loading", false, "connect to sources on first use instead of at startup")
	allowDangerous := flag.Bool("allow-dangerous", false, "permit dangerous cluster-level operations (disabled by default)")
	flag.Parse()

	// In stdio mode stdout is reserved for JSON-RPC; all logs go to stderr.
	logger := log.New(os.Stderr, "[nosql-mcp] ", log.LstdFlags)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatalf("load configuration: %v", err)
	}

	manager, err := clients.NewManager(cfg.Sources, registeredFactories(logger), *lazyLoading)
	if err != nil {
		logger.Fatalf("initialize client manager: %v", err)
	}
	defer func() { _ = manager.Close() }()

	if !*lazyLoading {
		if err := manager.ConnectAll(); err != nil {
			logger.Fatalf("connect data sources: %v", err)
		}
	}

	repo := repository.New(manager)
	guard := usecase.NewGuard(*allowDangerous)
	sourceUC := usecase.NewDataSourceUseCase(repo)
	redisUC := usecase.NewRedisUseCase(repo, guard)
	esUC := usecase.NewESUseCase(guard)

	mcpServer := server.NewMCPServer("NoSQL MCP Server", "0.1.0", logger)

	registry := mcp.NewToolRegistry(logger, sourceUC, esUC, redisUC)
	if err := registry.Register(context.Background(), mcpServer); err != nil {
		logger.Fatalf("register tools: %v", err)
	}

	switch *transport {
	case "stdio":
		if err := mcpServer.ServeStdio(); err != nil {
			logger.Fatalf("serve stdio: %v", err)
		}
	case "sse":
		mcpServer.SetAddress(*address)
		logger.Printf("serving SSE on %s", *address)
		if err := mcpServer.ServeHTTP(); err != nil {
			logger.Fatalf("serve http: %v", err)
		}
	default:
		logger.Fatalf("unsupported transport %q (allowed: stdio, sse)", *transport)
	}
}
