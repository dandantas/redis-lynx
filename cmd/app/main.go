package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dandantas/redis-lynx/internal/config"
	"github.com/dandantas/redis-lynx/internal/redis"
	"github.com/dandantas/redis-lynx/internal/websocket"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create Redis client
	redisClient, err := redis.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}
	defer redisClient.Close()

	// Create WebSocket server
	wsServer := websocket.NewServer(cfg, redisClient.Events())

	// Start WebSocket server in a goroutine
	go func() {
		if err := wsServer.Start(); err != nil {
			log.Fatalf("Failed to start WebSocket server: %v", err)
		}
	}()

	// Handle graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	<-shutdown
	log.Println("Shutting down...")

	// Close WebSocket server
	if err := wsServer.Close(); err != nil {
		log.Printf("Error shutting down WebSocket server: %v", err)
	}

	// Close Redis client
	if err := redisClient.Close(); err != nil {
		log.Printf("Error shutting down Redis client: %v", err)
	}

	log.Println("Shutdown complete")
}
