package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds all configuration settings for the application
type Config struct {
	// Redis configuration
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// WebSocket configuration
	WebSocketPort int

	// Application configuration
	Debug bool
}

// Load loads configuration from environment variables with fallback to .env file
func Load() (*Config, error) {
	// Try to load .env file, but continue if it doesn't exist
	_ = godotenv.Load()

	config := &Config{
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvAsInt("REDIS_DB", 0),
		WebSocketPort: getEnvAsInt("WEBSOCKET_PORT", 8080),
		Debug:         getEnvAsBool("DEBUG", false),
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.RedisAddr == "" {
		return fmt.Errorf("REDIS_ADDR is required")
	}

	if c.WebSocketPort <= 0 || c.WebSocketPort > 65535 {
		return fmt.Errorf("WEBSOCKET_PORT must be between 1 and 65535")
	}

	return nil
}

// Helper functions to get environment variables with default values
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}
