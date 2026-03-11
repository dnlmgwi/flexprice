package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/flexprice/flexprice/internal/config"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/redis/go-redis/v9"
)

// Client wraps Redis client functionality
type Client struct {
	rdb redis.UniversalClient
	log *logger.Logger
}

// NewClient creates a new Redis client
func NewClient(cfg *config.Configuration, log *logger.Logger) (*Client, error) {

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Redis.Timeout)
	defer cancel()

	addrs := redisAddrs(cfg.Redis.Host, cfg.Redis.Port)
	tlsConfig := redisTLSConfig(cfg.Redis.UseTLS)

	// Create cluster client
	clusterOpts := &redis.ClusterOptions{
		Addrs:        addrs,
		Password:     cfg.Redis.Password,
		ReadTimeout:  cfg.Redis.Timeout,
		WriteTimeout: cfg.Redis.Timeout,
		PoolSize:     cfg.Redis.PoolSize,
		TLSConfig:    tlsConfig,
	}

	clusterClient := redis.NewClusterClient(clusterOpts)

	result, err := clusterClient.Ping(ctx).Result()
	if err == nil {
		log.Infow("PING result", "result", result)
		log.Infow("Connected to Redis cluster successfully", "addrs", clusterOpts.Addrs)

		return &Client{
			rdb: clusterClient,
			log: log,
		}, nil
	}

	_ = clusterClient.Close()

	if len(addrs) != 1 || !isStandaloneRedisError(err) {
		return nil, fmt.Errorf("failed to create redis cluster client: %w", err)
	}

	log.Warnw("Redis cluster initialization failed, falling back to standalone client", "addr", addrs[0], "error", err)

	standaloneOpts := &redis.Options{
		Addr:         addrs[0],
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		ReadTimeout:  cfg.Redis.Timeout,
		WriteTimeout: cfg.Redis.Timeout,
		PoolSize:     cfg.Redis.PoolSize,
		TLSConfig:    tlsConfig,
	}

	standaloneClient := redis.NewClient(standaloneOpts)
	result, err = standaloneClient.Ping(ctx).Result()
	if err != nil {
		_ = standaloneClient.Close()
		return nil, fmt.Errorf("failed to create standalone redis client: %w", err)
	}

	log.Infow("PING result", "result", result)
	log.Infow("Connected to standalone Redis successfully", "addr", standaloneOpts.Addr, "db", standaloneOpts.DB)

	return &Client{
		rdb: standaloneClient,
		log: log,
	}, nil
}

func redisAddrs(host string, port int) []string {
	parts := strings.Split(host, ",")
	addrs := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		addrs = append(addrs, fmt.Sprintf("%s:%d", trimmed, port))
	}

	if len(addrs) == 0 {
		addrs = append(addrs, fmt.Sprintf("%s:%d", host, port))
	}

	return addrs
}

func redisTLSConfig(useTLS bool) *tls.Config {
	if !useTLS {
		return nil
	}

	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // Required for AWS ElastiCache wildcard certificates
	}
}

func isStandaloneRedisError(err error) bool {
	if err == nil {
		return false
	}

	lowerErr := strings.ToLower(err.Error())
	return strings.Contains(lowerErr, "cluster support disabled") ||
		strings.Contains(lowerErr, "unknown command `cluster`") ||
		(strings.Contains(lowerErr, "unknown subcommand") && strings.Contains(lowerErr, "cluster"))
}

// GetClient returns the underlying Redis client
func (c *Client) GetClient() redis.UniversalClient {
	return c.rdb
}

// Close closes the Redis client connection
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Ping checks the Redis connection
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.rdb.Ping(ctx).Result()
	return err
}
