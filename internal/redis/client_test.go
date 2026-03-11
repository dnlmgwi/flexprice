package redis

import (
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/flexprice/flexprice/internal/config"
	"github.com/flexprice/flexprice/internal/logger"
)

func TestRedisAddrs(t *testing.T) {
	t.Parallel()

	got := redisAddrs("redis-1, redis-2 ,redis-3", 6379)
	want := []string{"redis-1:6379", "redis-2:6379", "redis-3:6379"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("redisAddrs() = %v, want %v", got, want)
	}
}

func TestIsStandaloneRedisError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "cluster support disabled", err: errors.New("ERR This instance has cluster support disabled"), want: true},
		{name: "unknown cluster command", err: errors.New("ERR unknown command `cluster`, with args beginning with: `slots`"), want: true},
		{name: "connection refused", err: errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isStandaloneRedisError(tt.err); got != tt.want {
				t.Fatalf("isStandaloneRedisError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewClientWithStandaloneRedis(t *testing.T) {
	addr := os.Getenv("FLEXPRICE_TEST_STANDALONE_REDIS_ADDR")
	if addr == "" {
		t.Skip("set FLEXPRICE_TEST_STANDALONE_REDIS_ADDR to run standalone Redis integration test")
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Atoi(%q): %v", portStr, err)
	}

	client, err := NewClient(&config.Configuration{
		Redis: config.RedisConfig{
			Host:     host,
			Port:     port,
			DB:       0,
			PoolSize: 2,
			Timeout:  time.Second,
		},
	}, logger.GetLogger())
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() {
		_ = client.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
}
