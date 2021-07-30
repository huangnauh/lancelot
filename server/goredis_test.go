package server_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	t.Parallel()
	for i, tt := range testRedisString {
		tt := tt
		i := i
		t.Run("redis"+tt.Key, func(t *testing.T) {
			t.Parallel()
			c := redis.NewClient(&redis.Options{
				Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
				Password: cfg.Auth.Pass})
			defer func() {
				if err := c.Close(); err != nil {
					log.Fatalf("redis - failed to communicate to redis-server: %v", err)
				}
			}()

			ctx := context.Background()
			key := "redis" + tt.Key

			ok, err := c.SetXX(ctx, key, tt.Value+"xx", tt.Expire).Result()
			assert.NoError(t, err)
			assert.Equal(t, false, ok)

			if i%2 == 0 {
				set, err := c.Set(ctx, key, tt.Value, tt.Expire).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", set)
			} else {
				ok, err := c.SetNX(ctx, key, tt.Value, tt.Expire).Result()
				assert.NoError(t, err)
				assert.Equal(t, true, ok)
			}

			ok, err = c.SetNX(ctx, key, tt.Value+"nx", tt.Expire).Result()
			assert.NoError(t, err)
			assert.Equal(t, false, ok)

			get, err := c.Get(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, tt.Value, get)

			ok, err = c.SetXX(ctx, key, tt.Value+"xx", tt.Expire).Result()
			assert.NoError(t, err)
			assert.Equal(t, true, ok)
			get, err = c.Get(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, tt.Value+"xx", get)

			if tt.KeepTTL {
				time.Sleep(tt.Expire / 2)
				set, err := c.Set(ctx, key, tt.Value+"keepttl", -1).Result()
				assert.NoError(t, err)
				assert.Equal(t, "OK", set)
				get, err := c.Get(ctx, key).Result()
				assert.NoError(t, err)
				assert.Equal(t, tt.Value+"keepttl", get)
				time.Sleep(tt.Expire / 2)
			} else {
				time.Sleep(tt.Expire)
			}
			time.Sleep(time.Millisecond)

			if tt.Expire <= 0 {
				del, err := c.Del(ctx, key).Result()
				assert.NoError(t, err)
				assert.Equal(t, int64(1), del)
			}

			expire, err := c.TTL(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, time.Duration(-2), expire)

			value, err := c.Get(ctx, key).Result()
			assert.Equal(t, redis.Nil, err, "value %s", value)

			del, err := c.Del(ctx, key).Result()
			assert.NoError(t, err)
			assert.Equal(t, int64(0), del)
		})
	}
}
