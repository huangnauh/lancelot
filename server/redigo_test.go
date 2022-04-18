package server_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	redisgo "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedisgoString(t *testing.T) {
	t.Parallel()
	for i, tt := range testRedisString {
		tt := tt
		i := i
		t.Run("redisgo"+tt.Key, func(t *testing.T) {
			t.Parallel()
			c, err := redisgo.Dial("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
				redisgo.DialReadTimeout(time.Second),
				redisgo.DialWriteTimeout(time.Second),
			)
			assert.NoError(t, err, "redis.Dial error")

			defer func() {
				if err := c.Close(); err != nil {
					log.Fatalf("redisgo - failed to communicate to redis-server: %v", err)
				}
			}()

			reply, err := redisgo.String(c.Do("Auth", cfg.Auth.Pass))
			assert.NoError(t, err, fmt.Sprintf("redis auth %s error", cfg.Auth.Pass))
			assert.Equal(t, "OK", reply)
			key := "redisgo" + tt.Key

			args := []interface{}{key, tt.Value + "xx", "xx"}
			if tt.Expire == 0 {
			} else if tt.Expire >= time.Second {
				args = append(args, "ex", int(tt.Expire/time.Second))
			} else {
				args = append(args, "px", int(tt.Expire/time.Millisecond))
			}
			_, err = redisgo.String(c.Do("set", args...))
			assert.Equal(t, redisgo.ErrNil, err)

			args = []interface{}{key, tt.Value}
			if tt.Expire == 0 {
			} else if tt.Expire >= time.Second {
				args = append(args, "ex", int(tt.Expire/time.Second))
			} else {
				args = append(args, "px", int(tt.Expire/time.Millisecond))
			}
			if i%2 == 0 {
				args = append(args, "nx")
			}

			reply, err = redisgo.String(c.Do("set", args...))
			assert.NoError(t, err, "redis set error")
			assert.Equal(t, "OK", reply)

			args = []interface{}{key, tt.Value + "nx", "nx"}
			if tt.Expire == 0 {
			} else if tt.Expire >= time.Second {
				args = append(args, "ex", int(tt.Expire/time.Second))
			} else {
				args = append(args, "px", int(tt.Expire/time.Millisecond))
			}
			_, err = redisgo.String(c.Do("set", args...))
			assert.Equal(t, redisgo.ErrNil, err)

			data, err := redisgo.Bytes(c.Do("get", key))
			assert.NoError(t, err, "redis get error")
			assert.Equal(t, tt.Value, string(data))

			args = []interface{}{key, tt.Value + "xx", "xx"}
			if tt.Expire == 0 {
			} else if tt.Expire >= time.Second {
				args = append(args, "ex", int(tt.Expire/time.Second))
			} else {
				args = append(args, "px", int(tt.Expire/time.Millisecond))
			}
			reply, err = redisgo.String(c.Do("set", args...))
			assert.NoError(t, err)
			assert.Equal(t, "OK", reply)

			data, err = redisgo.Bytes(c.Do("get", key))
			assert.NoError(t, err, "redis get error")
			assert.Equal(t, tt.Value+"xx", string(data))

			if tt.KeepTTL {
				time.Sleep(tt.Expire / 2)
				reply, err = redisgo.String(c.Do("set", key, tt.Value+"keepttl", "keepttl"))
				assert.NoError(t, err, "redis set error")
				assert.Equal(t, "OK", reply)
				data, err := redisgo.Bytes(c.Do("get", key))
				assert.NoError(t, err, "redis get error")
				assert.Equal(t, tt.Value+"keepttl", string(data))
				time.Sleep(tt.Expire / 2)
			} else {
				time.Sleep(tt.Expire)
			}
			time.Sleep(time.Millisecond)

			if tt.Expire == 0 {
				count, err := redisgo.Int(c.Do("del", key))
				assert.NoError(t, err, "redis del error")
				assert.Equal(t, 1, count)
			}

			_, err = redisgo.Bytes(c.Do("get", key))
			assert.Equal(t, redisgo.ErrNil, err)

			count, err := redisgo.Int(c.Do("del", key))
			assert.NoError(t, err, "redis del error")
			assert.Equal(t, 0, count)
		})
	}
}

var _ = Describe("Scan", func() {
	var client redisgo.Conn
	var err error

	BeforeEach(func() {
		client, err = redisgo.Dial("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort),
			redisgo.DialReadTimeout(time.Second),
			redisgo.DialWriteTimeout(time.Second),
		)
		Expect(err).NotTo(HaveOccurred())
		_, err := client.Do("flushdb")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	FDescribe("scanning", func() {
		It("should Scan start end", func() {
			for i := 0; i <= 1000; i++ {
				_, err = client.Do("set", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
				Expect(err).NotTo(HaveOccurred())
			}

			resp, err := redisgo.Values(client.Do("scan", "0", "count", "2", "type", "string", "start", "key100", "end", "key200"))
			Expect(err).NotTo(HaveOccurred())
			var keys []string
			var cursor int64
			_, err = redisgo.Scan(resp, &cursor, &keys)
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"key100", "key1000"}))
		})
		It("should Scan withvalue", func() {
			for i := 0; i <= 1000; i++ {
				_, err = client.Do("set", fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
				Expect(err).NotTo(HaveOccurred())
			}

			resp, err := redisgo.Values(client.Do("scan", "0", "count", "2", "type", "string", "withvalue", "start", "key100", "end", "key200"))
			Expect(err).NotTo(HaveOccurred())
			var keys []string
			var cursor int64
			_, err = redisgo.Scan(resp, &cursor, &keys)
			Expect(err).NotTo(HaveOccurred())
			Expect(keys).To(Equal([]string{"key100", "value100", "key1000", "value1000"}))
		})
	})
})
