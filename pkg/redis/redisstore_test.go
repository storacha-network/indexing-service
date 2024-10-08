package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/storacha/indexing-service/pkg/internal/testutil"
	"github.com/storacha/indexing-service/pkg/redis"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRedisStore(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name       string
		opts       []MockOption
		behavior   func(t *testing.T, store *redis.Store[string, string])
		finalState map[string]*redisValue
	}{
		{
			name: "normal behavior",
			behavior: func(t *testing.T, store *redis.Store[string, string]) {
				store.Set(ctx, "key1", "value1", true)
				store.Set(ctx, "key2", "value2", false)
				store.Set(ctx, "key3", "value3", true)
				store.Set(ctx, "key4", "value4", false)
				store.SetExpirable(ctx, "key3", false)
				store.SetExpirable(ctx, "key4", true)
				require.Equal(t, "value1", testutil.Must(store.Get(ctx, "key1"))(t))
				require.Equal(t, "value2", testutil.Must(store.Get(ctx, "key2"))(t))
				require.Equal(t, "value3", testutil.Must(store.Get(ctx, "key3"))(t))
				require.Equal(t, "value4", testutil.Must(store.Get(ctx, "key4"))(t))
				_, err := store.Get(ctx, "key5")
				require.ErrorIs(t, err, types.ErrKeyNotFound)
			},
			finalState: map[string]*redisValue{
				"key1": {"value1", redis.DefaultExpire},
				"key2": {"value2", 0},
				"key3": {"value3", 0},
				"key4": {"value4", redis.DefaultExpire},
			},
		},
		{
			name: "get errors",
			opts: []MockOption{WithErrorOnGet(errors.New("something went wrong"))},
			behavior: func(t *testing.T, store *redis.Store[string, string]) {
				_, err := store.Get(ctx, "key1")
				require.EqualError(t, err, "error accessing redis: something went wrong")
			},
		},
		{
			name: "set errors",
			opts: []MockOption{WithErrorOnSet(errors.New("something went wrong"))},
			behavior: func(t *testing.T, store *redis.Store[string, string]) {
				err := store.Set(ctx, "key1", "value1", true)
				require.EqualError(t, err, "error accessing redis: something went wrong")
			},
		},
		{
			name: "set expiration errors",
			opts: []MockOption{WithErrorOnSetExpiration(errors.New("something went wrong"))},
			behavior: func(t *testing.T, store *redis.Store[string, string]) {
				err := store.SetExpirable(ctx, "key1", true)
				require.EqualError(t, err, "error accessing redis: something went wrong")
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mockRedis := NewMockRedis(testCase.opts...)
			redisStore := redis.NewStore[string, string](
				func(s string) (string, error) { return s, nil },
				func(s string) (string, error) { return s, nil },
				func(s string) string { return s },
				mockRedis)
			testCase.behavior(t, redisStore)
			expectedFinalState := testCase.finalState
			if expectedFinalState == nil {
				expectedFinalState = make(map[string]*redisValue)
			}
			require.Equal(t, expectedFinalState, mockRedis.data)
		})
	}
}

type redisValue struct {
	data    string
	expires time.Duration
}

type MockRedis struct {
	data             map[string]*redisValue
	errGet           error
	errSet           error
	errSetExpiration error
}

var _ redis.Client = (*MockRedis)(nil)

type MockOption func(*MockRedis)

func WithErrorOnGet(err error) MockOption {
	return func(m *MockRedis) {
		m.errGet = err
	}
}

func WithErrorOnSet(err error) MockOption {
	return func(m *MockRedis) {
		m.errSet = err
	}
}

func WithErrorOnSetExpiration(err error) MockOption {
	return func(m *MockRedis) {
		m.errSetExpiration = err
	}
}

func NewMockRedis(opts ...MockOption) *MockRedis {
	m := &MockRedis{data: make(map[string]*redisValue)}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Expire implements redis.RedisClient.
func (m *MockRedis) Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd {
	cmd := goredis.NewBoolCmd(ctx, nil)
	if m.errSetExpiration != nil {
		cmd.SetErr(m.errSetExpiration)
		return cmd
	}
	val, ok := m.data[key]
	if ok && val.expires != expiration {
		val.expires = expiration
		cmd.SetVal(true)
	}
	return cmd
}

// Get implements redis.RedisClient.
func (m *MockRedis) Get(ctx context.Context, key string) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(ctx, nil)
	if m.errGet != nil {
		cmd.SetErr(m.errGet)
		return cmd
	}
	val, ok := m.data[key]
	if !ok {
		cmd.SetErr(goredis.Nil)
	} else {
		cmd.SetVal(val.data)
	}
	return cmd
}

// Persist implements redis.RedisClient.
func (m *MockRedis) Persist(ctx context.Context, key string) *goredis.BoolCmd {
	cmd := goredis.NewBoolCmd(ctx, nil)
	if m.errSetExpiration != nil {
		cmd.SetErr(m.errSetExpiration)
		return cmd
	}
	val, ok := m.data[key]
	if ok && val.expires != 0 {
		val.expires = 0
		cmd.SetVal(true)
	}
	return cmd
}

// Set implements redis.RedisClient.
func (m *MockRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(ctx, nil)
	if m.errSet != nil {
		cmd.SetErr(m.errSet)
		return cmd
	}
	m.data[key] = &redisValue{value.(string), expiration}
	return cmd
}
