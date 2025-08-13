package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	
	"github.com/go-redis/redis/v8"
	"github.com/zzayne/go-crawler/config"
)

// Cache Redis缓存管理器
type Cache struct {
	client *redis.Client
	ctx    context.Context
	prefix string
}

// NewCache 创建Redis缓存
func NewCache(cfg config.RedisConfig, prefix string) (*Cache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	
	ctx := context.Background()
	
	// 测试连接
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	
	log.Printf("Successfully connected to Redis at %s", cfg.Address)
	
	return &Cache{
		client: client,
		ctx:    ctx,
		prefix: prefix,
	}, nil
}

// SetVisited 标记URL已访问
func (c *Cache) SetVisited(url string) error {
	key := c.getKey("visited", url)
	return c.client.Set(c.ctx, key, time.Now().Unix(), 7*24*time.Hour).Err()
}

// IsVisited 检查URL是否已访问
func (c *Cache) IsVisited(url string) (bool, error) {
	key := c.getKey("visited", url)
	exists, err := c.client.Exists(c.ctx, key).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// SetPageCache 缓存页面内容
func (c *Cache) SetPageCache(url string, content []byte, ttl time.Duration) error {
	key := c.getKey("page", url)
	return c.client.Set(c.ctx, key, content, ttl).Err()
}

// GetPageCache 获取缓存的页面内容
func (c *Cache) GetPageCache(url string) ([]byte, error) {
	key := c.getKey("page", url)
	val, err := c.client.Get(c.ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // 缓存不存在
	}
	return val, err
}

// PushQueue 推入队列
func (c *Cache) PushQueue(queueName string, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	key := c.getKey("queue", queueName)
	return c.client.LPush(c.ctx, key, bytes).Err()
}

// PopQueue 从队列取出
func (c *Cache) PopQueue(queueName string) ([]byte, error) {
	key := c.getKey("queue", queueName)
	val, err := c.client.RPop(c.ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // 队列为空
	}
	return val, err
}

// GetQueueLength 获取队列长度
func (c *Cache) GetQueueLength(queueName string) (int64, error) {
	key := c.getKey("queue", queueName)
	return c.client.LLen(c.ctx, key).Result()
}

// IncrCounter 增加计数器
func (c *Cache) IncrCounter(name string) (int64, error) {
	key := c.getKey("counter", name)
	return c.client.Incr(c.ctx, key).Result()
}

// GetCounter 获取计数器值
func (c *Cache) GetCounter(name string) (int64, error) {
	key := c.getKey("counter", name)
	val, err := c.client.Get(c.ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// SetHash 设置哈希表字段
func (c *Cache) SetHash(name string, field string, value interface{}) error {
	key := c.getKey("hash", name)
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.HSet(c.ctx, key, field, bytes).Err()
}

// GetHash 获取哈希表字段
func (c *Cache) GetHash(name string, field string) ([]byte, error) {
	key := c.getKey("hash", name)
	val, err := c.client.HGet(c.ctx, key, field).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

// Close 关闭Redis连接
func (c *Cache) Close() error {
	return c.client.Close()
}

// getKey 生成带前缀的key
func (c *Cache) getKey(keyType string, key string) string {
	return fmt.Sprintf("%s:%s:%s", c.prefix, keyType, key)
}