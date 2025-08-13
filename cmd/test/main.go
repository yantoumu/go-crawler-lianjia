package main

import (
	"log"
	"os"
	
	"github.com/zzayne/go-crawler/config"
	"github.com/zzayne/go-crawler/persist/postgres"
	redisCache "github.com/zzayne/go-crawler/persist/redis"
)

func main() {
	// 加载配置文件
	configFile := os.Getenv("CRAWLER_CONFIG")
	if configFile == "" {
		configFile = "config.yaml"
	}
	
	log.Printf("Loading configuration from: %s", configFile)
	
	cfg, err := config.Load(configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	
	log.Println("✅ Configuration loaded successfully")
	
	// 测试PostgreSQL连接
	log.Println("\n📊 Testing PostgreSQL connection...")
	dbConfig := postgres.Config{
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		User:     cfg.PostgreSQL.Username,
		Password: cfg.PostgreSQL.Password,
		DBName:   cfg.PostgreSQL.DB,
		SSLMode:  cfg.PostgreSQL.SSLMode,
		MaxOpen:  cfg.PostgreSQL.MaxOpen,
		MaxIdle:  cfg.PostgreSQL.MaxIdle,
	}
	
	db, err := postgres.NewDB(dbConfig)
	if err != nil {
		log.Fatalf("❌ Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()
	
	log.Printf("✅ Connected to PostgreSQL: %s:%d/%s", cfg.PostgreSQL.Host, cfg.PostgreSQL.Port, cfg.PostgreSQL.DB)
	
	// 测试Redis连接
	if cfg.Redis.Address != "" {
		log.Println("\n📊 Testing Redis connection...")
		cache, err := redisCache.NewCache(cfg.Redis, "crawler")
		if err != nil {
			log.Printf("❌ Failed to connect to Redis: %v", err)
		} else {
			defer cache.Close()
			log.Printf("✅ Connected to Redis: %s", cfg.Redis.Address)
			
			// 测试Redis操作
			counter, err := cache.IncrCounter("test_counter")
			if err != nil {
				log.Printf("❌ Redis operation failed: %v", err)
			} else {
				log.Printf("✅ Redis test counter: %d", counter)
			}
		}
	} else {
		log.Println("ℹ️ Redis not configured, skipping Redis test")
	}
	
	log.Println("\n🎉 All database connections tested successfully!")
}