package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	
	"github.com/zzayne/go-crawler/config"
	"github.com/zzayne/go-crawler/engine"
	"github.com/zzayne/go-crawler/fetcher"
	"github.com/zzayne/go-crawler/parser"
	"github.com/zzayne/go-crawler/persist/postgres"
	redisCache "github.com/zzayne/go-crawler/persist/redis"
	"github.com/zzayne/go-crawler/scheduler"
)

var (
	// 命令行参数
	configFile  = flag.String("config", "config.yaml", "Configuration file path")
	startURL    = flag.String("url", "", "Starting URL for crawling")
	usePostgres = flag.Bool("use-postgres", true, "Use PostgreSQL for storage")
	useRedis    = flag.Bool("use-redis", false, "Use Redis for caching")
)

func main() {
	flag.Parse()
	
	// 加载配置文件
	cfg, err := config.Load(*configFile)
	if err != nil {
		// 如果配置文件不存在，使用默认配置
		log.Printf("Warning: %v, using default configuration", err)
		cfg = &config.Config{
			PostgreSQL: config.PostgreSQLConfig{
				Host:     "localhost",
				Port:     5432,
				Username: "postgres",
				Password: "postgres",
				DB:       "crawler",
				SSLMode:  "disable",
				MaxOpen:  50,
				MaxIdle:  10,
			},
			Crawler: config.CrawlerConfig{
				WorkerCount:    10,
				RequestTimeout: 30,
				RateLimit:      200,
				UserAgent:      "Go-Crawler/1.0",
			},
		}
	}
	
	if *startURL == "" {
		log.Fatal("Please provide a starting URL with -url flag")
	}
	
	// 创建context用于优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// 设置信号处理
	setupSignalHandler(cancel)
	
	// 创建引擎
	e := engine.Engine{
		Scheduler:   &scheduler.QueueScheduler{},
		WorkerCount: cfg.Crawler.WorkerCount,
	}
	
	// Redis缓存（可选）
	var cache *redisCache.Cache
	if *useRedis && cfg.Redis.Address != "" {
		cache, err = redisCache.NewCache(cfg.Redis, "crawler")
		if err != nil {
			log.Printf("Failed to connect to Redis: %v", err)
		} else {
			defer cache.Close()
			log.Println("Redis cache enabled")
		}
	}
	
	// PostgreSQL存储
	if *usePostgres {
		// 转换配置格式
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
			log.Fatalf("Failed to connect to database: %v", err)
		}
		defer db.Close()
		
		// 创建ItemSaver
		itemChan, err := postgres.ItemSaver(ctx, db)
		if err != nil {
			log.Fatalf("Failed to create item saver: %v", err)
		}
		e.ItemChan = itemChan
	}
	
	// 创建通用解析器示例
	itemParser := &parser.ItemParser{
		TitleSelector:   "h1",
		ContentSelector: "p",
		LinkSelector:    "a[href]",
		AttributeSelectors: map[string]string{
			"author": ".author",
			"date":   ".date",
		},
	}
	
	// 启动爬虫
	log.Printf("Starting crawler with URL: %s", *startURL)
	log.Printf("Workers: %d, Rate limit: %dms", cfg.Crawler.WorkerCount, cfg.Crawler.RateLimit)
	
	// 如果有Redis缓存，检查URL是否已访问
	if cache != nil {
		visited, _ := cache.IsVisited(*startURL)
		if visited {
			log.Printf("URL %s already visited (cached)", *startURL)
		}
	}
	
	e.Run(engine.Request{
		URL:       *startURL,
		ParseFunc: itemParser.Parse,
	})
}

// setupSignalHandler 设置信号处理器
func setupSignalHandler(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("Received shutdown signal, gracefully stopping...")
		cancel()
		
		// 清理资源
		fetcher.Cleanup()
	}()
}