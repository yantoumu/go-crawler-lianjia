package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	
	"github.com/zzayne/go-crawler/config"
	"github.com/zzayne/go-crawler/engine"
	"github.com/zzayne/go-crawler/fetcher"
	"github.com/zzayne/go-crawler/parser"
	"github.com/zzayne/go-crawler/persist/postgres"
	redisCache "github.com/zzayne/go-crawler/persist/redis"
	"github.com/zzayne/go-crawler/scheduler"
	"github.com/zzayne/go-crawler/service/domain"
)

var (
	// 全局数据库连接
	db              *postgres.DB
	cache           *redisCache.Cache
	domainProcessor *domain.DomainProcessor
)

func init() {
	// 自动加载配置文件
	configFile := os.Getenv("CRAWLER_CONFIG")
	if configFile == "" {
		configFile = "config.yaml"
	}
	
	cfg, err := config.Load(configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	
	// 自动初始化PostgreSQL连接
	initPostgreSQL(cfg)
	
	// 自动初始化Redis连接（如果配置了）
	if cfg.Redis.Address != "" {
		initRedis(cfg)
	}
	
	// 自动初始化域名处理器（如果启用了）
	if cfg.DomainProcessor.Enabled {
		initDomainProcessor(cfg)
	}
	
	log.Println("✅ All services initialized successfully")
}

func initPostgreSQL(cfg *config.Config) {
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
	
	var err error
	db, err = postgres.NewDB(dbConfig)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	
	log.Printf("Connected to PostgreSQL: %s:%d/%s", cfg.PostgreSQL.Host, cfg.PostgreSQL.Port, cfg.PostgreSQL.DB)
}

func initRedis(cfg *config.Config) {
	var err error
	cache, err = redisCache.NewCache(cfg.Redis, "crawler")
	if err != nil {
		log.Printf("Warning: Failed to connect to Redis: %v", err)
		cache = nil
	} else {
		log.Printf("Connected to Redis: %s", cfg.Redis.Address)
	}
}

func initDomainProcessor(cfg *config.Config) {
	if db == nil {
		log.Printf("Warning: Cannot initialize domain processor without database connection")
		return
	}
	
	// 创建处理器配置
	processorConfig := domain.ProcessorConfig{
		BatchSize:    cfg.DomainProcessor.BatchSize,
		WorkerCount:  cfg.DomainProcessor.WorkerCount,
		PollInterval: time.Duration(cfg.DomainProcessor.PollInterval) * time.Second,
		RateLimit:    time.Duration(cfg.DomainProcessor.RateLimit) * time.Millisecond,
	}
	
	// 创建域名处理器
	domainProcessor = domain.NewDomainProcessor(db, processorConfig)
	
	log.Printf("Domain processor initialized: batch=%d, workers=%d, poll=%ds", 
		processorConfig.BatchSize, processorConfig.WorkerCount, cfg.DomainProcessor.PollInterval)
}

func main() {
	// 确保程序退出时关闭所有服务
	defer func() {
		// 先停止域名处理器
		if domainProcessor != nil {
			if err := domainProcessor.Stop(); err != nil {
				log.Printf("Error stopping domain processor: %v", err)
			}
		}
		
		// 再关闭数据库连接
		if db != nil {
			db.Close()
			log.Println("PostgreSQL connection closed")
		}
		if cache != nil {
			cache.Close()
			log.Println("Redis connection closed")
		}
	}()
	
	// 创建context用于优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// 设置信号处理
	setupSignalHandler(cancel)
	
	// 获取配置
	cfg := config.Get()
	
	// 启动域名处理器（如果启用了）
	if domainProcessor != nil {
		if err := domainProcessor.Start(); err != nil {
			log.Printf("Warning: Failed to start domain processor: %v", err)
		} else {
			log.Printf("✅ Domain processor started successfully")
		}
	}
	
	// 示例：启动一个简单的爬虫任务
	runCrawler(ctx, cfg)
}

// runCrawler 运行爬虫
func runCrawler(ctx context.Context, cfg *config.Config) {
	// 这里可以根据需要修改为从数据库读取任务或其他方式
	startURL := os.Getenv("START_URL")
	if startURL == "" {
		// 如果没有指定URL，可以从数据库读取任务或使用默认URL
		log.Println("No START_URL specified. Ready to accept crawl tasks...")
		
		// 示例：可以在这里实现从数据库读取待爬取的URL队列
		// tasks := loadTasksFromDB(db)
		// for _, task := range tasks {
		//     processCrawlTask(ctx, task)
		// }
		
		// 或者启动一个HTTP服务器接收爬取请求
		// startHTTPServer(ctx)
		
		// 目前只是等待信号
		<-ctx.Done()
		return
	}
	
	// 创建引擎
	e := engine.Engine{
		Scheduler:   &scheduler.QueueScheduler{},
		WorkerCount: cfg.Crawler.WorkerCount,
	}
	
	// 创建ItemSaver
	itemChan, err := postgres.ItemSaver(ctx, db)
	if err != nil {
		log.Fatalf("Failed to create item saver: %v", err)
	}
	e.ItemChan = itemChan
	
	// 创建通用解析器
	itemParser := &parser.ItemParser{
		TitleSelector:   "h1",
		ContentSelector: "p",
		LinkSelector:    "a[href]",
		AttributeSelectors: map[string]string{
			"author": ".author",
			"date":   ".date",
		},
	}
	
	// 检查Redis缓存
	if cache != nil {
		visited, _ := cache.IsVisited(startURL)
		if visited {
			log.Printf("URL %s already visited (cached)", startURL)
			return
		}
		// 标记为已访问
		cache.SetVisited(startURL)
	}
	
	log.Printf("Starting crawler with URL: %s", startURL)
	log.Printf("Workers: %d, Rate limit: %dms", cfg.Crawler.WorkerCount, cfg.Crawler.RateLimit)
	
	// 启动爬虫
	e.Run(engine.Request{
		URL:       startURL,
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

// 可选：添加一些辅助函数

// TestDatabaseConnection 测试数据库连接
func TestDatabaseConnection() error {
	// 测试PostgreSQL
	if db != nil {
		if err := db.Ping(); err != nil {
			return err
		}
		log.Println("✅ PostgreSQL connection test passed")
	}
	
	// 测试Redis
	if cache != nil {
		if _, err := cache.IncrCounter("test"); err != nil {
			return err
		}
		log.Println("✅ Redis connection test passed")
	}
	
	return nil
}

// GetDB 获取数据库连接（供其他包使用）
func GetDB() *postgres.DB {
	return db
}

// GetCache 获取Redis缓存（供其他包使用）
func GetCache() *redisCache.Cache {
	return cache
}

// GetDomainProcessor 获取域名处理器（供其他包使用）
func GetDomainProcessor() *domain.DomainProcessor {
	return domainProcessor
}

// GetDomainProcessorStats 获取域名处理器统计信息
func GetDomainProcessorStats() (*domain.ProcessorStats, error) {
	if domainProcessor == nil {
		return nil, fmt.Errorf("domain processor not initialized")
	}
	return domainProcessor.GetStats()
}

// RestartDomainProcessor 重启域名处理器
func RestartDomainProcessor() error {
	if domainProcessor == nil {
		return fmt.Errorf("domain processor not initialized")
	}
	
	log.Printf("Restarting domain processor...")
	
	// 停止处理器
	if err := domainProcessor.Stop(); err != nil {
		return fmt.Errorf("failed to stop domain processor: %w", err)
	}
	
	// 重新启动
	if err := domainProcessor.Start(); err != nil {
		return fmt.Errorf("failed to start domain processor: %w", err)
	}
	
	log.Printf("Domain processor restarted successfully")
	return nil
}