package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	
	"github.com/zzayne/go-crawler/engine"
	"github.com/zzayne/go-crawler/fetcher"
	"github.com/zzayne/go-crawler/parser"
	"github.com/zzayne/go-crawler/persist/postgres"
	"github.com/zzayne/go-crawler/scheduler"
)

var (
	// 命令行参数
	startURL    = flag.String("url", "", "Starting URL for crawling")
	dbHost      = flag.String("db-host", "localhost", "PostgreSQL host")
	dbPort      = flag.Int("db-port", 5432, "PostgreSQL port")
	dbUser      = flag.String("db-user", "postgres", "PostgreSQL user")
	dbPassword  = flag.String("db-pass", "postgres", "PostgreSQL password")
	dbName      = flag.String("db-name", "crawler", "PostgreSQL database name")
	workerCount = flag.Int("workers", 10, "Number of concurrent workers")
	usePostgres = flag.Bool("use-postgres", false, "Use PostgreSQL for storage")
)

func main() {
	flag.Parse()
	
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
		WorkerCount: *workerCount,
	}
	
	// 如果启用PostgreSQL存储
	if *usePostgres {
		// 连接数据库
		dbConfig := postgres.Config{
			Host:     *dbHost,
			Port:     *dbPort,
			User:     *dbUser,
			Password: *dbPassword,
			DBName:   *dbName,
			SSLMode:  "disable",
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
	// 这里可以根据需要配置不同的解析器
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

// Example usage:
// go run main.go -url https://example.com -use-postgres -workers 5
// go run main.go -url https://example.com -use-postgres -db-host localhost -db-user myuser -db-pass mypass