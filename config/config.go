package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	
	"gopkg.in/yaml.v3"
)

// Config 全局配置结构
type Config struct {
	PostgreSQL     PostgreSQLConfig     `yaml:"postgresql"`
	Redis          RedisConfig          `yaml:"redis"`
	Crawler        CrawlerConfig        `yaml:"crawler"`
	DomainProcessor DomainProcessorConfig `yaml:"domainProcessor"`
}

// PostgreSQLConfig PostgreSQL数据库配置
type PostgreSQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DB       string `yaml:"db"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
	Timezone string `yaml:"timezone"`
	MaxIdle  int    `yaml:"maxIdle"`
	MaxOpen  int    `yaml:"maxOpen"`
	LogLevel string `yaml:"logLevel"`
}

// RedisConfig Redis配置
type RedisConfig struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// CrawlerConfig 爬虫配置
type CrawlerConfig struct {
	WorkerCount    int    `yaml:"workerCount"`
	RequestTimeout int    `yaml:"requestTimeout"`
	RateLimit      int    `yaml:"rateLimit"`
	UserAgent      string `yaml:"userAgent"`
}

// DomainProcessorConfig 域名处理器配置
type DomainProcessorConfig struct {
	Enabled      bool     `yaml:"enabled"`
	BatchSize    int      `yaml:"batchSize"`
	WorkerCount  int      `yaml:"workerCount"`
	PollInterval int      `yaml:"pollInterval"`    // 轮询间隔，秒
	RateLimit    int      `yaml:"rateLimit"`       // 请求间隔，毫秒
	APIEndpoints []string `yaml:"apiEndpoints"`    // WHOIS API端点
}

var globalConfig *Config

// Load 加载配置文件
func Load(configPath string) (*Config, error) {
	// 如果文件不存在，尝试从环境变量获取路径
	if configPath == "" {
		configPath = os.Getenv("CRAWLER_CONFIG")
		if configPath == "" {
			configPath = "config.yaml"
		}
	}
	
	// 检查文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", configPath)
	}
	
	// 读取配置文件
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	
	// 解析YAML
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	
	// 设置默认值
	setDefaults(&config)
	
	// 保存到全局变量
	globalConfig = &config
	
	log.Printf("Configuration loaded from %s", configPath)
	return &config, nil
}

// setDefaults 设置默认值
func setDefaults(cfg *Config) {
	// PostgreSQL默认值
	if cfg.PostgreSQL.Port == 0 {
		cfg.PostgreSQL.Port = 5432
	}
	if cfg.PostgreSQL.SSLMode == "" {
		cfg.PostgreSQL.SSLMode = "disable"
	}
	if cfg.PostgreSQL.MaxIdle == 0 {
		cfg.PostgreSQL.MaxIdle = 10
	}
	if cfg.PostgreSQL.MaxOpen == 0 {
		cfg.PostgreSQL.MaxOpen = 50
	}
	if cfg.PostgreSQL.Timezone == "" {
		cfg.PostgreSQL.Timezone = "Asia/Shanghai"
	}
	
	// Crawler默认值
	if cfg.Crawler.WorkerCount == 0 {
		cfg.Crawler.WorkerCount = 10
	}
	if cfg.Crawler.RequestTimeout == 0 {
		cfg.Crawler.RequestTimeout = 30
	}
	if cfg.Crawler.RateLimit == 0 {
		cfg.Crawler.RateLimit = 200 // 200ms between requests
	}
	if cfg.Crawler.UserAgent == "" {
		cfg.Crawler.UserAgent = "Go-Crawler/1.0"
	}
	
	// DomainProcessor默认值
	if cfg.DomainProcessor.BatchSize == 0 {
		cfg.DomainProcessor.BatchSize = 10
	}
	if cfg.DomainProcessor.WorkerCount == 0 {
		cfg.DomainProcessor.WorkerCount = 3
	}
	if cfg.DomainProcessor.PollInterval == 0 {
		cfg.DomainProcessor.PollInterval = 30 // 30 seconds
	}
	if cfg.DomainProcessor.RateLimit == 0 {
		cfg.DomainProcessor.RateLimit = 200 // 200ms between requests
	}
	if len(cfg.DomainProcessor.APIEndpoints) == 0 {
		cfg.DomainProcessor.APIEndpoints = []string{
			"https://domain.seo9.org/query",
			"https://whois.seokey.vip/query",
			"https://whois-rdap-worker.gamesvchost.workers.dev/query",
		}
	}
}

// Get 获取全局配置
func Get() *Config {
	if globalConfig == nil {
		panic("configuration not loaded")
	}
	return globalConfig
}

// GetPostgreSQLDSN 获取PostgreSQL连接字符串
func (c *PostgreSQLConfig) GetDSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		c.Host, c.Port, c.Username, c.Password, c.DB, c.SSLMode, c.Timezone)
}