package model

import (
	"time"
)

// Item 通用的爬取项目结构
type Item struct {
	ID         int64                  `json:"id"`
	URL        string                 `json:"url"`
	Type       string                 `json:"type"`       // 数据类型标识
	Title      string                 `json:"title"`      
	Content    string                 `json:"content"`    
	Attributes map[string]interface{} `json:"attributes"` // 灵活存储各种属性
	CrawledAt  time.Time              `json:"crawled_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
}

// Page 页面结构
type Page struct {
	URL         string
	HTML        string
	StatusCode  int
	Headers     map[string]string
	CrawledAt   time.Time
}

// CrawlTask 爬取任务
type CrawlTask struct {
	ID          int64
	Name        string
	StartURL    string
	Selectors   map[string]string // CSS选择器配置
	MaxDepth    int
	Concurrency int
	Status      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}