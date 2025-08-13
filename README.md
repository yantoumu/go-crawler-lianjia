# Go通用爬虫框架

一个高性能、可扩展的Go语言爬虫框架，支持PostgreSQL存储和自定义解析器。

## 特性

- ✅ 高并发爬取
- ✅ PostgreSQL数据存储
- ✅ 灵活的解析器配置
- ✅ 优雅关闭机制
- ✅ 内存安全（无goroutine泄漏）
- ✅ 可配置的工作线程数
- ✅ 支持自定义CSS选择器

## 安装

```bash
# 克隆项目
git clone https://github.com/yourusername/go-crawler.git
cd go-crawler

# 安装依赖
go mod tidy

# 创建PostgreSQL数据库
createdb crawler

# 执行数据库迁移
psql -U postgres -d crawler < migrations/001_create_general_tables.sql
```

## 使用方法

### 基本使用

```bash
# 简单爬取（不保存到数据库）
go run main.go -url https://example.com

# 使用PostgreSQL存储
go run main.go -url https://example.com -use-postgres

# 自定义配置
go run main.go \
  -url https://example.com \
  -use-postgres \
  -db-host localhost \
  -db-port 5432 \
  -db-user myuser \
  -db-pass mypass \
  -db-name crawler \
  -workers 20
```

### 命令行参数

- `-url`: 爬取的起始URL（必需）
- `-use-postgres`: 启用PostgreSQL存储
- `-db-host`: PostgreSQL主机（默认: localhost）
- `-db-port`: PostgreSQL端口（默认: 5432）
- `-db-user`: PostgreSQL用户（默认: postgres）
- `-db-pass`: PostgreSQL密码（默认: postgres）
- `-db-name`: PostgreSQL数据库名（默认: crawler）
- `-workers`: 并发工作线程数（默认: 10）

## 自定义解析器

创建自定义解析器来适配不同的网站：

```go
// 创建自定义解析器
itemParser := &parser.ItemParser{
    TitleSelector:   "h1.title",
    ContentSelector: "div.content",
    LinkSelector:    "a.next-page",
    AttributeSelectors: map[string]string{
        "author": "span.author",
        "date":   "time.published",
        "tags":   "div.tags",
    },
}

// 使用解析器
e.Run(engine.Request{
    URL:       "https://example.com",
    ParseFunc: itemParser.Parse,
})
```

## 数据库结构

### crawled_items表
- `id`: 主键
- `url`: 页面URL（唯一）
- `type`: 数据类型
- `title`: 标题
- `content`: 内容
- `attributes`: JSON格式的额外属性
- `crawled_at`: 爬取时间
- `updated_at`: 更新时间

## 架构设计

```
┌─────────────┐
│   Engine    │ ← 核心调度引擎
└──────┬──────┘
       │
┌──────▼──────┐
│  Scheduler  │ ← 任务调度器
└──────┬──────┘
       │
┌──────▼──────┐
│   Fetcher   │ ← 页面获取器
└──────┬──────┘
       │
┌──────▼──────┐
│   Parser    │ ← 内容解析器
└──────┬──────┘
       │
┌──────▼──────┐
│  PostgreSQL │ ← 数据存储
└─────────────┘
```

## 性能优化

- 使用连接池管理数据库连接
- 实现了优雅关闭机制
- 修复了所有goroutine泄漏问题
- 支持并发控制和速率限制

## 开发

```bash
# 运行测试
go test ./...

# 构建二进制文件
go build -o crawler

# 检测数据竞争
go build -race -o crawler
./crawler -url https://example.com
```

## License

MIT