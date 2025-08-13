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

### 配置设置

1. **复制配置文件模板**：
```bash
cp config.example.yaml config.yaml
```

2. **编辑配置文件**，填入你的数据库信息：
```yaml
postgresql:
  host: your-db-host
  port: 5432
  db: your-database
  username: your-username
  password: your-password
```

3. **创建数据库表**：
```bash
psql -U your-username -d your-database < migrations/001_create_general_tables.sql
```

### 运行方式

程序启动时会**自动初始化数据库连接**，无需手动指定：

```bash
# 直接运行（自动连接数据库）
go run main.go

# 指定要爬取的URL（通过环境变量）
START_URL=https://example.com go run main.go

# 使用不同的配置文件
CRAWLER_CONFIG=myconfig.yaml go run main.go

# 测试数据库连接
go run cmd/test/main.go
```

### 环境变量

- `CRAWLER_CONFIG`: 配置文件路径（默认: config.yaml）
- `START_URL`: 要爬取的起始URL（可选）

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