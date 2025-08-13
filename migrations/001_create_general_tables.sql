-- 创建通用爬取项目表
CREATE TABLE IF NOT EXISTS crawled_items (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    type VARCHAR(100),
    title TEXT,
    content TEXT,
    attributes JSONB,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_item_url UNIQUE (url)
);

-- 创建爬取任务表
CREATE TABLE IF NOT EXISTS crawl_tasks (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    start_url TEXT NOT NULL,
    selectors JSONB,
    max_depth INT DEFAULT 3,
    concurrency INT DEFAULT 10,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建页面缓存表
CREATE TABLE IF NOT EXISTS page_cache (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    html TEXT,
    status_code INT,
    headers JSONB,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_page_url UNIQUE (url)
);

-- 创建索引
CREATE INDEX idx_items_type ON crawled_items(type);
CREATE INDEX idx_items_crawled_at ON crawled_items(crawled_at);
CREATE INDEX idx_items_attributes ON crawled_items USING GIN(attributes);
CREATE INDEX idx_tasks_status ON crawl_tasks(status);
CREATE INDEX idx_cache_url ON page_cache(url);
CREATE INDEX idx_cache_crawled_at ON page_cache(crawled_at);

-- 更新时间触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_items_updated_at BEFORE UPDATE ON crawled_items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_tasks_updated_at BEFORE UPDATE ON crawl_tasks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();