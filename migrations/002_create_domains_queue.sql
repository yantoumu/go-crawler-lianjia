-- 创建域名查询队列表
CREATE TABLE IF NOT EXISTS new_domains_queue (
    id BIGSERIAL PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    tld VARCHAR(63),                                    -- Top Level Domain
    discovered_date DATE NOT NULL,
    discovered_timestamp TIMESTAMP NOT NULL,
    processing_status VARCHAR(20) DEFAULT 'pending' NOT NULL
        CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed')),
    processed_at TIMESTAMP,
    retry_count INT DEFAULT 0 NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 域名唯一约束
    CONSTRAINT uk_domain UNIQUE (domain),
    
    -- 重试次数限制
    CONSTRAINT chk_retry_count CHECK (retry_count >= 0 AND retry_count <= 10),
    
    -- 状态逻辑约束
    CONSTRAINT chk_processed_at CHECK (
        (processing_status IN ('completed', 'failed') AND processed_at IS NOT NULL) OR 
        (processing_status IN ('pending', 'processing') AND processed_at IS NULL)
    )
);

-- 性能优化索引
-- 主查询索引：查找待处理的域名
CREATE INDEX idx_domains_queue_processing ON new_domains_queue (processing_status, retry_count, created_at) 
    WHERE processing_status IN ('pending', 'failed') AND retry_count < 3;

-- 域名快速查找
CREATE INDEX idx_domains_queue_domain ON new_domains_queue (domain);

-- TLD分析索引
CREATE INDEX idx_domains_queue_tld ON new_domains_queue (tld);

-- 时间范围查询索引
CREATE INDEX idx_domains_queue_discovered_date ON new_domains_queue (discovered_date);
CREATE INDEX idx_domains_queue_processed_at ON new_domains_queue (processed_at) WHERE processed_at IS NOT NULL;

-- 状态监控索引
CREATE INDEX idx_domains_queue_status_retry ON new_domains_queue (processing_status, retry_count);

-- 更新时间触发器 (重用现有函数)
CREATE TRIGGER update_domains_queue_updated_at BEFORE UPDATE ON new_domains_queue
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 清理过期处理中记录的函数
CREATE OR REPLACE FUNCTION cleanup_stale_processing_domains()
RETURNS INT AS $$
DECLARE
    updated_count INT;
BEGIN
    -- 将超过30分钟还在处理中的记录重置为pending状态
    UPDATE new_domains_queue 
    SET processing_status = 'pending',
        processed_at = NULL,
        retry_count = retry_count + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE processing_status = 'processing' 
      AND created_at < NOW() - INTERVAL '30 minutes'
      AND retry_count < 3;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    -- 将重试次数超限的记录标记为failed
    UPDATE new_domains_queue 
    SET processing_status = 'failed',
        processed_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE processing_status IN ('pending', 'processing')
      AND retry_count >= 3;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- 数据统计视图
CREATE OR REPLACE VIEW v_domains_queue_stats AS
SELECT 
    processing_status,
    COUNT(*) as count,
    AVG(retry_count) as avg_retry_count,
    MIN(created_at) as oldest_created,
    MAX(created_at) as newest_created,
    COUNT(CASE WHEN created_at >= CURRENT_DATE THEN 1 END) as today_count
FROM new_domains_queue 
GROUP BY processing_status;

-- 性能统计视图
CREATE OR REPLACE VIEW v_domains_processing_performance AS
SELECT 
    DATE(created_at) as process_date,
    COUNT(*) as total_processed,
    COUNT(CASE WHEN processing_status = 'completed' THEN 1 END) as completed_count,
    COUNT(CASE WHEN processing_status = 'failed' THEN 1 END) as failed_count,
    AVG(CASE WHEN processed_at IS NOT NULL THEN 
        EXTRACT(EPOCH FROM (processed_at - created_at)) 
    END) as avg_processing_time_seconds
FROM new_domains_queue 
WHERE processing_status IN ('completed', 'failed')
GROUP BY DATE(created_at)
ORDER BY process_date DESC;

-- 添加注释
COMMENT ON TABLE new_domains_queue IS '域名查询队列表，存储待查询的域名及其处理状态';
COMMENT ON COLUMN new_domains_queue.domain IS '完整域名，如 example.com';
COMMENT ON COLUMN new_domains_queue.tld IS '顶级域名，如 com, net, org';
COMMENT ON COLUMN new_domains_queue.discovered_date IS '域名发现日期';
COMMENT ON COLUMN new_domains_queue.discovered_timestamp IS '域名发现时间戳';
COMMENT ON COLUMN new_domains_queue.processing_status IS '处理状态：pending(待处理), processing(处理中), completed(已完成), failed(失败)';
COMMENT ON COLUMN new_domains_queue.processed_at IS '处理完成时间';
COMMENT ON COLUMN new_domains_queue.retry_count IS '重试次数，最大3次';

COMMENT ON FUNCTION cleanup_stale_processing_domains() IS '清理长时间处于processing状态的域名记录';
COMMENT ON VIEW v_domains_queue_stats IS '域名队列状态统计视图';
COMMENT ON VIEW v_domains_processing_performance IS '域名处理性能统计视图';