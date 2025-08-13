package domain

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/zzayne/go-crawler/model"
	"github.com/zzayne/go-crawler/persist/postgres"
	"github.com/zzayne/go-crawler/service/whois"
)

const (
	DefaultBatchSize    = 10
	DefaultWorkerCount  = 3
	DefaultPollInterval = 30 * time.Second
	MaxRetryCount       = 3
)

// ProcessorConfig 处理器配置
type ProcessorConfig struct {
	BatchSize    int
	WorkerCount  int
	PollInterval time.Duration
	RateLimit    time.Duration
}

// DefaultProcessorConfig 默认配置
func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		BatchSize:    DefaultBatchSize,
		WorkerCount:  DefaultWorkerCount,
		PollInterval: DefaultPollInterval,
		RateLimit:    200 * time.Millisecond,
	}
}

// DomainDAO 域名数据访问对象
type DomainDAO struct {
	db *postgres.DB
}

// NewDomainDAO 创建域名DAO
func NewDomainDAO(db *postgres.DB) *DomainDAO {
	return &DomainDAO{db: db}
}

// GetPendingDomains 获取待处理的域名（带锁）
func (dao *DomainDAO) GetPendingDomains(ctx context.Context, limit int) ([]*model.DomainQueue, error) {
	tx, err := dao.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// 查询并锁定待处理的域名
	query := `
		SELECT id, domain, tld, discovered_date, discovered_timestamp, 
			   processing_status, processed_at, retry_count, created_at, updated_at
		FROM new_domains_queue 
		WHERE (processing_status = 'pending' OR processing_status = 'failed') 
		  AND retry_count < $1
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.QueryContext(ctx, query, MaxRetryCount, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending domains: %w", err)
	}
	defer rows.Close()

	var domains []*model.DomainQueue
	var domainIDs []int64

	for rows.Next() {
		domain := &model.DomainQueue{}
		var processedAt sql.NullTime

		err := rows.Scan(
			&domain.ID,
			&domain.Domain,
			&domain.TLD,
			&domain.DiscoveredDate,
			&domain.DiscoveredTimestamp,
			&domain.ProcessingStatus,
			&processedAt,
			&domain.RetryCount,
			&domain.CreatedAt,
			&domain.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan domain: %w", err)
		}

		if processedAt.Valid {
			domain.ProcessedAt = &processedAt.Time
		}

		domains = append(domains, domain)
		domainIDs = append(domainIDs, domain.ID)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// 批量更新状态为processing
	if len(domainIDs) > 0 {
		err = dao.markDomainsAsProcessing(ctx, tx, domainIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to mark domains as processing: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Retrieved %d pending domains for processing", len(domains))
	return domains, nil
}

// markDomainsAsProcessing 批量标记域名为处理中
func (dao *DomainDAO) markDomainsAsProcessing(ctx context.Context, tx *sql.Tx, domainIDs []int64) error {
	if len(domainIDs) == 0 {
		return nil
	}

	// 使用PostgreSQL的ANY数组参数，避免SQL注入风险
	query := `
		UPDATE new_domains_queue 
		SET processing_status = 'processing', 
			processed_at = NULL,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ANY($1)`

	// 使用pq.Array安全地传递数组参数
	_, err := tx.ExecContext(ctx, query, pq.Array(domainIDs))
	if err != nil {
		return fmt.Errorf("failed to mark domains as processing: %w", err)
	}
	
	return nil
}

// UpdateDomainResult 更新域名查询结果
func (dao *DomainDAO) UpdateDomainResult(ctx context.Context, domainID int64, response *model.WhoisResponse, success bool) error {
	var query string
	var args []interface{}

	now := time.Now()

	if success && response != nil {
		// 成功的情况
		query = `
			UPDATE new_domains_queue 
			SET processing_status = 'completed',
				processed_at = $2,
				updated_at = $3
			WHERE id = $1`
		args = []interface{}{domainID, now, now}
	} else {
		// 失败的情况，增加重试次数
		query = `
			UPDATE new_domains_queue 
			SET processing_status = CASE 
					WHEN retry_count + 1 >= $2 THEN 'failed' 
					ELSE 'pending' 
				END,
				processed_at = CASE 
					WHEN retry_count + 1 >= $2 THEN $3 
					ELSE NULL 
				END,
				retry_count = retry_count + 1,
				updated_at = $3
			WHERE id = $1`
		args = []interface{}{domainID, MaxRetryCount, now}
	}

	_, err := dao.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update domain result: %w", err)
	}

	return nil
}

// CleanupStaleProcessing 清理过期的processing记录
func (dao *DomainDAO) CleanupStaleProcessing(ctx context.Context) (int, error) {
	query := `
		SELECT cleanup_stale_processing_domains()`

	var count int
	err := dao.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup stale processing domains: %w", err)
	}

	if count > 0 {
		log.Printf("Cleaned up %d stale processing domains", count)
	}

	return count, nil
}

// GetQueueStats 获取队列统计信息
func (dao *DomainDAO) GetQueueStats(ctx context.Context) ([]*model.DomainStats, error) {
	query := `
		SELECT processing_status, count, avg_retry_count, 
			   oldest_created, newest_created, today_count
		FROM v_domains_queue_stats`

	rows, err := dao.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue stats: %w", err)
	}
	defer rows.Close()

	var stats []*model.DomainStats
	for rows.Next() {
		stat := &model.DomainStats{}
		var statusStr string

		err := rows.Scan(
			&statusStr,
			&stat.Count,
			&stat.AvgRetryCount,
			&stat.OldestCreated,
			&stat.NewestCreated,
			&stat.TodayCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan stats: %w", err)
		}

		stat.Status = model.DomainProcessingStatus(statusStr)
		stats = append(stats, stat)
	}

	return stats, rows.Err()
}

// DomainProcessor 域名处理器
type DomainProcessor struct {
	config      ProcessorConfig
	dao         *DomainDAO
	whoisClient *whois.WhoisClient
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	running     bool
	mu          sync.RWMutex
}

// NewDomainProcessor 创建域名处理器
func NewDomainProcessor(db *postgres.DB, config ProcessorConfig) *DomainProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	return &DomainProcessor{
		config:      config,
		dao:         NewDomainDAO(db),
		whoisClient: whois.NewWhoisClient(nil, config.RateLimit),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start 启动处理器
func (p *DomainProcessor) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("processor already running")
	}

	p.running = true
	log.Printf("Starting domain processor with %d workers, batch size %d", 
		p.config.WorkerCount, p.config.BatchSize)

	// 启动清理goroutine
	p.wg.Add(1)
	go p.cleanupWorker()

	// 启动主处理goroutine
	p.wg.Add(1)
	go p.processWorker()

	return nil
}

// Stop 停止处理器
func (p *DomainProcessor) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	log.Printf("Stopping domain processor...")
	
	// 取消上下文
	p.cancel()
	
	// 等待所有goroutine完成
	p.wg.Wait()
	
	p.running = false
	log.Printf("Domain processor stopped")
	
	return nil
}

// IsRunning 检查是否正在运行
func (p *DomainProcessor) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

// cleanupWorker 清理工作者
func (p *DomainProcessor) cleanupWorker() {
	defer p.wg.Done()

	ticker := time.NewTicker(5 * time.Minute) // 每5分钟清理一次
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			log.Printf("Cleanup worker stopping...")
			return
		case <-ticker.C:
			if _, err := p.dao.CleanupStaleProcessing(p.ctx); err != nil {
				log.Printf("Error cleaning up stale processing domains: %v", err)
			}
		}
	}
}

// processWorker 主处理工作者
func (p *DomainProcessor) processWorker() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	log.Printf("Process worker started, polling every %v", p.config.PollInterval)

	for {
		select {
		case <-p.ctx.Done():
			log.Printf("Process worker stopping...")
			return
		case <-ticker.C:
			if err := p.processBatch(); err != nil {
				log.Printf("Error processing batch: %v", err)
				// 出错时等待更长时间再重试
				time.Sleep(time.Minute)
			}
		}
	}
}

// processBatch 处理一批域名
func (p *DomainProcessor) processBatch() error {
	// 获取待处理的域名
	domains, err := p.dao.GetPendingDomains(p.ctx, p.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to get pending domains: %w", err)
	}

	if len(domains) == 0 {
		// 没有待处理的域名，跳过日志
		return nil
	}

	log.Printf("Processing batch of %d domains", len(domains))

	// 创建工作通道
	domainChan := make(chan *model.DomainQueue, len(domains))
	resultChan := make(chan ProcessResult, len(domains))

	// 发送域名到通道
	for _, domain := range domains {
		domainChan <- domain
	}
	close(domainChan)

	// 启动worker goroutines
	var workerWg sync.WaitGroup
	workerCount := p.config.WorkerCount
	if len(domains) < workerCount {
		workerCount = len(domains)
	}

	for i := 0; i < workerCount; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			p.domainWorker(workerID, domainChan, resultChan)
		}(i)
	}

	// 等待所有worker完成
	go func() {
		workerWg.Wait()
		close(resultChan)
	}()

	// 收集结果并更新数据库
	successCount := 0
	errorCount := 0

	for result := range resultChan {
		err := p.dao.UpdateDomainResult(p.ctx, result.DomainID, result.Response, result.Success)
		if err != nil {
			log.Printf("Failed to update domain %d result: %v", result.DomainID, err)
			errorCount++
		} else if result.Success {
			successCount++
		} else {
			errorCount++
		}
	}

	log.Printf("Batch processing completed: %d successful, %d errors", successCount, errorCount)
	return nil
}

// ProcessResult 处理结果
type ProcessResult struct {
	DomainID int64
	Domain   string
	Response *model.WhoisResponse
	Success  bool
	Error    error
}

// domainWorker 域名处理worker
func (p *DomainProcessor) domainWorker(workerID int, domainChan <-chan *model.DomainQueue, resultChan chan<- ProcessResult) {
	for domain := range domainChan {
		select {
		case <-p.ctx.Done():
			// 上下文被取消，停止处理
			return
		default:
		}

		result := ProcessResult{
			DomainID: domain.ID,
			Domain:   domain.Domain,
		}

		log.Printf("Worker %d processing domain: %s (attempt %d)", 
			workerID, domain.Domain, domain.RetryCount+1)

		// 查询域名信息
		response, err := p.whoisClient.Query(p.ctx, domain.Domain)
		if err != nil {
			result.Error = err
			result.Success = false
			log.Printf("Worker %d failed to query domain %s: %v", 
				workerID, domain.Domain, err)
		} else {
			result.Response = response
			result.Success = true
			log.Printf("Worker %d successfully queried domain %s (registered: %t)", 
				workerID, domain.Domain, response.Registered)
		}

		// 发送结果
		select {
		case resultChan <- result:
		case <-p.ctx.Done():
			return
		}
	}
}

// GetStats 获取处理器统计信息
func (p *DomainProcessor) GetStats() (*ProcessorStats, error) {
	queueStats, err := p.dao.GetQueueStats(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue stats: %w", err)
	}

	endpointStats := p.whoisClient.GetEndpointStats()
	healthyEndpoints := p.whoisClient.GetHealthyEndpoints()

	return &ProcessorStats{
		Running:          p.IsRunning(),
		QueueStats:       queueStats,
		EndpointStats:    endpointStats,
		HealthyEndpoints: healthyEndpoints,
		TotalEndpoints:   len(endpointStats),
	}, nil
}

// ProcessorStats 处理器统计信息
type ProcessorStats struct {
	Running          bool                        `json:"running"`
	QueueStats       []*model.DomainStats        `json:"queue_stats"`
	EndpointStats    map[string]bool             `json:"endpoint_stats"`
	HealthyEndpoints int                         `json:"healthy_endpoints"`
	TotalEndpoints   int                         `json:"total_endpoints"`
}