package model

import (
	"strings"
	"time"
)

// DomainProcessingStatus 域名处理状态
type DomainProcessingStatus string

const (
	DomainStatusPending    DomainProcessingStatus = "pending"
	DomainStatusProcessing DomainProcessingStatus = "processing"
	DomainStatusCompleted  DomainProcessingStatus = "completed"
	DomainStatusFailed     DomainProcessingStatus = "failed"
)

// String 实现 Stringer 接口
func (s DomainProcessingStatus) String() string {
	return string(s)
}

// IsValid 验证状态是否有效
func (s DomainProcessingStatus) IsValid() bool {
	switch s {
	case DomainStatusPending, DomainStatusProcessing, DomainStatusCompleted, DomainStatusFailed:
		return true
	default:
		return false
	}
}

// DomainQueue 域名查询队列结构
type DomainQueue struct {
	ID                   int64                  `json:"id"`
	Domain               string                 `json:"domain"`
	TLD                  string                 `json:"tld"`
	DiscoveredDate       time.Time              `json:"discovered_date"`
	DiscoveredTimestamp  time.Time              `json:"discovered_timestamp"`
	ProcessingStatus     DomainProcessingStatus `json:"processing_status"`
	ProcessedAt          *time.Time             `json:"processed_at"`
	RetryCount           int                    `json:"retry_count"`
	CreatedAt            time.Time              `json:"created_at"`
	UpdatedAt            time.Time              `json:"updated_at"`
}

// NewDomainQueue 创建新的域名队列记录
func NewDomainQueue(domain string, discoveredAt time.Time) *DomainQueue {
	now := time.Now()
	tld := extractTLD(domain)
	
	return &DomainQueue{
		Domain:              domain,
		TLD:                 tld,
		DiscoveredDate:      discoveredAt.Truncate(24 * time.Hour), // 只保留日期部分
		DiscoveredTimestamp: discoveredAt,
		ProcessingStatus:    DomainStatusPending,
		RetryCount:          0,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

// extractTLD 从域名中提取TLD
func extractTLD(domain string) string {
	parts := strings.Split(domain, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-1]
}

// CanRetry 判断是否可以重试
func (d *DomainQueue) CanRetry() bool {
	return d.RetryCount < 3
}

// IsProcessable 判断是否可以处理
func (d *DomainQueue) IsProcessable() bool {
	return (d.ProcessingStatus == DomainStatusPending || d.ProcessingStatus == DomainStatusFailed) && d.CanRetry()
}

// MarkAsProcessing 标记为处理中
func (d *DomainQueue) MarkAsProcessing() {
	d.ProcessingStatus = DomainStatusProcessing
	d.ProcessedAt = nil
	d.UpdatedAt = time.Now()
}

// MarkAsCompleted 标记为完成
func (d *DomainQueue) MarkAsCompleted() {
	now := time.Now()
	d.ProcessingStatus = DomainStatusCompleted
	d.ProcessedAt = &now
	d.UpdatedAt = now
}

// MarkAsFailed 标记为失败
func (d *DomainQueue) MarkAsFailed() {
	now := time.Now()
	d.ProcessingStatus = DomainStatusFailed
	d.ProcessedAt = &now
	d.RetryCount++
	d.UpdatedAt = now
}

// ResetForRetry 重置为可重试状态
func (d *DomainQueue) ResetForRetry() {
	d.ProcessingStatus = DomainStatusPending
	d.ProcessedAt = nil
	d.RetryCount++
	d.UpdatedAt = time.Now()
}

// IsExpired 检查处理中的记录是否过期（超过30分钟）
func (d *DomainQueue) IsExpired() bool {
	if d.ProcessingStatus != DomainStatusProcessing {
		return false
	}
	// 使用UpdatedAt而不是CreatedAt，因为UpdatedAt反映最后的状态变更时间
	return time.Since(d.UpdatedAt) > 30*time.Minute
}

// WhoisResponse WHOIS查询响应结构
type WhoisResponse struct {
	Domain        string            `json:"domain"`
	Available     bool              `json:"available"`
	Registered    bool              `json:"registered"`
	Registrar     string            `json:"registrar"`
	CreatedDate   *time.Time        `json:"created_date"`
	ExpiryDate    *time.Time        `json:"expiry_date"`
	UpdatedDate   *time.Time        `json:"updated_date"`
	NameServers   []string          `json:"name_servers"`
	Status        []string          `json:"status"`
	Contacts      map[string]string `json:"contacts"`
	RawResponse   string            `json:"raw_response"`
	QueryTime     time.Time         `json:"query_time"`
	APISource     string            `json:"api_source"`
	Error         string            `json:"error,omitempty"`
}

// IsValid 验证响应是否有效
func (w *WhoisResponse) IsValid() bool {
	return w.Domain != "" && w.Error == ""
}

// HasRegistrationInfo 检查是否有注册信息
func (w *WhoisResponse) HasRegistrationInfo() bool {
	return w.Registered && (w.CreatedDate != nil || w.Registrar != "")
}

// DomainBatch 域名批处理结构
type DomainBatch struct {
	Domains     []*DomainQueue `json:"domains"`
	BatchSize   int            `json:"batch_size"`
	ProcessedAt time.Time      `json:"processed_at"`
}

// NewDomainBatch 创建新的域名批次
func NewDomainBatch(domains []*DomainQueue, batchSize int) *DomainBatch {
	return &DomainBatch{
		Domains:     domains,
		BatchSize:   batchSize,
		ProcessedAt: time.Now(),
	}
}

// ValidDomains 返回有效的域名列表
func (b *DomainBatch) ValidDomains() []*DomainQueue {
	valid := make([]*DomainQueue, 0, len(b.Domains))
	for _, domain := range b.Domains {
		if domain.IsProcessable() {
			valid = append(valid, domain)
		}
	}
	return valid
}

// Size 返回批次大小
func (b *DomainBatch) Size() int {
	return len(b.Domains)
}

// DomainStats 域名队列统计信息
type DomainStats struct {
	Status           DomainProcessingStatus `json:"status"`
	Count            int                    `json:"count"`
	AvgRetryCount    float64                `json:"avg_retry_count"`
	OldestCreated    time.Time              `json:"oldest_created"`
	NewestCreated    time.Time              `json:"newest_created"`
	TodayCount       int                    `json:"today_count"`
}

// DomainPerformanceStats 域名处理性能统计
type DomainPerformanceStats struct {
	ProcessDate           time.Time `json:"process_date"`
	TotalProcessed        int       `json:"total_processed"`
	CompletedCount        int       `json:"completed_count"`
	FailedCount           int       `json:"failed_count"`
	AvgProcessingTime     float64   `json:"avg_processing_time_seconds"`
	SuccessRate           float64   `json:"success_rate"`
}

// CalculateSuccessRate 计算成功率
func (s *DomainPerformanceStats) CalculateSuccessRate() {
	if s.TotalProcessed > 0 {
		s.SuccessRate = float64(s.CompletedCount) / float64(s.TotalProcessed) * 100
	}
}