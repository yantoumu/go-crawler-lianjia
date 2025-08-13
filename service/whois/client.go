package whois

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/zzayne/go-crawler/model"
)

// API端点配置
var (
	DefaultAPIEndpoints = []string{
		"https://domain.seo9.org/query",
		"https://whois.seokey.vip/query",
		"https://whois-rdap-worker.gamesvchost.workers.dev/query",
	}
)

// WhoisAPI 接口定义
type WhoisAPI interface {
	Query(ctx context.Context, domain string) (*model.WhoisResponse, error)
	GetEndpoint() string
	IsHealthy() bool
	MarkUnhealthy()
	MarkHealthy()
}

// APIEndpoint API端点实现
type APIEndpoint struct {
	url           string
	client        *http.Client
	healthy       bool
	lastFailure   time.Time
	failureCount  int
	mu            sync.RWMutex
}

// NewAPIEndpoint 创建新的API端点
func NewAPIEndpoint(endpoint string) *APIEndpoint {
	return &APIEndpoint{
		url: endpoint,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 5,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		healthy: true,
	}
}

// GetEndpoint 获取端点URL
func (e *APIEndpoint) GetEndpoint() string {
	return e.url
}

// IsHealthy 检查是否健康
func (e *APIEndpoint) IsHealthy() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// 如果失败次数超过3次，且最后失败时间在5分钟内，认为不健康
	if e.failureCount >= 3 && time.Since(e.lastFailure) < 5*time.Minute {
		return false
	}
	
	return e.healthy
}

// MarkUnhealthy 标记为不健康
func (e *APIEndpoint) MarkUnhealthy() {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.healthy = false
	e.lastFailure = time.Now()
	e.failureCount++
	
	log.Printf("API endpoint %s marked unhealthy (failure count: %d)", e.url, e.failureCount)
}

// MarkHealthy 标记为健康
func (e *APIEndpoint) MarkHealthy() {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.healthy = true
	e.failureCount = 0
	
	log.Printf("API endpoint %s marked healthy", e.url)
}

// Query 查询域名信息
func (e *APIEndpoint) Query(ctx context.Context, domain string) (*model.WhoisResponse, error) {
	// 构造请求URL
	reqURL := fmt.Sprintf("%s?q=%s", e.url, url.QueryEscape(domain))
	
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	// 设置请求头
	req.Header.Set("User-Agent", "Go-Crawler-Whois/1.0")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	
	// 发送请求
	resp, err := e.client.Do(req)
	if err != nil {
		e.MarkUnhealthy()
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	
	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		e.MarkUnhealthy()
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
	}
	
	// 限制响应大小，防止DoS
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB限制
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	
	// 解析响应
	response, err := e.parseResponse(domain, body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	
	// 标记为健康
	e.MarkHealthy()
	
	response.APISource = e.url
	response.QueryTime = time.Now()
	
	return response, nil
}

// parseResponse 解析API响应
func (e *APIEndpoint) parseResponse(domain string, body []byte) (*model.WhoisResponse, error) {
	var rawData map[string]interface{}
	if err := json.Unmarshal(body, &rawData); err != nil {
		return nil, fmt.Errorf("invalid JSON response: %w", err)
	}
	
	response := &model.WhoisResponse{
		Domain:      domain,
		RawResponse: string(body),
	}
	
	// 不同API可能有不同的响应格式，尝试通用解析
	if available, ok := rawData["available"].(bool); ok {
		response.Available = available
		response.Registered = !available
	}
	
	// 尝试解析注册商信息
	if registrar, ok := rawData["registrar"].(string); ok {
		response.Registrar = registrar
	}
	
	// 尝试解析日期信息
	if createdStr, ok := rawData["created_date"].(string); ok {
		if createdTime, err := parseDate(createdStr); err == nil {
			response.CreatedDate = &createdTime
		}
	}
	
	if expiryStr, ok := rawData["expiry_date"].(string); ok {
		if expiryTime, err := parseDate(expiryStr); err == nil {
			response.ExpiryDate = &expiryTime
		}
	}
	
	if updatedStr, ok := rawData["updated_date"].(string); ok {
		if updatedTime, err := parseDate(updatedStr); err == nil {
			response.UpdatedDate = &updatedTime
		}
	}
	
	// 尝试解析名称服务器
	if nsData, ok := rawData["name_servers"]; ok {
		if nsArray, ok := nsData.([]interface{}); ok {
			nameServers := make([]string, 0, len(nsArray))
			for _, ns := range nsArray {
				if nsStr, ok := ns.(string); ok {
					nameServers = append(nameServers, nsStr)
				}
			}
			response.NameServers = nameServers
		}
	}
	
	// 尝试解析状态信息
	if statusData, ok := rawData["status"]; ok {
		if statusArray, ok := statusData.([]interface{}); ok {
			statuses := make([]string, 0, len(statusArray))
			for _, status := range statusArray {
				if statusStr, ok := status.(string); ok {
					statuses = append(statuses, statusStr)
				}
			}
			response.Status = statuses
		}
	}
	
	// 检查是否有错误信息
	if errMsg, ok := rawData["error"].(string); ok {
		response.Error = errMsg
	}
	
	return response, nil
}

// parseDate 解析各种日期格式
func parseDate(dateStr string) (time.Time, error) {
	// 常见的日期格式
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"02-Jan-2006",
		"2006/01/02",
	}
	
	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t, nil
		}
	}
	
	return time.Time{}, fmt.Errorf("unable to parse date: %s", dateStr)
}

// WhoisClient WHOIS查询客户端
type WhoisClient struct {
	endpoints     []WhoisAPI
	currentIndex  int
	rateLimit     time.Duration
	lastRequest   time.Time
	mu            sync.Mutex
}

// NewWhoisClient 创建新的WHOIS客户端
func NewWhoisClient(endpoints []string, rateLimit time.Duration) *WhoisClient {
	if len(endpoints) == 0 {
		endpoints = DefaultAPIEndpoints
	}
	
	apis := make([]WhoisAPI, len(endpoints))
	for i, endpoint := range endpoints {
		apis[i] = NewAPIEndpoint(endpoint)
	}
	
	return &WhoisClient{
		endpoints:   apis,
		rateLimit:   rateLimit,
		lastRequest: time.Now().Add(-rateLimit), // 允许立即开始
	}
}

// Query 查询单个域名
func (c *WhoisClient) Query(ctx context.Context, domain string) (*model.WhoisResponse, error) {
	// 应用速率限制
	c.mu.Lock()
	elapsed := time.Since(c.lastRequest)
	if elapsed < c.rateLimit {
		sleepTime := c.rateLimit - elapsed
		c.mu.Unlock()
		
		log.Printf("Rate limiting: sleeping for %v", sleepTime)
		
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleepTime):
		}
		
		c.mu.Lock()
	}
	c.lastRequest = time.Now()
	c.mu.Unlock()
	
	// 清理域名输入
	domain = strings.TrimSpace(strings.ToLower(domain))
	if domain == "" {
		return nil, errors.New("empty domain")
	}
	
	// 尝试所有可用的API端点
	var lastError error
	attempted := 0
	maxAttempts := len(c.endpoints)
	
	for attempted < maxAttempts {
		// 获取下一个健康的端点
		api := c.getNextHealthyAPI()
		if api == nil {
			// 所有端点都不健康，等待一段时间再重试
			if attempted == 0 {
				log.Printf("All API endpoints are unhealthy, waiting 10 seconds...")
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Second):
				}
				
				// 重置所有端点的健康状态
				c.resetHealthStatus()
				continue
			}
			break
		}
		
		log.Printf("Querying domain %s using endpoint %s (attempt %d/%d)", 
			domain, api.GetEndpoint(), attempted+1, maxAttempts)
		
		// 发送查询请求
		response, err := api.Query(ctx, domain)
		if err != nil {
			lastError = err
			log.Printf("Query failed for endpoint %s: %v", api.GetEndpoint(), err)
			attempted++
			continue
		}
		
		// 验证响应
		if !response.IsValid() {
			lastError = fmt.Errorf("invalid response from %s: %s", api.GetEndpoint(), response.Error)
			attempted++
			continue
		}
		
		log.Printf("Successfully queried domain %s using %s", domain, api.GetEndpoint())
		return response, nil
	}
	
	// 所有端点都失败了
	if lastError == nil {
		lastError = errors.New("no healthy API endpoints available")
	}
	
	return nil, fmt.Errorf("all API endpoints failed, last error: %w", lastError)
}

// getNextHealthyAPI 获取下一个健康的API端点（轮询）
func (c *WhoisClient) getNextHealthyAPI() WhoisAPI {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	startIndex := c.currentIndex
	
	for {
		api := c.endpoints[c.currentIndex]
		c.currentIndex = (c.currentIndex + 1) % len(c.endpoints)
		
		if api.IsHealthy() {
			return api
		}
		
		// 如果所有端点都检查过了，返回nil
		if c.currentIndex == startIndex {
			return nil
		}
	}
}

// resetHealthStatus 重置所有端点的健康状态
func (c *WhoisClient) resetHealthStatus() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	log.Printf("Resetting health status for all API endpoints")
	for _, api := range c.endpoints {
		api.MarkHealthy()
	}
}

// GetHealthyEndpoints 获取健康的端点数量
func (c *WhoisClient) GetHealthyEndpoints() int {
	count := 0
	for _, api := range c.endpoints {
		if api.IsHealthy() {
			count++
		}
	}
	return count
}

// GetEndpointStats 获取端点统计信息
func (c *WhoisClient) GetEndpointStats() map[string]bool {
	stats := make(map[string]bool)
	for _, api := range c.endpoints {
		stats[api.GetEndpoint()] = api.IsHealthy()
	}
	return stats
}