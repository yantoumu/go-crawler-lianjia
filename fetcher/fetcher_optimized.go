package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/time/rate"
)

// OptimizedFetcher with connection pooling and adaptive rate limiting
type OptimizedFetcher struct {
	client  *http.Client
	limiter *rate.Limiter
	cache   *ResponseCache
	metrics *FetchMetrics
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
}

// ResponseCache for reducing redundant fetches
type ResponseCache struct {
	cache map[string]*CacheEntry
	mu    sync.RWMutex
	ttl   time.Duration
}

// CacheEntry stores cached response data
type CacheEntry struct {
	Content   []byte
	Timestamp time.Time
}

// FetchMetrics tracks performance metrics
type FetchMetrics struct {
	TotalRequests uint64
	CacheHits     uint64
	NetworkErrors uint64
	ResponseTimes []time.Duration
	mu            sync.RWMutex
}

// Global optimized fetcher instance
var (
	globalFetcher *OptimizedFetcher
	once          sync.Once
)

// InitOptimizedFetcher initializes the global fetcher with optimizations
func InitOptimizedFetcher() *OptimizedFetcher {
	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		globalFetcher = &OptimizedFetcher{
			client: &http.Client{
				Transport: &http.Transport{
					// Connection pooling configuration
					MaxIdleConns:        100,
					MaxIdleConnsPerHost: 10,
					MaxConnsPerHost:     10,
					IdleConnTimeout:     90 * time.Second,
					DisableCompression:  false,
					DisableKeepAlives:   false,

					// Optimize dial settings
					DialContext: (&net.Dialer{
						Timeout:   10 * time.Second,
						KeepAlive: 30 * time.Second,
						DualStack: true,
					}).DialContext,

					// TLS handshake timeout
					TLSHandshakeTimeout: 10 * time.Second,

					// Response header timeout
					ResponseHeaderTimeout: 10 * time.Second,
				},
				Timeout: 30 * time.Second,
			},
			// Adaptive rate limiting - starts at 5 requests per second
			limiter: rate.NewLimiter(rate.Every(200*time.Millisecond), 10), // 5 RPS with burst of 10
			cache: &ResponseCache{
				cache: make(map[string]*CacheEntry),
				ttl:   5 * time.Minute,
			},
			metrics: &FetchMetrics{
				ResponseTimes: make([]time.Duration, 0, 1000),
			},
			ctx:    ctx,
			cancel: cancel,
		}

		// Start cache cleanup routine
		go globalFetcher.cache.cleanup(ctx)

		// Start adaptive rate limiting adjuster
		go globalFetcher.adjustRateLimit()
	})

	return globalFetcher
}

// Shutdown gracefully shuts down the optimized fetcher
func (f *OptimizedFetcher) Shutdown() {
	if f.cancel != nil {
		f.cancel()
	}
}

// FetchOptimized fetches URL with optimizations
func FetchOptimized(ctx context.Context, url string) (*goquery.Document, error) {
	fetcher := InitOptimizedFetcher()

	// Check cache first
	if doc, hit := fetcher.cache.get(url); hit {
		fetcher.recordCacheHit()
		return doc, nil
	}

	// Rate limiting with context support
	if err := fetcher.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter error: %w", err)
	}

	// Record start time for metrics
	start := time.Now()

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	// Set headers for better compatibility
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("DNT", "1")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	// Execute request with retry logic
	resp, err := fetcher.executeWithRetry(req, 3)
	if err != nil {
		fetcher.recordNetworkError()
		return nil, err
	}
	defer resp.Body.Close()

	// Record response time
	fetcher.recordResponseTime(time.Since(start))

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status code error: %d %s", resp.StatusCode, resp.Status)
	}

	// Read response body with size limit
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB limit
	if err != nil {
		return nil, err
	}

	// Parse document
	doc, err := goquery.NewDocumentFromReader(io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return nil, err
	}

	// Cache the response
	fetcher.cache.set(url, body)

	return doc, nil
}

// executeWithRetry performs request with exponential backoff retry
func (f *OptimizedFetcher) executeWithRetry(req *http.Request, maxRetries int) (*http.Response, error) {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			// Exponential backoff: 1s, 2s, 4s...
			backoff := time.Duration(1<<uint(i-1)) * time.Second
			time.Sleep(backoff)
		}

		resp, err := f.client.Do(req)
		if err == nil && resp.StatusCode < 500 {
			// Success or client error (no retry needed)
			return resp, nil
		}

		if resp != nil {
			resp.Body.Close()
		}

		lastErr = err
		if err == nil {
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
		}
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// adjustRateLimit dynamically adjusts rate limiting based on performance
func (f *OptimizedFetcher) adjustRateLimit() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			f.metrics.mu.RLock()
			errorRate := float64(f.metrics.NetworkErrors) / float64(f.metrics.TotalRequests+1)
			avgResponseTime := f.calculateAvgResponseTime()
			f.metrics.mu.RUnlock()

			// Adjust rate limit based on error rate and response time
			if errorRate > 0.1 || avgResponseTime > 5*time.Second {
				// Slow down if errors are high or responses are slow
				f.limiter.SetLimit(rate.Every(500 * time.Millisecond))
				log.Println("Rate limit decreased due to high error rate or slow responses")
			} else if errorRate < 0.01 && avgResponseTime < 1*time.Second {
				// Speed up if everything is running smoothly
				f.limiter.SetLimit(rate.Every(100 * time.Millisecond))
				log.Println("Rate limit increased due to good performance")
			}
		}
	}
}

// ResponseCache methods

func (c *ResponseCache) get(url string) (*goquery.Document, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.cache[url]
	if !exists || time.Since(entry.Timestamp) > c.ttl {
		return nil, false
	}

	doc, err := goquery.NewDocumentFromReader(io.NopCloser(bytes.NewReader(entry.Content)))
	if err != nil {
		return nil, false
	}

	return doc, true
}

func (c *ResponseCache) set(url string, content []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[url] = &CacheEntry{
		Content:   content,
		Timestamp: time.Now(),
	}
}

func (c *ResponseCache) cleanup(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for url, entry := range c.cache {
				if now.Sub(entry.Timestamp) > c.ttl {
					delete(c.cache, url)
				}
			}
			c.mu.Unlock()
		}
	}
}

// Metrics methods

func (f *OptimizedFetcher) recordResponseTime(duration time.Duration) {
	f.metrics.mu.Lock()
	defer f.metrics.mu.Unlock()

	f.metrics.TotalRequests++
	f.metrics.ResponseTimes = append(f.metrics.ResponseTimes, duration)

	// Keep only last 1000 response times
	if len(f.metrics.ResponseTimes) > 1000 {
		f.metrics.ResponseTimes = f.metrics.ResponseTimes[len(f.metrics.ResponseTimes)-1000:]
	}
}

func (f *OptimizedFetcher) recordCacheHit() {
	f.metrics.mu.Lock()
	defer f.metrics.mu.Unlock()
	f.metrics.CacheHits++
}

func (f *OptimizedFetcher) recordNetworkError() {
	f.metrics.mu.Lock()
	defer f.metrics.mu.Unlock()
	f.metrics.NetworkErrors++
}

func (f *OptimizedFetcher) calculateAvgResponseTime() time.Duration {
	if len(f.metrics.ResponseTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, t := range f.metrics.ResponseTimes {
		total += t
	}

	return total / time.Duration(len(f.metrics.ResponseTimes))
}
