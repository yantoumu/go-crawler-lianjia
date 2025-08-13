package fetcher

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var (
	rateLimiter *time.Ticker
	rateMutex   sync.Mutex
	rateOnce    sync.Once
)

// initRateLimiter initializes the rate limiter safely
func initRateLimiter() {
	rateOnce.Do(func() {
		rateLimiter = time.NewTicker(200 * time.Millisecond)
	})
}

// waitForRateLimit waits for rate limiting with context support
func waitForRateLimit(ctx context.Context) error {
	initRateLimiter()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rateLimiter.C:
		return nil
	}
}

// Fetch 获取对应url的内容，输出文档供解析器解析
func Fetch(url string) (doc *goquery.Document, err error) {
	return FetchWithContext(context.Background(), url)
}

// FetchWithContext 获取对应url的内容，输出文档供解析器解析，支持context
func FetchWithContext(ctx context.Context, url string) (doc *goquery.Document, err error) {
	//设定请求间隔，简单的应对网站反爬虫措施
	if err := waitForRateLimit(ctx); err != nil {
		return nil, fmt.Errorf("rate limit error: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	//设置请求header头，简单的应对网站反爬虫措施
	request.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36")
	client := http.Client{
		Timeout: 30 * time.Second, // Add 30 second timeout
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			//fmt.Println("Redirect:", req)
			return nil
		},
	}

	res, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	// Load the HTML document
	doc, err = goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML document: %w", err)
	}

	return doc, nil
}

// Cleanup stops the rate limiter ticker
func Cleanup() {
	rateMutex.Lock()
	defer rateMutex.Unlock()
	if rateLimiter != nil {
		rateLimiter.Stop()
		rateLimiter = nil
		rateOnce = sync.Once{} // Reset once for potential re-initialization
	}
}
