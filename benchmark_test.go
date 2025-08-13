package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/zzayne/go-crawler/engine"
	"github.com/zzayne/go-crawler/fetcher"
	parser "github.com/zzayne/go-crawler/parser/lianjia"
	"github.com/zzayne/go-crawler/scheduler"
)

// Mock HTML response for testing
const mockHTML = `
<!DOCTYPE html>
<html>
<head><title>Test</title></head>
<body>
	<div class="house-lst">
		<li><h2><a href="https://sz.lianjia.com/zufang/123.html">House 1</a></h2></li>
		<li><h2><a href="https://sz.lianjia.com/zufang/456.html">House 2</a></h2></li>
	</div>
	<div class="content-wrapper">
		<div class="title-wrapper">
			<div class="title">
				<main>Test House</main>
				<sub>Great Location</sub>
			</div>
		</div>
		<div class="overview">
			<div class="price">
				<span class="total">5000</span>
				<div class="unit"><span>元/月</span></div>
			</div>
		</div>
	</div>
	<div class="zf-room">
		<p>100平米</p>
		<p>3室2厅</p>
		<p>高楼层</p>
		<p>朝南</p>
		<p>近地铁</p>
		<p><a href="/xiaoqu/123">测试小区</a></p>
		<p><a href="/region/1">宝安区</a><a href="/location/2">西乡</a></p>
		<p>2024-01-01</p>
		<p>123456789</p>
	</div>
	<div class="houseRecord">
		<span class="houseNum">链家编号：123456789</span>
	</div>
</body>
</html>
`

// BenchmarkOriginalParser tests the original parser performance
func BenchmarkOriginalParser(b *testing.B) {
	doc, _ := goquery.NewDocumentFromReader(strings.NewReader(mockHTML))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser.RentParser(doc)
	}
}

// BenchmarkOptimizedParser tests the optimized parser performance
func BenchmarkOptimizedParser(b *testing.B) {
	doc, _ := goquery.NewDocumentFromReader(strings.NewReader(mockHTML))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser.OptimizedRentParser(doc)
	}
}

// BenchmarkOriginalScheduler tests the original scheduler
func BenchmarkOriginalScheduler(b *testing.B) {
	s := &scheduler.QueueScheduler{}
	s.Run()

	workerChan := s.WorkChan()
	go func() {
		for {
			s.WorkerReady(workerChan)
			<-workerChan
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Submit(engine.Request{
			URL: fmt.Sprintf("https://example.com/%d", i),
		})
	}
}

// BenchmarkOptimizedScheduler tests the optimized scheduler
func BenchmarkOptimizedScheduler(b *testing.B) {
	s := scheduler.NewOptimizedQueueScheduler(10000)
	s.Run()

	workerChan := s.WorkChan()
	go func() {
		for {
			s.WorkerReady(workerChan)
			<-workerChan
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Submit(engine.Request{
			URL: fmt.Sprintf("https://example.com/%d", i),
		})
	}
}

// BenchmarkConcurrentFetching tests concurrent fetching performance
func BenchmarkConcurrentFetching(b *testing.B) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockHTML))
	}))
	defer server.Close()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			fetcher.FetchOptimized(ctx, server.URL)
			cancel()
		}
	})
}

// TestMemoryAllocation tests memory allocation patterns
func TestMemoryAllocation(t *testing.T) {
	doc, _ := goquery.NewDocumentFromReader(strings.NewReader(mockHTML))

	// Test original parser allocations
	origAllocs := testing.AllocsPerRun(100, func() {
		parser.RentParser(doc)
	})

	// Test optimized parser allocations
	optAllocs := testing.AllocsPerRun(100, func() {
		parser.OptimizedRentParser(doc)
	})

	t.Logf("Original Parser Allocations: %.0f", origAllocs)
	t.Logf("Optimized Parser Allocations: %.0f", optAllocs)

	if optAllocs < origAllocs {
		improvement := (1 - optAllocs/origAllocs) * 100
		t.Logf("Memory allocation improved by %.2f%%", improvement)
	}
}

// TestGoroutineLeaks checks for goroutine leaks
func TestGoroutineLeaks(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	// Run a short crawl
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	e := engine.NewOptimizedEngine(scheduler.NewOptimizedQueueScheduler(100), 5)
	go e.RunOptimized(engine.Request{
		URL: "https://example.com",
		ParseFunc: func(doc *goquery.Document) (engine.ParseResult, error) {
			return engine.ParseResult{}, nil
		},
	})

	<-ctx.Done()
	e.Shutdown()

	// Wait for goroutines to finish
	time.Sleep(2 * time.Second)

	finalGoroutines := runtime.NumGoroutine()
	leaked := finalGoroutines - initialGoroutines

	if leaked > 0 {
		t.Errorf("Goroutine leak detected: %d goroutines leaked", leaked)
	} else {
		t.Logf("No goroutine leaks detected (Initial: %d, Final: %d)", initialGoroutines, finalGoroutines)
	}
}

// BenchmarkEndToEnd tests end-to-end performance
func BenchmarkEndToEnd(b *testing.B) {
	// Create a test server with rate limiting simulation
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Simulate network latency
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockHTML))
	}))
	defer server.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		// Initialize components
		scheduler := scheduler.NewOptimizedQueueScheduler(1000)
		e := engine.NewOptimizedEngine(scheduler, 10)

		// Create test requests
		requests := make([]engine.Request, 10)
		for j := 0; j < 10; j++ {
			requests[j] = engine.Request{
				URL:       server.URL,
				ParseFunc: parser.OptimizedRentListParser,
			}
		}

		// Run crawler
		go e.RunOptimized(requests...)

		// Let it run for a bit
		time.Sleep(100 * time.Millisecond)

		e.Shutdown()
		cancel()
	}

	b.Logf("Total requests processed: %d", requestCount)
}
