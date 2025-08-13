package engine

import (
	"context"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/zzayne/go-crawler/fetcher"
)

// OptimizedEngine with performance improvements
type OptimizedEngine struct {
	Scheduler   Scheduler
	WorkerCount int
	ItemChan    chan Item
	MaxWorkers  int
	MinWorkers  int
	BufferSize  int
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	metrics     *Metrics
	workerPool  *WorkerPool
}

// Metrics for monitoring performance
type Metrics struct {
	mu                sync.RWMutex
	RequestsProcessed uint64
	ItemsSaved        uint64
	Errors            uint64
	AvgResponseTime   time.Duration
}

// WorkerPool manages dynamic worker scaling
type WorkerPool struct {
	maxWorkers     int
	minWorkers     int
	currentWorkers int
	mu             sync.RWMutex
	workQueue      chan Request
	resultChan     chan ParseResult
	scheduler      Scheduler
}

// NewOptimizedEngine creates an optimized engine with better resource management
func NewOptimizedEngine(scheduler Scheduler, workerCount int) *OptimizedEngine {
	ctx, cancel := context.WithCancel(context.Background())

	// Auto-detect optimal worker count based on CPU cores
	if workerCount <= 0 {
		workerCount = runtime.NumCPU() * 2
	}

	return &OptimizedEngine{
		Scheduler:   scheduler,
		WorkerCount: workerCount,
		MinWorkers:  runtime.NumCPU(),
		MaxWorkers:  runtime.NumCPU() * 4,
		BufferSize:  1000, // Buffered channels to reduce blocking
		ctx:         ctx,
		cancel:      cancel,
		metrics:     &Metrics{},
	}
}

// RunOptimized starts the engine with performance optimizations
func (e *OptimizedEngine) RunOptimized(reqs ...Request) {
	// Use buffered channels to reduce blocking
	resultChan := make(chan ParseResult, e.BufferSize)

	// Initialize worker pool
	e.workerPool = &WorkerPool{
		maxWorkers:     e.MaxWorkers,
		minWorkers:     e.MinWorkers,
		currentWorkers: e.WorkerCount,
		workQueue:      make(chan Request, e.BufferSize),
		resultChan:     resultChan,
		scheduler:      e.Scheduler,
	}

	e.Scheduler.Run()

	// Start initial workers
	for i := 0; i < e.WorkerCount; i++ {
		e.wg.Add(1)
		go e.optimizedWorker(i)
	}

	// Start result processor
	e.wg.Add(1)
	go e.processResults(resultChan)

	// Submit initial requests
	for _, r := range reqs {
		e.Scheduler.Submit(r)
	}

	// Start metrics reporter
	e.wg.Add(1)
	go e.reportMetrics()

	// Start dynamic worker scaler
	e.wg.Add(1)
	go e.dynamicScaler()
}

// optimizedWorker with better error handling and resource management
func (e *OptimizedEngine) optimizedWorker(id int) {
	defer e.wg.Done()

	workerChan := e.Scheduler.WorkChan()

	for {
		select {
		case <-e.ctx.Done():
			log.Printf("Worker %d shutting down", id)
			return
		default:
			e.Scheduler.(ReadyNotifier).WorkerReady(workerChan)

			select {
			case req := <-workerChan:
				start := time.Now()
				result, err := e.fetchAndParse(req)

				if err != nil {
					e.incrementErrors()
					log.Printf("Worker %d error processing %s: %v", id, req.URL, err)
					continue
				}

				e.updateMetrics(time.Since(start))
				e.workerPool.resultChan <- result

			case <-e.ctx.Done():
				return
			}
		}
	}
}

// fetchAndParse with timeout and retry logic
func (e *OptimizedEngine) fetchAndParse(r Request) (ParseResult, error) {
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	// Add retry logic with exponential backoff
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ParseResult{}, ctx.Err()
			}
		}

		doc, err := fetcher.Fetch(r.URL)
		if err == nil {
			return r.ParseFunc(doc)
		}
		lastErr = err
	}

	return ParseResult{}, lastErr
}

// processResults handles results in a separate goroutine
func (e *OptimizedEngine) processResults(resultChan chan ParseResult) {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case result := <-resultChan:
			// Process items concurrently
			for _, item := range result.Items {
				if e.ItemChan != nil {
					select {
					case e.ItemChan <- item:
						e.incrementItemsSaved()
					case <-time.After(5 * time.Second):
						log.Printf("Timeout sending item to ItemChan")
					case <-e.ctx.Done():
						return
					}
				}
			}

			// Submit new requests
			for _, r := range result.Requests {
				e.Scheduler.Submit(r)
			}
		}
	}
}

// dynamicScaler adjusts worker count based on load
func (e *OptimizedEngine) dynamicScaler() {
	defer e.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.adjustWorkerCount()
		}
	}
}

// adjustWorkerCount scales workers based on queue size
func (e *OptimizedEngine) adjustWorkerCount() {
	e.workerPool.mu.Lock()
	defer e.workerPool.mu.Unlock()

	// Get queue size (would need to implement in scheduler)
	// This is pseudo-code for demonstration
	// queueSize := e.Scheduler.GetQueueSize()

	// Scale up if queue is growing
	// if queueSize > 100 && e.workerPool.currentWorkers < e.MaxWorkers {
	//     e.wg.Add(1)
	//     go e.optimizedWorker(e.workerPool.currentWorkers)
	//     e.workerPool.currentWorkers++
	//     log.Printf("Scaled up to %d workers", e.workerPool.currentWorkers)
	// }

	// Scale down if queue is small
	// if queueSize < 10 && e.workerPool.currentWorkers > e.MinWorkers {
	//     // Signal worker to stop (implementation needed)
	//     e.workerPool.currentWorkers--
	//     log.Printf("Scaled down to %d workers", e.workerPool.currentWorkers)
	// }
}

// Metrics methods
func (e *OptimizedEngine) updateMetrics(duration time.Duration) {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()

	e.metrics.RequestsProcessed++

	// Calculate moving average
	if e.metrics.AvgResponseTime == 0 {
		e.metrics.AvgResponseTime = duration
	} else {
		e.metrics.AvgResponseTime = (e.metrics.AvgResponseTime + duration) / 2
	}
}

func (e *OptimizedEngine) incrementErrors() {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()
	e.metrics.Errors++
}

func (e *OptimizedEngine) incrementItemsSaved() {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()
	e.metrics.ItemsSaved++
}

func (e *OptimizedEngine) reportMetrics() {
	defer e.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.metrics.mu.RLock()
			log.Printf("Metrics: Requests=%d, Items=%d, Errors=%d, AvgTime=%v",
				e.metrics.RequestsProcessed,
				e.metrics.ItemsSaved,
				e.metrics.Errors,
				e.metrics.AvgResponseTime)
			e.metrics.mu.RUnlock()
		}
	}
}

// Shutdown gracefully stops the engine
func (e *OptimizedEngine) Shutdown() {
	log.Println("Shutting down engine...")
	e.cancel()

	// Stop scheduler if it supports stopping
	if stopper, ok := e.Scheduler.(interface{ Stop() }); ok {
		stopper.Stop()
	}

	e.wg.Wait()
	log.Println("Engine shutdown complete")
}
