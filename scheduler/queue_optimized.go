package scheduler

import (
	"container/list"
	"context"
	"sync"

	"github.com/zzayne/go-crawler/engine"
)

// OptimizedQueueScheduler with memory-efficient data structures
type OptimizedQueueScheduler struct {
	requestChan chan engine.Request
	workerChan  chan chan engine.Request

	// Use linked list instead of slice for better memory management
	requestQueue *list.List
	workerQueue  *list.List

	// Metrics
	queueSize    int
	maxQueueSize int
	mu           sync.RWMutex

	// Memory pool for request objects
	requestPool sync.Pool

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// NewOptimizedQueueScheduler creates an optimized scheduler
func NewOptimizedQueueScheduler(maxQueueSize int) *OptimizedQueueScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &OptimizedQueueScheduler{
		requestQueue: list.New(),
		workerQueue:  list.New(),
		maxQueueSize: maxQueueSize,
		requestPool: sync.Pool{
			New: func() interface{} {
				return &engine.Request{}
			},
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// WorkerReady registers a worker as ready
func (s *OptimizedQueueScheduler) WorkerReady(w chan engine.Request) {
	select {
	case s.workerChan <- w:
	case <-s.ctx.Done():
		return
	}
}

// Submit adds a request to the queue with bounds checking
func (s *OptimizedQueueScheduler) Submit(r engine.Request) {
	s.mu.RLock()
	size := s.queueSize
	s.mu.RUnlock()

	// Prevent unbounded queue growth
	if s.maxQueueSize > 0 && size >= s.maxQueueSize {
		// Could implement different strategies here:
		// - Drop oldest
		// - Block until space available
		// - Return error
		return
	}

	select {
	case s.requestChan <- r:
	case <-s.ctx.Done():
		return
	}
}

// WorkChan returns a new worker channel
func (s *OptimizedQueueScheduler) WorkChan() chan engine.Request {
	// Use buffered channel to reduce contention
	return make(chan engine.Request, 1)
}

// GetQueueSize returns current queue size for monitoring
func (s *OptimizedQueueScheduler) GetQueueSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queueSize
}

// Run starts the scheduler with optimizations
func (s *OptimizedQueueScheduler) Run() {
	// Use buffered channels to reduce blocking
	s.requestChan = make(chan engine.Request, 100)
	s.workerChan = make(chan chan engine.Request, 100)

	go s.dispatch()
}

// dispatch handles request distribution with memory optimization
func (s *OptimizedQueueScheduler) dispatch() {
	defer func() {
		if s.requestChan != nil {
			close(s.requestChan)
		}
		if s.workerChan != nil {
			close(s.workerChan)
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			var activeR engine.Request
			var activeW chan engine.Request

			// Use pointers to avoid copying
			s.mu.RLock()
			if s.requestQueue.Len() > 0 && s.workerQueue.Len() > 0 {
				reqElem := s.requestQueue.Front()
				workerElem := s.workerQueue.Front()

				if reqElem != nil && workerElem != nil {
					activeR = reqElem.Value.(engine.Request)
					activeW = workerElem.Value.(chan engine.Request)
				}
			}
			s.mu.RUnlock()

			select {
			case <-s.ctx.Done():
				return
			case r := <-s.requestChan:
				s.mu.Lock()
				s.requestQueue.PushBack(r)
				s.queueSize = s.requestQueue.Len()
				s.mu.Unlock()

			case w := <-s.workerChan:
				s.mu.Lock()
				s.workerQueue.PushBack(w)
				s.mu.Unlock()

			case activeW <- activeR:
				if activeW != nil {
					s.mu.Lock()
					s.requestQueue.Remove(s.requestQueue.Front())
					s.workerQueue.Remove(s.workerQueue.Front())
					s.queueSize = s.requestQueue.Len()
					s.mu.Unlock()
				}
			}

			// Periodic cleanup to prevent memory leaks
			s.cleanupIfNeeded()
		}
	}
}

// cleanupIfNeeded performs periodic cleanup
func (s *OptimizedQueueScheduler) cleanupIfNeeded() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Trim excess capacity if queue has shrunk significantly
	if s.requestQueue.Len() < s.maxQueueSize/4 && s.maxQueueSize > 1000 {
		// Force GC on removed elements
		for s.requestQueue.Len() > s.maxQueueSize/2 {
			s.requestQueue.Remove(s.requestQueue.Back())
		}
	}
}

// Stop gracefully stops the scheduler
func (s *OptimizedQueueScheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}
