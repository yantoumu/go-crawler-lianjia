package persist

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/zzayne/go-crawler/engine"
	"gopkg.in/olivere/elastic.v5"
)

// OptimizedSaver with bulk operations and better resource management
type OptimizedSaver struct {
	client        *elastic.Client
	bulkService   *elastic.BulkService
	index         string
	itemChan      chan engine.Item
	batchSize     int
	flushInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	metrics       *SaverMetrics
}

// SaverMetrics tracks persistence performance
type SaverMetrics struct {
	ItemsSaved   uint64
	BatchesSaved uint64
	Errors       uint64
	AvgBatchTime time.Duration
	mu           sync.RWMutex
}

// NewOptimizedSaver creates an optimized saver with bulk operations
func NewOptimizedSaver(index string, batchSize int) (*OptimizedSaver, error) {
	// Configure Elasticsearch client with connection pooling
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetHealthcheck(true),
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetMaxRetries(3),
		elastic.SetGzip(true),
		// Connection pool settings
		elastic.SetBasicAuth("", ""), // Add if needed
	)

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	saver := &OptimizedSaver{
		client:        client,
		bulkService:   client.Bulk(),
		index:         index,
		itemChan:      make(chan engine.Item, 1000), // Buffered channel
		batchSize:     batchSize,
		flushInterval: 5 * time.Second,
		ctx:           ctx,
		cancel:        cancel,
		metrics:       &SaverMetrics{},
	}

	// Ensure index exists with optimized settings
	if err := saver.ensureIndex(); err != nil {
		return nil, err
	}

	return saver, nil
}

// ensureIndex creates index with optimized settings if it doesn't exist
func (s *OptimizedSaver) ensureIndex() error {
	exists, err := s.client.IndexExists(s.index).Do(context.Background())
	if err != nil {
		return err
	}

	if !exists {
		// Create index with optimized settings
		createIndex, err := s.client.CreateIndex(s.index).
			BodyString(`{
				"settings": {
					"number_of_shards": 2,
					"number_of_replicas": 1,
					"refresh_interval": "30s",
					"index": {
						"translog": {
							"durability": "async",
							"sync_interval": "5s"
						}
					}
				}
			}`).
			Do(context.Background())

		if err != nil {
			return err
		}

		if !createIndex.Acknowledged {
			return errors.New("index creation not acknowledged")
		}

		log.Printf("Created index %s with optimized settings", s.index)
	}

	return nil
}

// Start begins the optimized saving process
func (s *OptimizedSaver) Start() chan engine.Item {
	// Start multiple bulk processors for parallel processing
	numProcessors := 2
	for i := 0; i < numProcessors; i++ {
		s.wg.Add(1)
		go s.bulkProcessor(i)
	}

	// Start metrics reporter
	go s.reportMetrics()

	return s.itemChan
}

// bulkProcessor handles batch saving
func (s *OptimizedSaver) bulkProcessor(id int) {
	defer s.wg.Done()

	batch := make([]engine.Item, 0, s.batchSize)
	timer := time.NewTimer(s.flushInterval)
	defer timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			// Flush remaining items before shutdown
			if len(batch) > 0 {
				s.saveBatch(batch)
			}
			return

		case item := <-s.itemChan:
			batch = append(batch, item)

			// Save batch if it reaches the size limit
			if len(batch) >= s.batchSize {
				s.saveBatch(batch)
				batch = batch[:0] // Reset batch
				timer.Reset(s.flushInterval)
			}

		case <-timer.C:
			// Periodic flush even if batch is not full
			if len(batch) > 0 {
				s.saveBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(s.flushInterval)
		}
	}
}

// saveBatch saves a batch of items using bulk API
func (s *OptimizedSaver) saveBatch(items []engine.Item) {
	start := time.Now()

	bulkRequest := s.client.Bulk()

	for _, item := range items {
		if item.Type == "" {
			log.Printf("Skipping item with empty type: %v", item)
			continue
		}

		req := elastic.NewBulkIndexRequest().
			Index(s.index).
			Type(item.Type).
			Doc(item)

		if item.ID != "" {
			req.Id(item.ID)
		}

		bulkRequest.Add(req)
	}

	// Execute bulk request with retry logic
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bulkResponse, err := s.executeWithRetry(bulkRequest, ctx, 3)
	if err != nil {
		log.Printf("Bulk save error: %v", err)
		s.incrementErrors()

		// Try to save items individually as fallback
		s.fallbackSave(items)
		return
	}

	// Check for individual failures
	if bulkResponse.Errors {
		for _, item := range bulkResponse.Failed() {
			log.Printf("Failed to index item %s: %v", item.Id, item.Error)
			s.incrementErrors()
		}
	}

	// Update metrics
	s.updateMetrics(len(items), time.Since(start))

	log.Printf("Bulk saved %d items in %v", len(items), time.Since(start))
}

// executeWithRetry performs bulk request with retry logic
func (s *OptimizedSaver) executeWithRetry(bulk *elastic.BulkService, ctx context.Context, maxRetries int) (*elastic.BulkResponse, error) {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			// Exponential backoff
			time.Sleep(time.Duration(1<<uint(i)) * time.Second)
		}

		response, err := bulk.Do(ctx)
		if err == nil {
			return response, nil
		}

		lastErr = err
		log.Printf("Bulk request attempt %d failed: %v", i+1, err)
	}

	return nil, lastErr
}

// fallbackSave attempts to save items individually when bulk fails
func (s *OptimizedSaver) fallbackSave(items []engine.Item) {
	log.Println("Attempting fallback save for failed batch")

	for _, item := range items {
		if item.Type == "" {
			continue
		}

		indexService := s.client.Index().
			Index(s.index).
			Type(item.Type).
			BodyJson(item)

		if item.ID != "" {
			indexService.Id(item.ID)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := indexService.Do(ctx)
		cancel()

		if err != nil {
			log.Printf("Fallback save failed for item %s: %v", item.ID, err)
			s.incrementErrors()
		}
	}
}

// Metrics methods

func (s *OptimizedSaver) updateMetrics(itemCount int, duration time.Duration) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	s.metrics.ItemsSaved += uint64(itemCount)
	s.metrics.BatchesSaved++

	// Calculate moving average
	if s.metrics.AvgBatchTime == 0 {
		s.metrics.AvgBatchTime = duration
	} else {
		s.metrics.AvgBatchTime = (s.metrics.AvgBatchTime + duration) / 2
	}
}

func (s *OptimizedSaver) incrementErrors() {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	s.metrics.Errors++
}

func (s *OptimizedSaver) reportMetrics() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.metrics.mu.RLock()
			log.Printf("Saver Metrics: Items=%d, Batches=%d, Errors=%d, AvgBatchTime=%v",
				s.metrics.ItemsSaved,
				s.metrics.BatchesSaved,
				s.metrics.Errors,
				s.metrics.AvgBatchTime)
			s.metrics.mu.RUnlock()
		}
	}
}

// Shutdown gracefully stops the saver
func (s *OptimizedSaver) Shutdown() {
	log.Println("Shutting down saver...")
	s.cancel()
	close(s.itemChan)
	s.wg.Wait()

	// Force refresh to make all documents searchable
	s.client.Refresh(s.index).Do(context.Background())

	log.Println("Saver shutdown complete")
}
