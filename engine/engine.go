package engine

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/zzayne/go-crawler/fetcher"
)

// Engine 初始解析入口
type Engine struct {
	Scheduler   Scheduler
	WorkerCount int
	ItemChan    chan Item
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// Scheduler ...
type Scheduler interface {
	ReadyNotifier
	WorkChan() chan Request
	Run()
	Submit(Request)
}

// ReadyNotifier ...
type ReadyNotifier interface {
	WorkerReady(chan Request)
}

// Run 启动解析引擎
func (e *Engine) Run(reqs ...Request) {
	// Setup context for graceful shutdown
	e.ctx, e.cancel = context.WithCancel(context.Background())

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal, stopping crawler...")
		e.cancel()
		// Stop scheduler if it supports stopping
		if stopper, ok := e.Scheduler.(interface{ Stop() }); ok {
			stopper.Stop()
		}
	}()

	resultChan := make(chan ParseResult, e.WorkerCount)

	e.Scheduler.Run()
	for i := 0; i < e.WorkerCount; i++ {
		e.createWorker(e.Scheduler.WorkChan(), resultChan, e.Scheduler)
	}

	for _, r := range reqs {
		e.Scheduler.Submit(r)
	}

	for {
		select {
		case <-e.ctx.Done():
			log.Println("Shutting down engine...")
			e.wg.Wait()
			close(e.ItemChan)
			return
		case result := <-resultChan:
			for _, item := range result.Items {
				select {
				case e.ItemChan <- item:
				case <-e.ctx.Done():
					return
				}
			}

			for _, r := range result.Requests {
				e.Scheduler.Submit(r)
			}
		}
	}
}

func worker(r Request) (ParseResult, error) {

	doc, err := fetcher.Fetch(r.URL)
	if err != nil {
		log.Printf(" fetch url error:%s\n", r.URL)
		return ParseResult{}, err
	}
	return r.ParseFunc(doc)
}

func (e *Engine) createWorker(in chan Request, out chan ParseResult, noticer ReadyNotifier) {
	e.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Worker panic recovered: %v", r)
			}
			e.wg.Done()
		}()
		for {
			select {
			case <-e.ctx.Done():
				return
			default:
				noticer.WorkerReady(in)
				select {
				case <-e.ctx.Done():
					return
				case req := <-in:
					result, err := worker(req)
					if err != nil {
						continue
					}
					select {
					case out <- result:
					case <-e.ctx.Done():
						return
					}
				}
			}
		}
	}()
}
