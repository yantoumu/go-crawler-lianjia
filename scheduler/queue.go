package scheduler

import (
	"context"
	"github.com/zzayne/go-crawler/engine"
)

// QueueScheduler 为每个worker单独分配一个chanel，由调度器来按照队列的方式，对worker用到的chanel和request进行对应分配
type QueueScheduler struct {
	requestChan chan engine.Request
	workerChan  chan chan engine.Request
	ctx         context.Context
	cancel      context.CancelFunc
}

// WorkerReady ...
func (s *QueueScheduler) WorkerReady(w chan engine.Request) {
	if s.workerChan != nil {
		select {
		case s.workerChan <- w:
		case <-s.ctx.Done():
			return
		}
	}
}

// Submit ...
func (s *QueueScheduler) Submit(r engine.Request) {
	if s.requestChan != nil {
		select {
		case s.requestChan <- r:
		case <-s.ctx.Done():
			return
		}
	}
}

// WorkChan returns a new channel for each worker
func (s *QueueScheduler) WorkChan() chan engine.Request {
	return make(chan engine.Request)
}

// Run ...
func (s *QueueScheduler) Run() {
	// Initialize context for graceful shutdown
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Use buffered channels to prevent blocking
	s.requestChan = make(chan engine.Request, 100)
	s.workerChan = make(chan chan engine.Request, 10)

	go func() {
		defer func() {
			if s.requestChan != nil {
				close(s.requestChan)
			}
			if s.workerChan != nil {
				close(s.workerChan)
			}
		}()

		var requestQ []engine.Request
		var workerQ []chan engine.Request
		for {
			var activeR engine.Request
			var activeW chan engine.Request

			if len(requestQ) > 0 && len(workerQ) > 0 {
				activeR = requestQ[0]
				activeW = workerQ[0]
			}
			select {
			case <-s.ctx.Done():
				return
			case r := <-s.requestChan:
				requestQ = append(requestQ, r)
			case w := <-s.workerChan:
				workerQ = append(workerQ, w)
			case activeW <- activeR:
				requestQ = requestQ[1:]
				workerQ = workerQ[1:]
			}
		}
	}()
}

// Stop gracefully stops the scheduler
func (s *QueueScheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}
