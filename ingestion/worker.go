package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/SalehMWS/fleet-tracker/models"
)

type WorkerPool struct {
	jobs      chan models.GPSPayload
	wg        sync.WaitGroup
	batchSize int
}

func NewWorkderPool(numWorkders, bufferSize, batchSize int) *WorkderPool {
	pool := &WorkderPool{
		jobs:      make(chan models.GPSPayload, bufferSize),
		batchSize: batchSize,
	}

	for i := 0; i < numWorkers; i++ {
		pool.wg.Add(1)
		go pool.workder(i)
	}

	return pool
}

func (p *WorkderPool) worker(id int) {
	defer p.wg.Done()

	batch := make([]models.GPSPayload, 0, p.batchSize)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				if len(batch) > 0 {
					p.flush(id, batch)
				}

				return
			}

			batch = append(batch, job)
			if len(batch) >= p.batchSize {
				p.flush(id, batch)

				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				p.flush(id, batch)
				batch = batch[:0]
			}
		}
	}
}

func (p *WorkderPool) flush(workerID int, batch []models.GPSPayload) {
	fmt.Printf("[worker %d] Flushed batch of %d records\n", workerID, len(batch))
}

func (p *WorkerPool) Stop() {
	close(p.jobs)
	p.wg.Wait()
	fmt.Println("All workers finished gracefully")
}
