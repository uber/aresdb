package tools

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type worker struct {
	batches [][]interface{}
	time    time.Time
	mutex   sync.Mutex
}

func (w *worker) now() time.Time {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.time
}

func (w *worker) advanceClock(duration time.Duration) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.time = w.time.Add(duration)
}

func (w *worker) run(batches chan []interface{}, wg *sync.WaitGroup) {
	for batch := range batches {
		w.batches = append(w.batches, batch)
	}
	wg.Done()
}

var _ = Describe("Batcher", func() {
	It("flushes when max batch size is reached", func() {
		worker := &worker{}
		batcher := NewBatcher(4, 100, worker.now)
		Ω(batcher).ShouldNot(BeNil())
		batcher.StartWorker(worker.run)
		batcher.Add(1, worker.now())
		batcher.Add(2, worker.now())
		batcher.Add(3, worker.now())
		batcher.Add(4, worker.now())
		batcher.Add(5, worker.now()) // Ensure that first batch is flushed.
		batcher.Close()

		Ω(worker.batches).Should(Equal([][]interface{}{
			{1, 2, 3, 4},
			{5},
		}))
	})

	It("flushes when max delay is reached", func() {
		worker := &worker{}
		batcher := NewBatcher(4, 100, worker.now)
		Ω(batcher).ShouldNot(BeNil())
		batcher.StartWorker(worker.run)
		now := worker.now()
		worker.advanceClock(100)
		batcher.Add(1, now) // Flushed immediately since it's too old.
		batcher.Add(2, worker.now())
		batcher.Close()

		Ω(worker.batches).Should(Equal([][]interface{}{
			{1},
			{2},
		}))
	})
})
