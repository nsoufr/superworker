package superworker

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Job struct {
	Id         string   `json:"id"`
	Name       string   `json:"name"`
	Queue      string   `json:"queue"`
	Args       []string `json:"args"`
	Retry      bool     `json:"retry"`
	RetryCount int      `json:"retry_count"`
	At         int      `json:"at"`
}

type Executor interface {
	Execute(Job, *Worker) error
}

type Worker struct {
	Queues      []string
	Concurrency int
	Executors   map[string]Executor
	Storage     Storage
}

type processor struct {
	Worker *Worker
	Stop   chan struct{}
}

func (w *Worker) Run() {
	stopChan := make(chan struct{}, 1)

	w.startProcessors(stopChan)
	m := w.trapSignals()

	select {
	case <-m:
		log.Println("Stopping....")

		// notify processors about errors
		stopChan <- struct{}{}
		time.Sleep(2 * time.Second)
		os.Exit(0)
	}
}

func (w *Worker) startProcessors(stop <-chan struct{}) {
	go w.process(stop)
}

func (w *Worker) process(stop <-chan struct{}) {
	bucket := make(chan struct{}, w.Concurrency)

	for i := 0; i < w.Concurrency; i++ {
		bucket <- struct{}{}
	}

	for {
		select {
		case <-stop:
			log.Println("stopping processors")
			return
		default:
			<-bucket
			go func() {
				for _, q := range w.Queues {
					j, err := w.Storage.Pull(q)
					// not a better place for this
					if err.Error() == "redis: nil" {
						time.Sleep(1 * time.Second)
						continue
					}

					if err != nil {
						log.Printf("%s while dequeuing from %s\n", err, q)
						continue
					}
					if w.Executors[j.Name] == nil {
						continue

					}

					err = w.Executors[j.Name].Execute(*j, w)
					if err != nil {
						log.Println(err)
						continue
					}

				}
				bucket <- struct{}{}
			}()

		}
	}
}

func (w *Worker) trapSignals() (c chan os.Signal) {
	c = make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	return c
}
