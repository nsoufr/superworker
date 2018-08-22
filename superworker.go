package superworker

import (
	"log"
	"os"
	"os/signal"
	"sync"
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
		time.Sleep(9 * time.Second)
		os.Exit(0)
	}
}

func (w *Worker) startProcessors(stop <-chan struct{}) {
	go func() {
		for {
			select {
			case <-stop:
				log.Println("stopping processors")
				return
			default:
				{
					w.process()
				}
			}
		}
	}()
}

func (w *Worker) process() {
	wg := &sync.WaitGroup{}
	wg.Add(w.Concurrency)

	for {
		for _, q := range w.Queues {
			j, err := w.Storage.Pull(q)
			// not a better place for this
			if err.Error() == "redis: nil" {
				log.Printf("there is no  job in %s queue\n", q)
				return
			}

			if err != nil {
				log.Printf("%s while dequeuing from %s\n", err, q)
				return
			}
			if w.Executors[j.Name] == nil {
				log.Printf("There is no executor registered for %s\n", j.Name)
			}

			err = w.Executors[j.Name].Execute(*j, w)
			if err != nil {
				log.Println(err)
			}
		}

	}
}

func (w *Worker) trapSignals() (c chan os.Signal) {
	c = make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	return c
}
