package superworker

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Worker struct {
	Queues    []string
	Executors map[string]Executor
	Storage   Storage
	Options   *WorkerOptions
}

type WorkerOptions struct {
	Concurrency int
}

func NewWorker() *Worker {
	opts := parseOptions()
	return &Worker{Options: opts}
}

func (w *Worker) Run() {
	fmt.Printf("%s: %+v\n", "starting with opts", *w.Options)

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
	opts := *w.Options
	bucket := make(chan struct{}, opts.Concurrency)

	for i := 0; i < opts.Concurrency; i++ {
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

func parseOptions() *WorkerOptions {
	opts := WorkerOptions{}

	flag.IntVar(&opts.Concurrency, "concurrency", 5, "concurrency")
	flag.Parse()

	return &opts
}
