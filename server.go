package superworker

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type workerEntry struct {
	concurrency int
	worker      Worker
	opts        *WorkerOpts
}

type Server struct {
	Queues  []string
	Storage Storage
	workers map[string]workerEntry
}

type WorkerOpts struct {
	Concurrency int
}

func (s *Server) AddWorker(pattern string, worker Worker, opts *WorkerOpts) {
	o := WorkerOpts{}

	if opts != nil {
		o = *opts
	}

	if pattern == "" {
		panic("superworker: invalid pattern")
	}
	if worker == nil {
		panic("superworker: nil handler")
	}
	if _, exist := s.workers[pattern]; exist {
		panic("superworker: multiple registrations for " + pattern)
	}

	if s.workers == nil {
		s.workers = make(map[string]workerEntry)
	}

	s.workers[pattern] = workerEntry{worker: worker, opts: &o}
}

type WorkerOptions struct {
	Concurrency int
}

func NewServer() *Server {
	return &Server{}
}

func (w *Server) Run() {
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

func (w *Server) startProcessors(stop <-chan struct{}) {
	go w.process(stop)
}

func (w *Server) process(stop <-chan struct{}) {
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

func (w *Server) trapSignals() (c chan os.Signal) {
	c = make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	return c
}
