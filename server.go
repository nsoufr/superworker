package superworker

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Server struct {
	Queues  []string
	Storage Storage
	workers map[string]Worker
}

type ServerOptions struct {
	Concurrency  int
	WorkersToRun []string // TODO
}

func (s *Server) AddWorker(pattern string, worker Worker) {
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
		s.workers = make(map[string]Worker)
	}

	s.workers[pattern] = worker
}

func NewServer() *Server {
	return &Server{}
}

func (w *Server) Run() {
	opts := parseOptions()
	log.Printf("%s: %+v\n", "Starting server with options", *opts)
	stopChan := make(chan struct{}, 1)

	w.startManager(stopChan, opts)
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

func (w *Server) startManager(stop <-chan struct{}, opts *ServerOptions) {
	go w.process(stop, opts)
}

func (w *Server) process(stop <-chan struct{}, options *ServerOptions) {
	opts := *options
	// Separate process as an entity

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
				for queue, worker := range w.workers {
					j, err := w.Storage.Pull(queue)
					if err.Error() == "redis: nil" {
						time.Sleep(1 * time.Second)
						continue
					}

					if err != nil {
						log.Printf("%s while dequeuing from %s\n", err, queue)
						continue
					}

					err = worker.Process(*j)
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

func parseOptions() *ServerOptions {
	o := ServerOptions{}

	flag.IntVar(&o.Concurrency, "concurrency", 20, "Concurrency: How many go-routines should execute workload.")
	flag.Parse()

	return &o
}
