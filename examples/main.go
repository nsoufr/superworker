package main

import (
	"log"

	"github.com/nandosousafr/superworker"
	"github.com/nandosousafr/superworker/redis"
)

type MyWorker struct{}

func (m *MyWorker) Execute(j superworker.Job, w *superworker.Worker) error {
	log.Println(j.Args, "testing....")
	return nil
}

func main() {
	storage, err := redis.NewWithURL("redis://localhost:6379")
	if err != nil {
		log.Fatal(err)
		return
	}

	worker := superworker.NewWorker()
	worker.Queues = []string{"normal"}
	worker.Executors = map[string]superworker.Executor{
		"put-item-to-service": &MyWorker{},
	}

	worker.AddHandler("put-item-to-service", MyWorker)

	worker.Storage = storage
	worker.Run()
}
