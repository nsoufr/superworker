package main

import (
	"log"

	"github.com/nandosousafr/superworker"
	"github.com/nandosousafr/superworker/redis"
)

type myWorker struct{}

func (m *myWorker) Process(job superworker.Job) error {
	return nil
}

func main() {
	storage, err := redis.NewWithURL("redis://localhost:6379")
	if err != nil {
		log.Fatal(err)
		return
	}

	server := superworker.NewServer()
	server.Storage = storage

	server.AddWorker("create-lead", &myWorker{})
	server.Run()
}
