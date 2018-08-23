# superworker

Basic, reliable and fast background processing for Go.

### Example

WIP

```go
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

func process(j superworker.Job, w *superWorker.Worker) {
  args := j.Args
  saveToDatabase(args)
  
  w.Ack()
}

func main() {
	storage, err := redis.NewWithURL("redis://localhost:6379")
	if err != nil {
		log.Fatal(err)
		return
	}

	worker := superworker.NewWorker()
	worker.Queues = []string{"normal"}

	worker.Handle("put-item-to-service", MyWorker)
  worker.HandleFunc("put-item-to-service", )

	worker.Storage = storage
	worker.Run()
}

```
