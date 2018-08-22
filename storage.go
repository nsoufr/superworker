package superworker

type Storage interface {
	Enqueue(*Job) (jobId string, err error)
	Pull(queue string) (*Job, error)
	Clear(queue string) error
}
