package superworker

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
