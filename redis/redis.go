package redis

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-redis/redis"
	"github.com/nandosousafr/superworker"
)

const namespacePrefix = "superworker"

type Client struct {
	Redis *redis.Client
}

func (c *Client) Enqueue(job *superworker.Job) (jobId string, err error) {
	jid, err := randomHex(16)
	if err != nil {
		return
	}
	j := *job

	j.Id = jid

	json, err := json.Marshal(j)
	if err != nil {
		return
	}

	cmd := c.Redis.RPush(key(j.Queue), json)

	if cmd.Val() == 0 {
		return "", errors.New("error while enqueuing")
	}

	return j.Id, nil
}

func (c *Client) Pull(queue string) (job *superworker.Job, err error) {
	job = &superworker.Job{}
	strCMD := c.Redis.LPop(key(queue))

	b, err := strCMD.Bytes()
	if err != nil {
		return
	}

	err = json.Unmarshal(b, job)
	return
}

func (c *Client) Clear(queue string) error {
	c.Redis.Del(key(queue))
	return nil
}

func NewWithURL(redisURL string) (*Client, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(options)
	return &Client{Redis: client}, nil
}

// Move it to a util package
func randomHex(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func key(suffix string) string {
	return fmt.Sprintf("%s:%s", namespacePrefix, suffix)
}
