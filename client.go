package superworker

type Client struct {
	Storage *Storage
}

func NewClient(storage *Storage) *Client {
	return &Client{storage}
}
