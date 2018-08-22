package redis

import (
	"testing"

	"github.com/nandosousafr/superworker"
)

func TestPushPullAndClear(t *testing.T) {
	r, err := NewWithURL("redis://localhost:6379")
	if err != nil {
		t.Fatal(err)
	}

	j := &superworker.Job{
		Name:  "put-item-to-service",
		Args:  []string{"itemA", "itemB"},
		Queue: "normal",
	}

	_, err = r.Enqueue(j)
	if err != nil {
		t.Error(err)
	}

	pulled, err := r.Pull(j.Queue)
	if err != nil {
		t.Error(err)
	}

	if jid != pulled.Id {
		t.Errorf("expected %s got %s", jid, pulled.Id)
	}

	r.Clear(j.Queue)
}
