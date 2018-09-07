package redis

import (
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	single := SingleConfig{
		Addr: "127.0.0.1:6379",
	}
	client, err := NewClient(&Config{
		DeployType: "single",
		ForSingle:  single,
	})
	if err != nil {
		t.Errorf("new client err->%v", err)
	}
	m := NewModule("ooz-test")
	s, err := client.Set(m.GetKey("ooz_key"), "ooz_value", time.Second).Result()
	if err != nil {
		t.Fatalf("c.Set().Result() err-> %v", err)
	}
	t.Logf("c.Set().Result() result-> %s", s)

	s, err = client.Get(m.GetKey("ooz_key")).Result()
	if err != nil {
		t.Fatalf("c.Get().Result() error-> %v", err)
	}
	t.Logf("c.Get().Result() result-> %s", s)
}
