package pkv

import (
	"testing"
)

func TestSet(t *testing.T) {
	servers := StartServers()
	if servers == nil {
		t.Errorf("Failed to start servers")
	}

	client := NewClient(0)
	val := client.Set("key1", 1234)

	if val == nil {
		t.Errorf("Nil value returned from client")
	}

	if val.V != 1234 {
		t.Errorf("Value does not match: %v, expected V=1234", val)
	}
}
