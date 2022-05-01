package pkv

import (
	"testing"
)

func TestSet(t *testing.T) {
	servers := StartServers()
	if servers == nil {
		t.Errorf("Failed to start servers")
	}

	client := NewClient(13)

	// Set key1 to 1234.
	val := client.Set("key1", 1234)
	if val == nil {
		t.Errorf("Nil value returned from client")
	}
	if val.V != 1234 {
		t.Errorf("Value does not match: %v, expected V=1234", val)
	}

	// Set this key again, this should create another version.
	val = client.Set("key1", 4321)
	if val == nil {
		t.Errorf("Nil value returned from client")
	}
	if val.V != 4321 {
		t.Errorf("Value does not match: %v, expected V=4321", val)
	}

	// Set another key.
	val = client.Set("key2", 2222)
	if val == nil {
		t.Errorf("Nil value returned from client")
	}
	if val.V != 2222 {
		t.Errorf("Value does not match: %v, expected V=2222", val)
	}

	// Set key1 with a past version.
	val = client.SetVersion("key1", 3333, 1)
	if val == nil {
		t.Errorf("Nil value returned from client")
	}
	// You can't change version that is already accepted.
	// Paxos will force your proposal value to be original (1234).
	if val.V != 1234 {
		t.Errorf("Value does not match: %v, expected V=1234", val)
	}
}
