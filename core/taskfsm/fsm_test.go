package taskfsm

import "testing"

func TestLegalAndIllegal(t *testing.T) {
	if err := CanTransition(Created, Planning); err != nil {
		t.Fatal(err)
	}
	if err := CanTransition(Validating, Ready); err != nil {
		t.Fatal(err)
	}
	if err := CanTransition(Running, Unknown); err != nil {
		t.Fatal(err)
	}
	if err := CanTransition(Succeeded, Running); err == nil {
		t.Fatal("terminal must not move")
	}
	if err := CanTransition(Created, Running); err == nil {
		t.Fatal("skip planning must be illegal")
	}
}
