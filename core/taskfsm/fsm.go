package taskfsm

import "fmt"

type Status string

const (
	Created          Status = "CREATED"
	Planning         Status = "PLANNING"
	Validating       Status = "VALIDATING"
	WaitingApproval  Status = "WAITING_APPROVAL"
	Ready            Status = "READY"
	Running          Status = "RUNNING"
	Interrupted      Status = "INTERRUPTED"
	Expired          Status = "EXPIRED"
	Succeeded        Status = "SUCCEEDED"
	Failed           Status = "FAILED"
	Partial          Status = "PARTIAL"
	Unknown          Status = "UNKNOWN"
	Cancelled        Status = "CANCELLED"
)

var terminals = map[Status]bool{
	Expired: true, Succeeded: true, Failed: true, Partial: true, Unknown: true, Cancelled: true,
}

func IsTerminal(s Status) bool { return terminals[s] }

// allowed edges from Rev.4.1 §7 (including △ conditions, enforced by caller).
var allowed = map[Status]map[Status]bool{
	Created:         {Planning: true, Cancelled: true},
	Planning:        {Validating: true, Interrupted: true, Failed: true, Cancelled: true},
	Validating:      {WaitingApproval: true, Ready: true, Failed: true, Cancelled: true},
	WaitingApproval: {Ready: true, Interrupted: true, Expired: true, Cancelled: true},
	Ready:           {Validating: true, Running: true, Expired: true, Cancelled: true},
	Running:         {Interrupted: true, Succeeded: true, Failed: true, Partial: true, Unknown: true, Cancelled: true},
	Interrupted:     {Validating: true, Running: true, Failed: true, Unknown: true, Cancelled: true},
}

func CanTransition(from, to Status) error {
	if from == to {
		return fmt.Errorf("no-op transition %s", from)
	}
	if IsTerminal(from) {
		return fmt.Errorf("terminal %s cannot move to %s", from, to)
	}
	if allowed[from][to] {
		return nil
	}
	return fmt.Errorf("illegal transition %s → %s", from, to)
}
