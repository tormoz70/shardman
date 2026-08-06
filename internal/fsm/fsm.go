package fsm

import "fmt"

type State string

const (
	StateStandby  State = "standby"
	StateActive   State = "active"
	StateSealed   State = "sealed"
	StateCleaning State = "cleaning"
)

type Role string

const (
	RoleData  Role = "data"
	RoleError Role = "error"
)

var dataTransitions = map[State]map[State]bool{
	StateStandby:  {StateActive: true},
	StateActive:   {StateSealed: true},
	StateSealed:   {StateCleaning: true},
	StateCleaning: {StateStandby: true},
}

// CanTransition checks if state change is allowed for role.
func CanTransition(role Role, from, to State) bool {
	if role == RoleError {
		return from == to || (from == StateStandby && to == StateActive) || to == StateActive
	}
	allowed, ok := dataTransitions[from]
	if !ok {
		return false
	}
	return allowed[to]
}

func ValidateTransition(role Role, from, to State) error {
	if !CanTransition(role, from, to) {
		return fmt.Errorf("invalid transition %s -> %s for role %s", from, to, role)
	}
	return nil
}

// StartupStateAllowed rejects hot-add as active/sealed.
func StartupStateAllowed(state State) bool {
	return state == "" || state == StateStandby
}
