package fsm

import "testing"

func TestTransitions(t *testing.T) {
	if !CanTransition(RoleData, StateStandby, StateActive) {
		t.Fatal("standby->active")
	}
	if CanTransition(RoleData, StateActive, StateStandby) {
		t.Fatal("active->standby forbidden")
	}
	if !CanTransition(RoleData, StateSealed, StateCleaning) {
		t.Fatal("sealed->cleaning")
	}
	if err := ValidateTransition(RoleData, StateStandby, StateSealed); err == nil {
		t.Fatal("expected error")
	}
}

func TestStartupState(t *testing.T) {
	if !StartupStateAllowed(StateStandby) {
		t.Fatal()
	}
	if StartupStateAllowed(StateActive) {
		t.Fatal()
	}
}
