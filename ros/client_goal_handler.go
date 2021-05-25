package ros

import (
	"fmt"
	"reflect"

	"github.com/team-rocos/go-common/logging"
)

type clientGoalHandler struct {
	actionClient *defaultActionClient
	stateMachine *clientStateMachine
	actionGoal   ActionGoal
	actionGoalID string
	transitionCb interface{}
	feedbackCb   interface{}
	logger       logging.Log
}

func newClientGoalHandler(ac *defaultActionClient, ag ActionGoal, transitionCb, feedbackCb interface{}) (*clientGoalHandler, error) {
	id, err := ag.GetGoalId()
	if err != nil {
		return nil, err
	}

	gh := &clientGoalHandler{
		actionClient: ac,
		stateMachine: newClientStateMachine(),
		actionGoal:   ag,
		actionGoalID: id.GetID(),
		transitionCb: transitionCb,
		feedbackCb:   feedbackCb,
		logger:       ac.logger,
	}

	return gh, nil
}

func findGoalStatus(statusArr ActionStatusArray, id string) ActionStatus {
	// Create a dynamic message for status message
	var status ActionStatus
	// loop through goal status array for matching status message
	for _, st := range statusArr.GetStatusArray() {
		goalID := st.GetGoalID()
		if goalID.GetID() == id {
			status = st
			break
		}
	}
	return status
}

func (gh *clientGoalHandler) GetCommState() (CommState, error) {
	if gh.stateMachine == nil {
		return Lost, fmt.Errorf("trying to get state on an inactive ClientGoalHandler")
	}

	return gh.stateMachine.getState(), nil
}

func (gh *clientGoalHandler) GetGoalStatus() (uint8, error) {
	if gh.stateMachine == nil {
		return uint8(9), fmt.Errorf("trying to get goal status on an inactive ClientGoalHandler")
	}

	return gh.stateMachine.getGoalStatus().GetStatus(), nil
}

func (gh *clientGoalHandler) GetGoalStatusText() (string, error) {
	if gh.stateMachine == nil {
		return "", fmt.Errorf("trying to get goal status text on an inactive ClientGoalHandler")
	}

	return gh.stateMachine.getGoalStatus().GetStatusText(), nil
}

func (gh *clientGoalHandler) GetTerminalState() (uint8, error) {
	if gh.stateMachine == nil {
		return 0, fmt.Errorf("trying to get goal status on inactive clientGoalHandler")
	}

	if gh.stateMachine.state != Done {
		gh.actionClient.logger.Warn().Stringer("state", gh.stateMachine.state).Msg("asking for terminal state when we are in state")
	}

	// implement get status
	goalStatus := gh.stateMachine.getGoalStatus().GetStatus()
	if goalStatus == uint8(2) ||
		goalStatus == uint8(3) ||
		goalStatus == uint8(4) ||
		goalStatus == uint8(5) ||
		goalStatus == uint8(7) ||
		goalStatus == uint8(9) {

		return goalStatus, nil
	}

	gh.actionClient.logger.Warn().Uint8("state", goalStatus).Msg("asking for terminal state when latest goal is in state")
	return uint8(9), nil
}

func (gh *clientGoalHandler) GetResult() (Message, error) {
	if gh.stateMachine == nil {
		return nil, fmt.Errorf("trying to get goal status on inactive clientGoalHandler")
	}

	result := gh.stateMachine.getGoalResult()

	if result == nil {
		return nil, fmt.Errorf("trying to get result when no result has been recieved")
	}

	return result.GetResult(), nil
}

func (gh *clientGoalHandler) Resend() error {
	if gh.stateMachine == nil {
		return fmt.Errorf("trying to call resend on inactive client goal hanlder")
	}

	gh.actionClient.goalPub.Publish(gh.actionGoal)
	return nil
}

func (gh *clientGoalHandler) IsExpired() bool {
	return gh.stateMachine == nil
}

func (gh *clientGoalHandler) Cancel() error {
	if gh.stateMachine == nil {
		return fmt.Errorf("trying to call cancel on inactive client goal hanlder")
	}
	// Create a goal id message with timestamp and goal id
	cancel := NewActionGoalIDType().NewGoalIDMessage()
	cancel.SetStamp(Now())
	cancel.SetID(gh.actionGoalID)
	gh.actionClient.cancelPub.Publish(cancel)
	gh.stateMachine.transitionTo(WaitingForCancelAck, gh, gh.transitionCb)
	return nil
}

func (gh *clientGoalHandler) Shutdown(deleteFromManager bool) {
	gh.stateMachine = nil
	if deleteFromManager {
		gh.actionClient.DeleteGoalHandler(gh)
	}
}

func (gh *clientGoalHandler) updateFeedback(af ActionFeedback) {
	if gh.actionGoalID != af.GetStatus().GetGoalID().GetID() {
		return
	}

	if gh.feedbackCb != nil && gh.stateMachine.getState() != Done {
		fun := reflect.ValueOf(gh.feedbackCb)
		args := []reflect.Value{reflect.ValueOf(gh), reflect.ValueOf(af.GetFeedback())}
		numArgsNeeded := fun.Type().NumIn()

		if numArgsNeeded == 2 {
			fun.Call(args)
		}
	}
}

func (gh *clientGoalHandler) updateResult(result ActionResult) error {
	if gh.actionGoalID != result.GetStatus().GetGoalID().GetID() {
		return nil
	}

	status := result.GetStatus()
	state := gh.stateMachine.getState()

	gh.stateMachine.setGoalStatus(status.GetGoalID(), status.GetStatus(), status.GetStatusText())
	gh.stateMachine.setGoalResult(result)

	if state == WaitingForGoalAck ||
		state == WaitingForCancelAck ||
		state == Pending ||
		state == Active ||
		state == WaitingForResult ||
		state == Recalling ||
		state == Preempting {

		// Create a status array message
		statusArray := NewActionStatusArrayType().NewStatusArrayMessage()
		array := make([]ActionStatus, 0)
		array = append(array, result.GetStatus())
		statusArray.SetStatusArray(array)
		// Update the goal handler status
		if err := gh.updateStatus(statusArray); err != nil {
			return err
		}

		gh.stateMachine.transitionTo(Done, gh, gh.transitionCb)
		return nil

	} else if state == Done {

		return fmt.Errorf("got a result when we are in the `DONE` state")
	} else {
		return fmt.Errorf("unknown state %v", state)
	}
}

func (gh *clientGoalHandler) updateStatus(statusArr ActionStatusArray) error {
	state := gh.stateMachine.getState()
	if state == Done {
		return nil
	}

	status := findGoalStatus(statusArr, gh.actionGoalID)
	if status == nil {
		if state != WaitingForGoalAck &&
			state != WaitingForResult &&
			state != Done {

			gh.logger.Warn().Str("actionGoalID", gh.actionGoalID).Msg("transitioning goal with actionGoalID to `Lost`")
			gh.stateMachine.setAsLost()
			gh.stateMachine.transitionTo(Done, gh, gh.transitionCb)
		}
		return nil
	}

	gh.stateMachine.setGoalStatus(status.GetGoalID(), status.GetStatus(), status.GetStatusText())
	nextStates, err := gh.stateMachine.getTransitions(status)
	if err != nil {
		return fmt.Errorf("error getting transitions: %v", err)
	}

	for e := nextStates.Front(); e != nil; e = e.Next() {
		gh.stateMachine.transitionTo(e.Value.(CommState), gh, gh.transitionCb)
	}

	return nil
}
