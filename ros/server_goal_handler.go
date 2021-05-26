package ros

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/rs/zerolog"
)

type serverGoalHandler struct {
	as                     ActionServer
	sm                     *serverStateMachine
	goal                   ActionGoal
	handlerDestructionTime Time
	handlerMutex           sync.RWMutex
	logger                 zerolog.Logger
}

func newServerGoalHandlerWithGoal(as ActionServer, goal ActionGoal) (*serverGoalHandler, error) {
	id, err := goal.GetGoalId()
	if err != nil {
		return nil, err
	}
	gh := &serverGoalHandler{
		as:   as,
		sm:   newServerStateMachine(id),
		goal: goal,
	}

	return gh, nil
}

func newServerGoalHandlerWithGoalId(as ActionServer, goalID ActionGoalID) *serverGoalHandler {
	return &serverGoalHandler{
		as: as,
		sm: newServerStateMachine(goalID),
	}
}

func (gh *serverGoalHandler) GetHandlerDestructionTime() Time {
	gh.handlerMutex.RLock()
	defer gh.handlerMutex.RUnlock()

	return gh.handlerDestructionTime
}

func (gh *serverGoalHandler) SetHandlerDestructionTime(t Time) {
	gh.handlerMutex.Lock()
	defer gh.handlerMutex.Unlock()

	gh.handlerDestructionTime = t
}

func (gh *serverGoalHandler) SetAccepted(text string) error {
	if gh.goal == nil {
		return fmt.Errorf("attempt to set handler on an uninitialized handler")
	}

	if status, err := gh.sm.transition(Accept, text); err != nil {
		return fmt.Errorf("to transition to an active state, the goal must be in a pending"+
			"or recalling state, it is currently in state: %d", status.GetStatus())
	}

	gh.as.PublishStatus()

	return nil
}

func (gh *serverGoalHandler) SetCancelled(result Message, text string) error {
	if gh.goal == nil {
		return fmt.Errorf("attempt to set handler on an uninitialized handler handler")
	}

	status, err := gh.sm.transition(Cancel, text)
	if err != nil {
		return fmt.Errorf("to transition to an Canceled state, the goal must be in a pending"+
			" or recalling state, it is currently in state: %d", status.GetStatus())
	}

	gh.SetHandlerDestructionTime(Now())
	gh.as.PublishResult(status, result)

	return nil
}

func (gh *serverGoalHandler) SetRejected(result Message, text string) error {
	if gh.goal == nil {
		return fmt.Errorf("attempt to set handler on an uninitialized handler handler")
	}

	status, err := gh.sm.transition(Reject, text)
	if err != nil {
		return fmt.Errorf("to transition to an Rejected state, the goal must be in a pending"+
			"or recalling state, it is currently in state: %d", status.GetStatus())
	}

	gh.SetHandlerDestructionTime(Now())
	gh.as.PublishResult(status, result)

	return nil
}

func (gh *serverGoalHandler) SetAborted(result Message, text string) error {
	if gh.goal == nil {
		return fmt.Errorf("attempt to set handler on an uninitialized handler handler")
	}

	status, err := gh.sm.transition(Abort, text)
	if err != nil {
		return fmt.Errorf("to transition to an Aborted state, the goal must be in a pending"+
			"or recalling state, it is currently in state: %d", status.GetStatus())
	}

	gh.SetHandlerDestructionTime(Now())
	gh.as.PublishResult(status, result)

	return nil
}

func (gh *serverGoalHandler) SetSucceeded(result Message, text string) error {
	logger := gh.logger
	if gh.goal == nil {
		return fmt.Errorf("attempt to set handler on an uninitialized handler handler")
	}

	status, err := gh.sm.transition(Succeed, text)
	if err != nil {
		logger.Error().Uint8("state", status.GetStatus()).Msg("to transition to an Succeeded state, the goal must be in a pending or recalling state, it is currently in state")
		return err
	}

	gh.SetHandlerDestructionTime(Now())
	gh.as.PublishResult(status, result)

	return nil
}

func (gh *serverGoalHandler) SetCancelRequested() bool {
	if gh.goal == nil {
		return false
	}

	if _, err := gh.sm.transition(CancelRequest, "Cancel requested"); err != nil {
		return false
	}

	gh.SetHandlerDestructionTime(Now())
	return true
}

func (gh *serverGoalHandler) PublishFeedback(feedback Message) {
	gh.as.PublishFeedback(gh.sm.getStatus(), feedback)
}

func (gh *serverGoalHandler) GetGoal() (Message, error) {
	if gh.goal == nil {
		return nil, nil
	}

	return gh.goal.GetGoal()
}

func (gh *serverGoalHandler) GetGoalId() (ActionGoalID, error) {
	if gh.goal == nil {
		// Create a new Goal id message
		goalMsg := NewActionGoalIDType().NewGoalIDMessage()
		return goalMsg, nil
	}

	return gh.goal.GetGoalId()
}

func (gh *serverGoalHandler) GetGoalStatus() (ActionStatus, error) {
	status := gh.sm.getStatus()
	if status.GetStatus() != 0 && gh.goal != nil {
		if id, err := gh.goal.GetGoalId(); err != nil {
			return nil, err
		} else if id.GetID() != "" {
			return status, nil
		}
	}
	// Create a new goal status message
	status = NewActionStatusType().NewMessage().(*DynamicActionStatus)
	return status, nil
}

func (gh *serverGoalHandler) Equal(other ServerGoalHandler) bool {
	if gh.goal == nil || other == nil {
		return false
	}
	id, err := gh.goal.GetGoalId()
	if err != nil {
		return false
	}
	otherID, err := other.GetGoalId()
	if err != nil {
		return false
	}

	return id.GetID() == otherID.GetID()
}

func (gh *serverGoalHandler) NotEqual(other ServerGoalHandler) bool {
	return !gh.Equal(other)
}

func (gh *serverGoalHandler) Hash() (uint32, error) {
	goalID, err := gh.goal.GetGoalId()
	if err != nil {
		return 0, err
	}
	id := goalID.GetID()
	hs := fnv.New32a()
	hs.Write([]byte(id))

	return hs.Sum32(), nil
}
