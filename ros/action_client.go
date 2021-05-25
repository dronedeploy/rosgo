package ros

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/team-rocos/go-common/logging"
)

type defaultActionClient struct {
	started          bool
	node             Node
	action           string
	actionType       ActionType
	actionResult     MessageType
	actionResultType MessageType
	actionFeedback   MessageType
	actionGoal       MessageType
	goalPub          Publisher
	cancelPub        Publisher
	resultSub        Subscriber
	feedbackSub      Subscriber
	statusSub        Subscriber
	logger           logging.Log
	handlers         []*clientGoalHandler
	handlersMutex    sync.RWMutex
	goalIDGen        *goalIDGenerator
	statusReceived   bool
	callerID         string
}

func newDefaultActionClient(node Node, action string, actType ActionType) (*defaultActionClient, error) {
	var err error

	ac := &defaultActionClient{
		node:           node,
		action:         action,
		actionType:     actType,
		actionResult:   actType.ResultType(),
		actionFeedback: actType.FeedbackType(),
		actionGoal:     actType.GoalType(),
		logger:         node.Logger(),
		statusReceived: false,
		goalIDGen:      newGoalIDGenerator(node.Name()),
	}

	// Create goal publisher
	ac.goalPub, err = node.NewPublisher(fmt.Sprintf("%s/goal", action), actType.GoalType())
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create goal publisher:")
	}
	// Create cancel publisher
	ac.cancelPub, err = node.NewPublisher(fmt.Sprintf("%s/cancel", action), NewActionGoalIDType())
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create cancel publisher:")
	}
	// Create result subscriber
	ac.resultSub, err = node.NewSubscriber(fmt.Sprintf("%s/result", action), actType.ResultType(), ac.internalResultCallback)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create result subscriber:")
	}
	// Create feedback subscriber
	ac.feedbackSub, err = node.NewSubscriber(fmt.Sprintf("%s/feedback", action), actType.FeedbackType(), ac.internalFeedbackCallback)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create feedback subscriber:")
	}
	// Create status subscriber
	ac.statusSub, err = node.NewSubscriber(fmt.Sprintf("%s/status", action), NewActionStatusArrayType(), ac.internalStatusCallback)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create status subscriber:")
	}
	return ac, nil
}

func (ac *defaultActionClient) SendGoal(goal Message, transitionCb, feedbackCb interface{}, goalID string) (ClientGoalHandler, error) {
	if !ac.started {
		ac.logger.Error().Msg("[ActionClient] Trying to send a goal on an inactive ActionClient")
	}

	// Create a new action goal message
	ag := ac.actionType.GoalType().NewGoalMessage().(*DynamicActionGoal)

	// make a goalId message with timestamp and generated id
	goalid := NewActionGoalIDType().NewGoalIDMessage()

	// If goalID not provided, generate ID using time stamp and node name.
	if goalID == "" {
		goalid.SetStamp(Now())
		goalid.SetID(ac.goalIDGen.generateID())
	} else {
		goalid.SetStamp(Now())
		goalid.SetID(goalID)
	}

	// set the action goal fields
	ag.SetGoal(goal)
	ag.SetGoalId(goalid)
	ag.SetHeader(NewActionHeader())

	// publish the goal to the action server
	err := ac.PublishActionGoal(ag)
	if err != nil {
		return nil, errors.Wrap(err, "failed to publish action goal")
	}
	ac.logger.Debug().Str("id", goalID).Msg("action goal with id published successfully")
	// create an internal handler to track this goal
	handler, err := newClientGoalHandler(ac, ag, transitionCb, feedbackCb)
	if err != nil {
		return nil, err
	}

	ac.handlersMutex.Lock()
	ac.handlers = append(ac.handlers, handler)
	ac.handlersMutex.Unlock()

	return handler, nil
}

func (ac *defaultActionClient) CancelAllGoals() {
	if !ac.started {
		ac.logger.Error().Msg("[ActionClient] Trying to cancel goals on an inactive ActionClient")
		return
	}

	// Create a goal id message
	goalid := NewActionGoalIDType().NewGoalIDMessage()
	ac.cancelPub.Publish(goalid)
}

func (ac *defaultActionClient) CancelAllGoalsBeforeTime(stamp Time) {
	// Create a goal id message
	if !ac.started {
		ac.logger.Error().Msg("[ActionClient] Trying to cancel goals on an inactive ActionClient")
		return
	}
	// Create a goal id message using timestamp
	goalid := NewActionGoalIDType().NewGoalIDMessage()
	goalid.SetStamp(stamp)
	ac.cancelPub.Publish(goalid)
}

// Shutdown client ends an action client and its pub/subs, but keeps the node alive
// Takes a set of current active subscription booleans as to not remove any subscribers that the node is consuming
func (ac *defaultActionClient) ShutdownClient(status bool, feedback bool, result bool) {
	// Create a goal id message
	ac.handlersMutex.Lock()
	defer ac.handlersMutex.Unlock()

	ac.started = false
	for _, h := range ac.handlers {
		h.Shutdown(false)
	}

	// Shutdown publishers and subscribers
	ac.node.RemovePublisher(fmt.Sprintf("%s/goal", ac.action))
	ac.node.RemovePublisher(fmt.Sprintf("%s/cancel", ac.action))

	if !feedback {
		ac.node.RemoveSubscriber(fmt.Sprintf("%s/feedback", ac.action))
	}
	if !result {
		ac.node.RemoveSubscriber(fmt.Sprintf("%s/result", ac.action))
	}
	if !status {
		ac.node.RemoveSubscriber(fmt.Sprintf("%s/status", ac.action))
	}
	ac.handlers = nil
}

// Shutdown completely ends a client and its associated node
func (ac *defaultActionClient) Shutdown() {
	ac.handlersMutex.Lock()
	defer ac.handlersMutex.Unlock()

	ac.started = false
	for _, h := range ac.handlers {
		h.Shutdown(false)
	}

	ac.handlers = nil
	ac.node.Shutdown()
}

func (ac *defaultActionClient) PublishActionGoal(ag ActionGoal) error {
	if ac.started {
		err := ac.goalPub.TryPublish(ag)
		if err != nil {
			return err
		}
		return nil
	}

	return errors.New("action client not started")
}

func (ac *defaultActionClient) PublishCancel(cancel *DynamicMessage) {
	if ac.started {
		ac.cancelPub.Publish(cancel)
	}
}

func (ac *defaultActionClient) WaitForServer(timeout Duration) bool {
	started := false
	ac.logger.Info().Msg("[ActionClient] Waiting action server to start")
	rate := CycleTime(NewDuration(0, 10000000))
	waitStart := Now()

LOOP:
	for !started {
		gSubs := ac.goalPub.GetNumSubscribers()
		cSubs := ac.cancelPub.GetNumSubscribers()
		fPubs := ac.feedbackSub.GetNumPublishers()
		rPubs := ac.resultSub.GetNumPublishers()
		sPubs := ac.statusSub.GetNumPublishers()
		started = (gSubs > 0 && cSubs > 0 && fPubs > 0 && rPubs > 0 && sPubs > 0)

		now := Now()
		diff := now.Diff(waitStart)
		if !timeout.IsZero() && diff.Cmp(timeout) >= 0 {
			break LOOP
		}

		rate.Sleep()
	}

	if started {
		ac.started = started
	}

	return started
}

func (ac *defaultActionClient) DeleteGoalHandler(gh *clientGoalHandler) {
	ac.handlersMutex.Lock()
	defer ac.handlersMutex.Unlock()

	for i, h := range ac.handlers {
		if h == gh {
			ac.handlers[i] = ac.handlers[len(ac.handlers)-1]
			ac.handlers[len(ac.handlers)-1] = nil
			ac.handlers = ac.handlers[:len(ac.handlers)-1]
		}
	}
}

// Internal Result Callback for Result subscriber
func (ac *defaultActionClient) internalResultCallback(result interface{}, event MessageEvent) {
	ac.handlersMutex.RLock()
	defer ac.handlersMutex.RUnlock()

	// Interface to ActionResult
	results := ac.actionType.ResultType().(*DynamicActionResultType).NewResultMessageFromInterface(result)

	for _, h := range ac.handlers {
		if err := h.updateResult(results); err != nil {
			ac.logger.Error().Err(err).Msg("")
		}
	}
}

// Itnernal Feedback Callback for Feedback subscriber
func (ac *defaultActionClient) internalFeedbackCallback(feedback interface{}, event MessageEvent) {
	ac.handlersMutex.RLock()
	defer ac.handlersMutex.RUnlock()

	// Interface to ActionFeedback
	feed := ac.actionType.FeedbackType().(*DynamicActionFeedbackType).NewFeedbackMessageFromInterface(feedback)

	for _, h := range ac.handlers {
		h.updateFeedback(feed)
	}
}

// Internal Status Callback for status subscriber
func (ac *defaultActionClient) internalStatusCallback(statusArr interface{}, event MessageEvent) {
	ac.handlersMutex.RLock()
	defer ac.handlersMutex.RUnlock()

	if !ac.statusReceived {
		ac.statusReceived = true
		ac.logger.Debug().Msg("recieved first status message from action server ")
	} else if ac.callerID != event.PublisherName {
		ac.logger.Debug().Str("callerID", ac.callerID).Str("pub", event.PublisherName).Msg("previously received status from callerID, now from pub. did the action server change")
	}

	// Interface to status array conversion
	statusArray := NewActionStatusArrayType().(*DynamicActionStatusArrayType).NewStatusArrayFromInterface(statusArr)
	ac.callerID = event.PublisherName
	for _, h := range ac.handlers {
		if err := h.updateStatus(statusArray); err != nil {
			ac.logger.Error().Err(err).Msg("")
		}
	}
}
