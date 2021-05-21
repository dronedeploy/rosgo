package ros

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type defaultActionServer struct {
	node             Node
	autoStart        bool
	started          bool
	action           string
	actionType       ActionType
	actionResult     MessageType
	actionResultType MessageType
	actionFeedback   MessageType
	actionGoal       MessageType
	statusMutex      sync.RWMutex
	statusFrequency  Rate
	statusTimer      *time.Ticker
	handlers         map[string]*serverGoalHandler
	handlersTimeout  Duration
	handlersMutex    sync.Mutex
	goalCallback     interface{}
	cancelCallback   interface{}
	lastCancel       Time
	pubQueueSize     int
	subQueueSize     int
	goalSub          Subscriber
	cancelSub        Subscriber
	resultPub        Publisher
	feedbackPub      Publisher
	statusPub        Publisher
	statusPubChan    chan struct{}
	goalIDGen        *goalIDGenerator
	shutdownChan     chan struct{}
}

func newDefaultActionServer(node Node, action string, actType ActionType, goalCb interface{}, cancelCb interface{}, start bool) *defaultActionServer {
	return &defaultActionServer{
		node:            node,
		autoStart:       start,
		started:         false,
		action:          action,
		actionType:      actType,
		actionResult:    actType.ResultType(),
		actionFeedback:  actType.FeedbackType(),
		actionGoal:      actType.GoalType(),
		handlersTimeout: NewDuration(60, 0),
		goalCallback:    goalCb,
		cancelCallback:  cancelCb,
		lastCancel:      Now(),
	}
}

func (as *defaultActionServer) initialize() error {
	var err error

	as.statusPubChan = make(chan struct{}, 10)
	as.shutdownChan = make(chan struct{}, 10)

	// setup goal id generator and goal handlers
	as.goalIDGen = newGoalIDGenerator(as.node.Name())
	as.handlers = map[string]*serverGoalHandler{}

	// setup action result type so that we can create default result messages
	//res := .NewMessage().(ActionResult).GetResult()
	as.actionResultType = as.actionResult

	// get frequency from ros params
	as.statusFrequency = NewRate(5.0)

	// get queue sizes from ros params
	// queue sizes not implemented by Node yet
	as.pubQueueSize = 50
	as.subQueueSize = 50

	// Create goal subscription
	as.goalSub, err = as.node.NewSubscriber(fmt.Sprintf("%s/goal", as.action), as.actionType.GoalType(), as.internalGoalCallback)
	if err != nil {
		return errors.Wrap(err, "Failed to create goal publisher:")
	}
	// Create a cancel subscription
	as.cancelSub, err = as.node.NewSubscriber(fmt.Sprintf("%s/cancel", as.action), NewActionGoalIDType(), as.internalCancelCallback)
	if err != nil {
		return errors.Wrap(err, "Failed to create goal publisher:")
	}
	// Create result publisher
	as.resultPub, err = as.node.NewPublisher(fmt.Sprintf("%s/result", as.action), as.actionType.ResultType())
	if err != nil {
		return errors.Wrap(err, "Failed to create goal publisher:")
	}
	// Create feedback publisher
	as.feedbackPub, err = as.node.NewPublisher(fmt.Sprintf("%s/feedback", as.action), as.actionType.FeedbackType())
	if err != nil {
		return errors.Wrap(err, "Failed to create goal publisher:")
	}
	// Create Status publisher
	as.statusPub, err = as.node.NewPublisher(fmt.Sprintf("%s/status", as.action), NewActionStatusArrayType())
	if err != nil {
		return errors.Wrap(err, "Failed to create goal publisher:")
	}

	return nil
}

func (as *defaultActionServer) Start() {
	logger := as.node.Logger()
	defer func() {
		logger.Debug().Msg("defaultActionServer.start exit")
		as.started = false
	}()

	// initialize subscribers and publishers
	err := as.initialize()
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize action server")
	}

	// start status publish ticker that notifies at 5hz
	as.statusTimer = time.NewTicker(time.Second / 5.0)
	defer as.statusTimer.Stop()

	as.started = true

	for {
		select {
		case <-as.shutdownChan:
			return
		case <-as.statusTimer.C:
			as.PublishStatus()

		case <-as.statusPubChan:
			arr, err := as.getStatus()
			if err != nil {
				logger.Error().Err(err).Msg("failed to get action server status")
			} else {
				as.statusPub.Publish(arr)
			}
		}
	}
}

// PublishResult publishes action result message
func (as *defaultActionServer) PublishResult(status ActionStatus, result Message) {
	msg := as.actionResult.(*DynamicActionResultType).NewResultMessage()

	msg.SetHeader(NewActionHeader())
	msg.SetStatus(status)
	msg.SetResult(result)
	as.resultPub.Publish(msg)
}

// PublishFeedback publishes action feedback messages
func (as *defaultActionServer) PublishFeedback(status ActionStatus, feedback Message) {
	msg := as.actionFeedback.(*DynamicActionFeedbackType).NewFeedbackMessage()

	msg.SetHeader(NewActionHeader())
	msg.SetStatus(status)
	msg.SetFeedback(feedback)
	as.feedbackPub.Publish(msg)
}

func (as *defaultActionServer) getStatus() (ActionStatusArray, error) {
	as.handlersMutex.Lock()
	defer as.handlersMutex.Unlock()
	var statusList []ActionStatus

	if as.node.OK() {
		for id, gh := range as.handlers {
			handlerTime := gh.GetHandlerDestructionTime()
			destroyTime := handlerTime.Add(as.handlersTimeout)
			if !handlerTime.IsZero() && destroyTime.Cmp(Now()) <= 0 {
				delete(as.handlers, id)
				continue
			}

			status, err := gh.GetGoalStatus()
			if err != nil {
				return nil, err
			}
			statusList = append(statusList, status)
		}
	}
	// Create a goal status array message
	statusArray := NewActionStatusArrayType().NewStatusArrayMessage().(*DynamicActionStatusArray)

	// Add status list
	statusArray.SetStatusArray(statusList)
	statusArray.SetHeader(NewActionHeader())
	return statusArray, nil
}

func (as *defaultActionServer) PublishStatus() {
	as.statusPubChan <- struct{}{}
}

// internalCancelCallback recieves cancel message from client
func (as *defaultActionServer) internalCancelCallback(goalid interface{}, event MessageEvent) {
	as.handlersMutex.Lock()
	defer as.handlersMutex.Unlock()

	goalFound := false
	logger := as.node.Logger()
	logger.Debug().Msg("Action server has received a new cancel request")

	goalIDType := NewActionGoalIDType()
	goalID := goalIDType.(*DynamicActionGoalIDType).NewGoalIDMessageFromInterface(goalid).(*DynamicActionGoalID)

	for id, gh := range as.handlers {
		idStamp := goalID.GetStamp()
		cancelAll := (goalID.GetID() == "" && idStamp.IsZero())
		cancelCurrent := (goalID.GetID() == id)

		st, err := gh.GetGoalStatus()
		if err != nil {
			logger.Error().Err(err).Msg("unable to get goal status from goal handler")
			continue
		}
		statusStamp := st.GetGoalID().GetStamp()
		cancelBeforeStamp := (!idStamp.IsZero() && statusStamp.Cmp(idStamp) <= 0)

		if cancelAll || cancelCurrent || cancelBeforeStamp {
			if goalID.GetID() == st.GetGoalID().GetID() {
				goalFound = true
			}

			if gh.SetCancelRequested() {
				args := []reflect.Value{reflect.ValueOf(goalID)}
				fun := reflect.ValueOf(as.cancelCallback)
				numArgsNeeded := fun.Type().NumIn()

				if numArgsNeeded <= 1 {
					fun.Call(args[0:numArgsNeeded])
				}
			}
		}
	}

	if goalID.GetID() != "" && !goalFound {
		gh := newServerGoalHandlerWithGoalId(as, goalID)
		as.handlers[goalID.GetID()] = gh
		gh.SetHandlerDestructionTime(Now())
	}
	stamp := goalID.GetStamp()
	if stamp.Cmp(as.lastCancel) > 0 {
		as.lastCancel = stamp
	}
}

// internalGoalCallback recieves the goals from client and checks if
// the goalID already exists in the status list. If not, it will call
// server's goalCallback with goal that was recieved from the client.
func (as *defaultActionServer) internalGoalCallback(goals interface{}, event MessageEvent) error {
	as.handlersMutex.Lock()
	defer as.handlersMutex.Unlock()

	logger := as.node.Logger()

	// Convert interface to Message
	goal := as.actionType.GoalType().(*DynamicActionGoalType).NewGoalMessageFromInterface(goals).(*DynamicActionGoal)
	goalID, err := goal.GetGoalId()
	if err != nil {
		return err
	}

	for id, gh := range as.handlers {
		if goalID.GetID() == id {
			st, err := gh.GetGoalStatus()
			if err != nil {
				logger.Error().Err(err).Msg("failed to get goal status from goal handler")
				return err
			}
			logger.Debug().Str("id", goalID.GetID()).Uint8("status", st.GetStatus()).Msg("Goal was already in the status list with status")
			if st.GetStatus() == uint8(7) {
				st.SetStatus(uint8(8))
				result := as.actionResultType.NewMessage()
				as.PublishResult(st, result)
			}

			gh.SetHandlerDestructionTime(Now())
			return nil
		}
	}

	id := goalID.GetID()
	if len(id) == 0 {
		id = as.goalIDGen.generateID()
		// Create goal id message with id and time stamp
		newGoalID := NewActionGoalIDType().NewGoalIDMessage()
		newGoalID.SetID(id)
		newGoalID.SetStamp(goalID.GetStamp())
		// Set goal id
		goal.SetGoalId(newGoalID)
	}

	gh, err := newServerGoalHandlerWithGoal(as, goal)
	if err != nil {
		return err
	}
	as.handlers[id] = gh
	stamp := goalID.GetStamp()
	if !stamp.IsZero() && stamp.Cmp(as.lastCancel) <= 0 {
		gh.SetCancelled(nil, "timestamp older than last goal cancel")
		return nil
	}

	args := []reflect.Value{reflect.ValueOf(goal), reflect.ValueOf(event)}
	fun := reflect.ValueOf(as.goalCallback)
	numArgsNeeded := fun.Type().NumIn()

	if numArgsNeeded <= 1 {
		fun.Call(args[0:numArgsNeeded])
	}
	return nil
}

func (as *defaultActionServer) getHandler(id string) *serverGoalHandler {
	handler := as.handlers[id]
	return handler
}

// RegisterGoalCallback replaces existing goal callback function with newly
// provided goal callback function.
func (as *defaultActionServer) RegisterGoalCallback(goalCb interface{}) {
	as.goalCallback = goalCb
}

func (as *defaultActionServer) RegisterCancelCallback(cancelCb interface{}) {
	as.cancelCallback = cancelCb
}

func (as *defaultActionServer) Shutdown() {
	as.shutdownChan <- struct{}{}
}
